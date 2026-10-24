import json
import os
import random
import socket
import string
from typing import Dict, Optional

from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

app = FastAPI()

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
STATIC_DIR = os.path.join(BASE_DIR, "static")
TEMPLATES_DIR = os.path.join(BASE_DIR, "templates")
os.makedirs(STATIC_DIR, exist_ok=True)
os.makedirs(TEMPLATES_DIR, exist_ok=True)

app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")
templates = Jinja2Templates(directory=TEMPLATES_DIR)

# Each gesture beats exactly two others. The verb is what the winner does.
RULES = {
    "Rock": {"Scissors": "crushes", "Lizard": "crushes"},
    "Paper": {"Rock": "covers", "Spock": "disproves"},
    "Scissors": {"Paper": "cuts", "Lizard": "decapitates"},
    "Lizard": {"Spock": "poisons", "Paper": "eats"},
    "Spock": {"Scissors": "smashes", "Rock": "vaporizes"},
}
CHOICES = tuple(RULES.keys())
MAX_HUMANS = 16
MAX_BOTS = 10


def lan_host() -> str:
    """Best-effort LAN address so a localhost host can tell phones where to join."""
    try:
        probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        probe.connect(("8.8.8.8", 80))
        ip = probe.getsockname()[0]
        probe.close()
        return ip
    except OSError:
        return ""


def clean_code(raw: str) -> str:
    code = "".join(ch for ch in (raw or "").upper() if ch.isalnum())[:6]
    return code or "MAIN"


def clean_name(raw: object, fallback: str) -> str:
    name = " ".join(str(raw or "").split())[:12]
    return name or fallback


async def send_json(ws: Optional[WebSocket], payload: dict) -> None:
    if ws is None:
        return
    try:
        await ws.send_text(json.dumps(payload))
    except Exception:
        pass


class Room:
    def __init__(self, code: str):
        self.code = code
        self.host_socket: Optional[WebSocket] = None
        self.players: Dict[str, dict] = {}
        self.state = "LOBBY"  # LOBBY, CHOOSING, RESOLVED
        self.round = 1
        self.bot_seq = 0
        self.last_victories: list = []

    def _roster(self) -> list:
        roster = []
        for pid, player in self.players.items():
            roster.append({
                "id": pid,
                "name": player["name"],
                "score": player["score"],
                "has_chosen": player["choice"] is not None,
                "choice": player["choice"] if self.state == "RESOLVED" else None,
                "is_cpu": player["is_cpu"],
            })
        return roster

    async def broadcast_state(self) -> None:
        payload = {
            "type": "STATE_UPDATE",
            "state": self.state,
            "round": self.round,
            "room": self.code,
            "players": self._roster(),
            "victories": self.last_victories if self.state == "RESOLVED" else [],
        }
        await send_json(self.host_socket, payload)
        for pid, player in list(self.players.items()):
            socket = player.get("socket")
            if socket is None:
                continue
            personal = dict(payload)
            personal["players"] = self._roster()
            personal["my_choice"] = player["choice"]
            personal["my_id"] = pid
            await send_json(socket, personal)

    async def check_all_submitted(self) -> str:
        """Advance a choosing round. Returns resolved, aborted, waiting, or idle."""
        if self.state != "CHOOSING":
            return "idle"
        if len(self.players) < 2:
            self.state = "LOBBY"
            self.last_victories = []
            for player in self.players.values():
                player["choice"] = None
            await self.broadcast_state()
            return "aborted"
        if all(player["choice"] is not None for player in self.players.values()):
            await self.resolve_round()
            return "resolved"
        return "waiting"

    async def resolve_round(self) -> None:
        if self.state != "CHOOSING":
            return
        self.state = "RESOLVED"
        order = list(self.players.keys())
        victories = []
        for winner_id in order:
            winner_choice = self.players[winner_id]["choice"]
            for loser_id in order:
                if winner_id == loser_id:
                    continue
                loser_choice = self.players[loser_id]["choice"]
                verb = RULES.get(winner_choice, {}).get(loser_choice)
                if not verb:
                    continue
                self.players[winner_id]["score"] += 1
                victories.append({
                    "winner": winner_id,
                    "loser": loser_id,
                    "winner_name": self.players[winner_id]["name"],
                    "loser_name": self.players[loser_id]["name"],
                    "winner_choice": winner_choice,
                    "loser_choice": loser_choice,
                    "verb": verb,
                })
        self.last_victories = victories
        await self.broadcast_state()

    async def begin_choosing(self, advance_round: bool) -> bool:
        if len(self.players) < 2:
            return False
        if advance_round:
            self.round += 1
        self.state = "CHOOSING"
        self.last_victories = []
        for player in self.players.values():
            player["choice"] = random.choice(CHOICES) if player["is_cpu"] else None
        await self.broadcast_state()
        await self.check_all_submitted()
        return True

    async def add_bot(self) -> bool:
        bots = sum(1 for player in self.players.values() if player["is_cpu"])
        if bots >= MAX_BOTS:
            return False
        self.bot_seq += 1
        bot_id = f"bot_{self.bot_seq}"
        self.players[bot_id] = {
            "name": f"Bot {self.bot_seq}",
            "socket": None,
            "score": 0,
            "choice": random.choice(CHOICES) if self.state == "CHOOSING" else None,
            "is_cpu": True,
        }
        status = await self.check_all_submitted()
        if status in ("idle", "waiting"):
            await self.broadcast_state()
        return True

    async def reset_game(self) -> None:
        self.state = "LOBBY"
        self.round = 1
        self.last_victories = []
        for player in self.players.values():
            player["score"] = 0
            player["choice"] = None
        await self.broadcast_state()


rooms: Dict[str, Room] = {}


def is_host(websocket: WebSocket, room: Room) -> bool:
    return room.host_socket is websocket


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/", response_class=HTMLResponse)
async def serve_index(request: Request):
    return templates.TemplateResponse(
        request,
        "index.html",
        {"lan_host": lan_host()},
        headers={"Cache-Control": "no-store"},
    )


@app.websocket("/ws/{room_code}/{client_type}")
async def websocket_endpoint(websocket: WebSocket, room_code: str, client_type: str):
    await websocket.accept()
    if client_type not in ("host", "player"):
        await websocket.close(code=1008)
        return

    code = clean_code(room_code)
    room = rooms.get(code)
    if room is None:
        room = Room(code)
        rooms[code] = room

    pid = None
    try:
        if client_type == "host":
            room.host_socket = websocket
            await room.broadcast_state()

        while True:
            try:
                data = json.loads(await websocket.receive_text())
            except json.JSONDecodeError:
                continue
            if not isinstance(data, dict):
                continue
            action = data.get("action")

            if action == "PING":
                await send_json(websocket, {"type": "PONG"})
                continue

            if action == "JOIN":
                if client_type != "player" or pid is not None:
                    continue
                humans = sum(1 for player in room.players.values() if not player["is_cpu"])
                if humans >= MAX_HUMANS:
                    await send_json(websocket, {"type": "INFO", "message": "This room is full."})
                    continue
                pid = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
                while pid in room.players:
                    pid = "".join(random.choices(string.ascii_lowercase + string.digits, k=6))
                fallback = f"Player {humans + 1}"
                room.players[pid] = {
                    "name": clean_name(data.get("name"), fallback),
                    "socket": websocket,
                    "score": 0,
                    "choice": None,
                    "is_cpu": False,
                }
                await send_json(websocket, {"type": "JOIN_SUCCESS", "pid": pid, "room": room.code})
                await room.broadcast_state()
                continue

            if action == "SUBMIT_CHOICE":
                # Trust the socket, not a pid in the message.
                player = room.players.get(pid) if pid else None
                choice = data.get("choice")
                if player is None or player["is_cpu"] or room.state != "CHOOSING":
                    continue
                if choice not in RULES:
                    continue
                player["choice"] = choice
                status = await room.check_all_submitted()
                if status in ("idle", "waiting"):
                    await room.broadcast_state()
                continue

            if not is_host(websocket, room):
                continue

            if action == "ADD_BOT":
                added = await room.add_bot()
                if not added:
                    await send_json(websocket, {"type": "INFO", "message": "CPU limit reached."})
            elif action == "START_ROUND":
                if room.state != "LOBBY":
                    continue
                started = await room.begin_choosing(False)
                if not started:
                    await send_json(websocket, {
                        "type": "INFO",
                        "message": "Add at least 2 players or CPUs before starting.",
                    })
            elif action == "NEXT_ROUND":
                if room.state != "RESOLVED":
                    continue
                started = await room.begin_choosing(True)
                if not started:
                    await send_json(websocket, {
                        "type": "INFO",
                        "message": "Need at least 2 players for the next round.",
                    })
            elif action == "RESET_GAME":
                await room.reset_game()
                await send_json(websocket, {
                    "type": "INFO",
                    "message": "Game reset. Scores are back to zero.",
                })

    except WebSocketDisconnect:
        pass
    finally:
        removed_player = False
        if client_type == "host" and room.host_socket is websocket:
            room.host_socket = None
        if pid and room.players.get(pid, {}).get("socket") is websocket:
            del room.players[pid]
            removed_player = True

        room_alive = rooms.get(room.code) is room
        if room_alive:
            humans_left = any(player["socket"] is not None for player in room.players.values())
            if room.host_socket is None and not humans_left:
                rooms.pop(room.code, None)
                room_alive = False
        if room_alive and (removed_player or client_type == "host"):
            status = await room.check_all_submitted()
            if status in ("idle", "waiting"):
                await room.broadcast_state()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.environ.get("PORT", "8000")),
        reload=True,
    )
