"""HyeArcade Online: small in-memory HTTP/SSE room relay compatible with the LAN protocol."""
from __future__ import annotations

import asyncio
import json
import os
import secrets
import time
import uuid
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse

MAX_BODY = 1_000_000
MAX_EVENTS = 500
MAX_HISTORY = 1500
PRIVATE_STATE_GAMES = {"crazy-eights", "uno", "whot", "monopoly-deal", "scrabble", "mahjong", "treasure-hunt", "scramble", "monopoly"}
GAME_LIMITS = {
    "chess": (2, 2), "checkers": (2, 2), "drafts": (2, 2), "tic-tac-toe": (2, 2),
    "connect-four": (2, 2), "gomoku": (2, 2), "pirate-go": (2, 2), "ludo": (2, 4),
    "snake-ladder": (2, 4), "crazy-eights": (2, 4), "uno": (2, 4), "whot": (2, 6),
    "scrabble": (2, 4), "mahjong": (4, 4), "monopoly-deal": (2, 5), "monopoly": (2, 6),
    "scramble": (1, 4), "treasure-hunt": (1, 4), "solitaire": (1, 1),
    "mahjong-solitaire": (1, 1), "vita-mahjong": (1, 1), "ayo": (2, 2),
}


def now_ms() -> int:
    return int(time.time() * 1000)


def safe_text(value: Any, limit: int) -> str:
    return value.strip()[:limit] if isinstance(value, str) else ""


def limits(game_id: str) -> tuple[int, int]:
    return GAME_LIMITS.get(game_id, (2, 4))


def safe_profile(value: Any) -> dict | None:
    if not isinstance(value, dict):
        return None
    def num(v: Any) -> int:
        try:
            return max(0, min(1_000_000, int(float(v or 0))))
        except (ValueError, TypeError):
            return 0
    return {
        "title": safe_text(value.get("title"), 40) or "Player",
        "matchesPlayed": num(value.get("matchesPlayed")), "wins": num(value.get("wins")),
        "losses": num(value.get("losses")), "draws": num(value.get("draws")),
        "totalXp": num(value.get("totalXp")), "bestWinStreak": num(value.get("bestWinStreak")),
        "unlockedAchievements": list(dict.fromkeys(safe_text(x, 80) for x in value.get("unlockedAchievements", []) if isinstance(x, str)))[:250],
    }


def unique_players(players: list[dict]) -> list[dict]:
    result, seen = [], set()
    for p in players:
        if not isinstance(p, dict) or not p.get("id") or p["id"] in seen:
            continue
        seen.add(p["id"])
        cp = dict(p)
        cp["seat"] = len(result)
        cp["ready"] = True if cp.get("kind") == "bot" else bool(cp.get("ready"))
        result.append(cp)
    return result


class Room:
    def __init__(self, code: str, host_id: str, game_id: str, player: dict, token: str):
        stamp = now_ms()
        self.code, self.host_id, self.game_id = code, host_id, game_id
        self.session = {"id": str(uuid.uuid4()), "gameId": game_id, "players": [player], "state": None,
                        "history": [], "status": "lobby", "result": None, "createdAt": stamp, "updatedAt": stamp}
        self.session_views: dict[str, dict] = {}
        self.clients: dict[str, set[asyncio.Queue]] = defaultdict(set)
        self.sequence = 0
        self.events: deque[dict] = deque(maxlen=MAX_EVENTS)
        self.tokens = {token: host_id} if token else {}
        self.request_cache: dict[str, dict[str, dict]] = defaultdict(dict)
        self.message_times: dict[str, deque] = defaultdict(deque)
        self.last_active = time.time()

    def is_private(self) -> bool:
        return self.game_id in PRIVATE_STATE_GAMES

    def view_for(self, player_id: str, session: dict | None = None) -> dict:
        source = session or self.session
        view = self.session_views.get(player_id)
        if view:
            return view
        if self.is_private():
            return {**source, "state": None, "initialState": None, "history": []}
        return source

    def public_info(self, player_id: str) -> dict:
        return {"code": self.code, "clientId": player_id, "session": self.view_for(player_id),
                "host": self.host_id == player_id, "sequence": self.sequence}

    def emit(self, message: dict, views: dict | None = None) -> dict:
        self.sequence += 1
        enriched = {**message, "version": 1, "sequence": self.sequence,
                    "eventId": f"{self.code}:{self.sequence}",
                    "senderId": message.get("senderId") or message.get("player") or (message.get("move") or {}).get("player")}
        event = {"sequence": self.sequence, "message": enriched, "at": now_ms(),
                 "views": views, "privateState": self.is_private()}
        if message.get("type") == "session":
            self.events = deque((e for e in self.events if e["message"].get("type") != "session"), maxlen=MAX_EVENTS)
        self.events.append(event)
        self.last_active = time.time()
        for player_id, queues in list(self.clients.items()):
            if enriched.get("type") == "move" and player_id != self.host_id:
                continue
            if enriched.get("type") == "move.rejected" and player_id != enriched.get("player"):
                continue
            outgoing = dict(enriched)
            if enriched.get("type") == "session":
                outgoing["session"] = (views or {}).get(player_id) or self.view_for(player_id, message.get("session"))
            for queue in list(queues):
                try:
                    queue.put_nowait(outgoing)
                except asyncio.QueueFull:
                    pass
        return event


rooms: dict[str, Room] = {}
app = FastAPI(title="HyeArcade Online Relay", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["GET", "POST", "OPTIONS"], allow_headers=["*"], expose_headers=["Last-Event-ID"])


@app.get("/health")
async def health():
    return {"ok": True, "service": "hyearcade-online", "rooms": len(rooms)}


@app.get("/api/rooms")
async def list_rooms(gameId: str | None = None):
    """Public room browser: only waiting rooms are listed; private room codes still work."""
    result = []
    for room in list(rooms.values()):
        if room.session["status"] != "lobby":
            continue
        if gameId and room.game_id != gameId:
            continue
        _, maximum = limits(room.game_id)
        if len(room.session["players"]) >= maximum:
            continue
        result.append({"code": room.code, "gameId": room.game_id, "players": len(room.session["players"]),
                       "maxPlayers": maximum, "status": room.session["status"],
                       "hostName": next((p["name"] for p in room.session["players"] if p["id"] == room.host_id), "Host"),
                       "hostOnline": any(p["id"] == room.host_id and p.get("connected") for p in room.session["players"]),
                       "createdAt": room.session["createdAt"]})
    result.sort(key=lambda x: x["createdAt"])
    return {"rooms": result}


@app.post("/api/rooms")
async def create_room(request: Request):
    body = await read_body(request)
    game_id, incoming = safe_text(body.get("gameId"), 80), body.get("player")
    if not game_id or not isinstance(incoming, dict) or not isinstance(incoming.get("name"), str):
        raise HTTPException(400, "gameId and player name are required")
    code = secrets.token_hex(3).upper()
    while code in rooms:
        code = secrets.token_hex(3).upper()
    client_id = str(uuid.uuid4())
    player = {"id": client_id, "profileId": safe_text(incoming.get("profileId"), 100) or None,
              "publicProfile": safe_profile(incoming.get("publicProfile")), "name": safe_text(incoming.get("name"), 32) or "Player",
              "kind": "human", "seat": 0, "connected": True, "ready": False}
    room = Room(code, client_id, game_id, player, safe_text(body.get("clientToken"), 120))
    rooms[code] = room
    return room.public_info(client_id)


async def read_body(request: Request) -> dict:
    raw = await request.body()
    if len(raw) > MAX_BODY:
        raise HTTPException(413, "Request body too large")
    try:
        result = json.loads(raw or b"{}")
    except json.JSONDecodeError:
        raise HTTPException(400, "Invalid JSON")
    if not isinstance(result, dict):
        raise HTTPException(400, "JSON object required")
    return result


def get_room(code: str) -> Room:
    room = rooms.get(code.upper())
    if not room:
        raise HTTPException(404, "Room not found")
    return room


def emit_session(room: Room) -> dict:
    room.session["updatedAt"] = now_ms()
    return room.emit({"type": "session", "session": room.session}, room.session_views)


@app.get("/api/rooms/{code}")
async def room_info(code: str):
    room = get_room(code)
    return {"code": room.code, "gameId": room.game_id, "status": room.session["status"],
            "hostOnline": any(p["id"] == room.host_id and p.get("connected") for p in room.session["players"]),
            "players": [{"name": p["name"], "seat": p["seat"], "connected": p.get("connected", False)} for p in room.session["players"]]}


@app.post("/api/rooms/{code}/join")
async def join_room(code: str, request: Request):
    room, body = get_room(code), await read_body(request)
    token = safe_text(body.get("clientToken"), 120)
    existing_id = room.tokens.get(token) if token else None
    if existing_id and any(p["id"] == existing_id for p in room.session["players"]):
        player = next(p for p in room.session["players"] if p["id"] == existing_id)
        player["connected"] = True
        return room.public_info(existing_id)
    if body.get("gameId") and body["gameId"] != room.game_id:
        raise HTTPException(409, "Wrong game for this room")
    if room.session["status"] != "lobby":
        raise HTTPException(409, "Game already started")
    _, maximum = limits(room.game_id)
    if len(room.session["players"]) >= maximum:
        raise HTTPException(409, f"Room is full ({maximum} players max)")
    incoming = body.get("player")
    if not isinstance(incoming, dict) or not isinstance(incoming.get("name"), str):
        raise HTTPException(400, "Player name is required")
    player_id = str(uuid.uuid4())
    player = {"id": player_id, "profileId": safe_text(incoming.get("profileId"), 100) or None,
              "publicProfile": safe_profile(incoming.get("publicProfile")), "name": safe_text(incoming.get("name"), 32) or "Player",
              "kind": "remote", "seat": len(room.session["players"]), "connected": True, "ready": False}
    room.session["players"] = unique_players([*room.session["players"], player])
    if token:
        room.tokens[token] = player_id
    emit_session(room)
    return room.public_info(player_id)


@app.get("/api/rooms/{code}/events")
async def room_events(code: str, request: Request, clientId: str = "", afterSequence: int = 0):
    room = get_room(code)
    if not any(p["id"] == clientId for p in room.session["players"]):
        raise HTTPException(403, "Unknown player")
    queue: asyncio.Queue = asyncio.Queue(maxsize=256)
    room.clients[clientId].add(queue)
    player = next(p for p in room.session["players"] if p["id"] == clientId)
    player["connected"] = True
    last = int(request.headers.get("last-event-id", afterSequence) or 0)
    async def stream():
        try:
            yield ": connected\n\n"
            if last:
                replay = [e for e in room.events if e["sequence"] > last]
                for event in replay:
                    yield sse_data(event["message"], event["sequence"])
            else:
                snapshot = {"type": "session", "session": room.view_for(clientId), "version": 1,
                            "sequence": room.sequence, "eventId": f"{room.code}:{room.sequence}"}
                yield sse_data(snapshot, room.sequence)
            while not await request.is_disconnected():
                try:
                    message = await asyncio.wait_for(queue.get(), timeout=15)
                    yield sse_data(message, message.get("sequence", 0))
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            room.clients[clientId].discard(queue)
            if not room.clients[clientId]:
                room.clients.pop(clientId, None)
            # A short grace period lets SSE reconnects survive mobile network changes.
            asyncio.create_task(disconnect_after_grace(room, clientId))
    return StreamingResponse(stream(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "Connection": "keep-alive", "X-Accel-Buffering": "no"})


def sse_data(message: dict, sequence: int) -> str:
    return f"id: {sequence}\ndata: {json.dumps(message, separators=(',', ':'))}\n\n"


async def disconnect_after_grace(room: Room, player_id: str):
    await asyncio.sleep(3.5)
    if room.clients.get(player_id):
        return
    player = next((p for p in room.session["players"] if p["id"] == player_id), None)
    if not player:
        return
    player["connected"] = False
    player["ready"] = False
    remove = room.session["status"] == "lobby"
    if remove and player_id == room.host_id:
        next_player = next((p for p in room.session["players"] if p["id"] != player_id and p.get("connected") and p.get("kind") != "bot"), None)
        if next_player:
            room.host_id = next_player["id"]
            room.emit({"type": "host", "player": room.host_id})
        else:
            rooms.pop(room.code, None)
            return
    if remove and player_id != room.host_id:
        room.session["players"] = unique_players([p for p in room.session["players"] if p["id"] != player_id])
        room.tokens = {token: pid for token, pid in room.tokens.items() if pid != player_id}
    room.emit({"type": "leave", "player": player_id, "name": player.get("name", "Player")})
    emit_session(room)


@app.post("/api/rooms/{code}/switch-game")
async def switch_game(code: str, request: Request):
    room, body = get_room(code), await read_body(request)
    if body.get("clientId") != room.host_id:
        raise HTTPException(403, "Only host can change the game")
    if room.session["status"] != "lobby":
        raise HTTPException(409, "Game can only be changed before the match starts")
    game_id = safe_text(body.get("gameId"), 80)
    if not game_id or not all(c.islower() or c.isdigit() or c == "-" for c in game_id):
        raise HTTPException(400, "Invalid game id")
    _, maximum = limits(game_id)
    if len(room.session["players"]) > maximum:
        raise HTTPException(409, f"This game supports at most {maximum} players; remove players before switching")
    room.game_id = game_id
    room.session = {**room.session, "gameId": game_id, "boardId": None, "state": None,
                    "initialState": None, "history": [], "status": "lobby", "result": None,
                    "players": unique_players([{**p, "ready": p.get("kind") == "bot"} for p in room.session["players"]])}
    room.session_views = {}
    event = emit_session(room)
    return {"ok": True, "gameId": game_id, "sequence": event["sequence"]}


@app.post("/api/rooms/{code}/start")
async def start_game(code: str, request: Request):
    room, body = get_room(code), await read_body(request)
    if body.get("clientId") != room.host_id:
        raise HTTPException(403, "Only host can start")
    if room.session["status"] != "lobby":
        raise HTTPException(409, "Game already started")
    if any(p.get("kind") != "bot" and p["id"] != room.host_id and (not p.get("connected") or not p.get("ready")) for p in room.session["players"]):
        raise HTTPException(409, "Every human guest must be connected and ready before starting")
    _, maximum = limits(room.game_id)
    bots = body.get("bots") if isinstance(body.get("bots"), list) else []
    for bot in bots[:max(0, maximum - len(room.session["players"]))]:
        if isinstance(bot, dict):
            room.session["players"].append({**bot, "id": safe_text(bot.get("id"), 100) or str(uuid.uuid4()),
                "name": safe_text(bot.get("name"), 32) or "Bot", "kind": "bot", "connected": True, "ready": True})
    room.session["players"] = unique_players(room.session["players"])
    minimum, maximum = limits(room.game_id)
    if len(room.session["players"]) > maximum:
        raise HTTPException(409, "Too many players")
    if len(room.session["players"]) < minimum:
        raise HTTPException(409, f"At least {minimum} players are required")
    room.session["status"] = "playing"
    room.session["boardId"] = safe_text(body.get("boardId"), 80) or None
    emit_session(room)
    return {"ok": True, "sequence": room.sequence}


@app.post("/api/rooms/{code}/session")
async def publish_session(code: str, request: Request):
    room, body = get_room(code), await read_body(request)
    if body.get("clientId") != room.host_id:
        raise HTTPException(403, "Only host can publish session")
    incoming = body.get("session")
    if not isinstance(incoming, dict) or incoming.get("gameId") != room.game_id or not isinstance(incoming.get("players"), list) or not isinstance(incoming.get("history"), list):
        raise HTTPException(400, "Invalid session snapshot")
    if len(json.dumps(incoming).encode()) > 700_000:
        raise HTTPException(413, "Session snapshot is too large")
    _, maximum = limits(room.game_id)
    if len(incoming["players"]) > maximum:
        raise HTTPException(409, "Session has too many players")
    members = {p["id"]: p for p in room.session["players"]}
    if len(incoming["players"]) != len(members) or any(p.get("id") not in members for p in incoming["players"] if isinstance(p, dict)):
        raise HTTPException(409, "Session player list does not match room membership")
    if len(incoming["history"]) > MAX_HISTORY:
        raise HTTPException(413, "Match history is too large; start a new match to continue")
    canonical = []
    for idx, member in enumerate(room.session["players"]):
        submitted = next(p for p in incoming["players"] if p["id"] == member["id"])
        canonical.append({**submitted, **{k: member.get(k) for k in ("id", "profileId", "publicProfile", "name", "kind", "connected", "ready")}, "seat": idx,
                          "ready": True if member.get("kind") == "bot" else member.get("ready", False)})
    next_session = {**incoming, "players": canonical, "history": incoming["history"][-MAX_HISTORY:], "updatedAt": now_ms()}
    next_views = {}
    views = body.get("views") if isinstance(body.get("views"), dict) else {}
    for player_id, view in views.items():
        if player_id in members and isinstance(view, dict) and view.get("gameId") == room.game_id and isinstance(view.get("players"), list) and isinstance(view.get("history"), list) and len(view["players"]) <= maximum:
            next_views[player_id] = {**view, "players": canonical, "history": view["history"][-MAX_HISTORY:]}
    if room.is_private() and not all(p["id"] in next_views for p in canonical):
        raise HTTPException(409, "Private-information protection is missing for one or more players")
    room.session, room.session_views = next_session, next_views
    event = emit_session(room)
    return {"ok": True, "sequence": event["sequence"]}


@app.post("/api/rooms/{code}/message")
async def post_message(code: str, request: Request):
    room, body = get_room(code), await read_body(request)
    client_id = safe_text(body.get("clientId"), 100)
    player = next((p for p in room.session["players"] if p["id"] == client_id), None)
    if not player:
        raise HTTPException(403, "Unknown player")
    message = body.get("message")
    if not isinstance(message, dict) or not isinstance(message.get("type"), str):
        raise HTTPException(400, "Invalid message")
    kind, request_id = message["type"], safe_text(message.get("requestId"), 100)
    if request_id and request_id in room.request_cache[client_id]:
        return {"ok": True, **room.request_cache[client_id][request_id], "deduplicated": True}
    if kind in ("chat", "emote"):
        times = room.message_times[client_id]
        while times and time.time() - times[0] >= 5:
            times.popleft()
        if len(times) >= 12:
            raise HTTPException(429, "You are sending messages too quickly")
        times.append(time.time())
    if kind == "ready":
        if room.session["status"] != "lobby":
            raise HTTPException(409, "Readiness is locked after the match starts")
        if player.get("kind") == "bot":
            raise HTTPException(403, "Only human players can change readiness")
        player["ready"] = bool(message.get("ready"))
        event = emit_session(room)
        ack = {"sequence": event["sequence"], "eventId": event["message"]["eventId"], "ready": player["ready"]}
        if request_id: room.request_cache[client_id][request_id] = ack
        return {"ok": True, **ack}
    if kind == "chat":
        text = safe_text(message.get("text"), 200)
        if not text: raise HTTPException(400, "Message cannot be empty")
        actor = safe_text(message.get("player"), 100)
        bot_actor = actor and any(p["id"] == actor and p.get("kind") == "bot" for p in room.session["players"])
        if actor and actor != client_id and not (client_id == room.host_id and bot_actor):
            raise HTTPException(403, "Cannot speak as another player")
        normalized = {"type": "chat", "player": actor if bot_actor else client_id, "text": text,
                      "ts": now_ms(), "presetId": safe_text(message.get("presetId"), 40) or None, "senderId": client_id}
    elif kind == "emote":
        emoji = safe_text(message.get("emoji"), 12)
        if not emoji or len(emoji) > 4: raise HTTPException(400, "Invalid reaction")
        normalized = {"type": "emote", "player": client_id, "emoji": emoji, "ts": now_ms(), "senderId": client_id}
    elif kind == "move.accepted":
        if client_id != room.host_id: raise HTTPException(403, "Only the room coordinator can publish accepted moves")
        move = message.get("move")
        if not isinstance(move, dict) or not any(p["id"] == move.get("player") for p in room.session["players"]) or not isinstance(move.get("type"), str):
            raise HTTPException(400, "Invalid accepted move")
        normalized = {"type": kind, "move": move, "senderId": client_id}
    elif kind == "move.rejected":
        if client_id != room.host_id: raise HTTPException(403, "Only the room coordinator can reject a move")
        target = safe_text(message.get("player"), 100)
        if not any(p["id"] == target for p in room.session["players"]): raise HTTPException(400, "Unknown rejected-move recipient")
        normalized = {"type": kind, "player": target, "message": safe_text(message.get("message"), 160) or "Move rejected", "senderId": client_id}
    elif kind == "move":
        if room.session["status"] != "playing": raise HTTPException(409, "Match is not active")
        if client_id == room.host_id: raise HTTPException(409, "Host must dispatch its own moves locally")
        move = message.get("move")
        if not isinstance(move, dict) or move.get("player") != client_id or not isinstance(move.get("type"), str):
            raise HTTPException(403, "Move sender does not match the connected player")
        if room.host_id not in room.clients: raise HTTPException(503, "Room coordinator is offline; reconnect the host before moving")
        if len(json.dumps(move).encode()) > 16_000: raise HTTPException(413, "Move payload is too large")
        normalized = {"type": "move", "move": {**move, "player": client_id}, "senderId": client_id}
    elif kind == "resync_request":
        after = max(0, int(message.get("afterSequence") or 0))
        target_queues = room.clients.get(client_id, set())
        for event in room.events:
            if event["sequence"] > after:
                outgoing = dict(event["message"])
                if outgoing.get("type") == "session": outgoing["session"] = (event.get("views") or {}).get(client_id) or room.view_for(client_id, outgoing.get("session"))
                for queue in target_queues:
                    queue.put_nowait(outgoing)
        return {"ok": True, "sequence": room.sequence}
    elif kind in ("join", "leave", "host"):
        raise HTTPException(403, "Room membership events are server-controlled")
    else:
        raise HTTPException(400, "Unsupported message type")
    if kind in ("move", "move.rejected"):
        target_id = room.host_id if kind == "move" else normalized["player"]
        direct = {**normalized, "version": 1, "sequence": 0, "eventId": f"{room.code}:direct:{request_id or uuid.uuid4()}"}
        for queue in room.clients.get(target_id, set()):
            queue.put_nowait(direct)
        ack = {"sequence": room.sequence, "eventId": direct["eventId"]}
    else:
        event = room.emit(normalized)
        ack = {"sequence": event["sequence"], "eventId": event["message"]["eventId"]}
    if request_id:
        room.request_cache[client_id][request_id] = ack
        if len(room.request_cache[client_id]) > 500:
            room.request_cache[client_id].pop(next(iter(room.request_cache[client_id])))
    return {"ok": True, **ack}


@app.on_event("startup")
async def cleanup_loop():
    async def cleaner():
        while True:
            await asyncio.sleep(60)
            cutoff = time.time() - 60 * 30
            for code, room in list(rooms.items()):
                if room.last_active < cutoff or (room.session["status"] == "lobby" and not room.clients and time.time() - room.last_active > 180):
                    rooms.pop(code, None)
    asyncio.create_task(cleaner())


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(os.getenv("PORT", "8787")), reload=False)
