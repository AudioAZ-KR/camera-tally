"""
탈리 중계 서버 (aiohttp)
- HTTP : web/ 폴더의 탈리 페이지 서빙, /bridge.py 다운로드
- WS   : /ws  브릿지(ATEM 상태 송신) <-> 스마트폰(수신) 중계, 방 코드별 격리
실행: python server.py  (PORT 환경변수, 기본 8080)
"""
import asyncio, json, os
from aiohttp import web, WSMsgType

BASE = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE, "web")
PORT = int(os.environ.get("PORT", 8080))

rooms: dict[str, set] = {}   # room -> set(ws)
state: dict[str, dict] = {}  # room -> {"program","preview","online"}
OFFLINE = {"program": 0, "preview": 0, "online": False}

def tally_msg(room):
    return json.dumps({"type": "tally", **state.get(room, OFFLINE)})

async def broadcast(room):
    msg = tally_msg(room)
    for ws in list(rooms.get(room, ())):
        try:
            await ws.send_str(msg)
        except Exception:
            rooms[room].discard(ws)

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=20)
    await ws.prepare(request)
    room, is_bridge = None, False
    try:
        async for msg in ws:
            if msg.type != WSMsgType.TEXT:
                continue
            try:
                data = json.loads(msg.data)
            except Exception:
                continue
            t = data.get("type")
            if t == "join":
                room = str(data.get("room", "")).strip().upper() or "DEFAULT"
                is_bridge = data.get("role") == "bridge"
                rooms.setdefault(room, set()).add(ws)
                if is_bridge:
                    state[room] = {"program": 0, "preview": 0, "online": True}
                    print(f"[bridge] joined {room}", flush=True)
                    await broadcast(room)
                else:
                    print(f"[phone ] joined {room} ({len(rooms[room])} clients)", flush=True)
                    await ws.send_str(tally_msg(room))
            elif t == "tally" and is_bridge and room:
                state[room] = {"program": int(data.get("program", 0)),
                               "preview": int(data.get("preview", 0)), "online": True}
                print(f"[{room}] PGM={state[room]['program']} PVW={state[room]['preview']}", flush=True)
                await broadcast(room)
            elif t == "ping":
                await ws.send_str('{"type":"pong"}')
    finally:
        if room:
            rooms.get(room, set()).discard(ws)
            if is_bridge:
                state[room] = dict(OFFLINE)
                print(f"[bridge] left {room}", flush=True)
                await broadcast(room)
    return ws

async def index(request):
    return web.FileResponse(os.path.join(WEB_DIR, "index.html"), headers={"Cache-Control": "no-cache"})

async def bridge_file(request):
    return web.FileResponse(os.path.join(BASE, "bridge.py"),
                            headers={"Content-Type": "text/x-python; charset=utf-8",
                                     "Content-Disposition": "attachment; filename=bridge.py"})

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

app = web.Application()
app.router.add_get("/", index)
app.router.add_get("/ws", ws_handler)
app.router.add_get("/bridge.py", bridge_file)
app.router.add_get("/health", health)
app.router.add_static("/", WEB_DIR, show_index=False)

if __name__ == "__main__":
    print(f"탈리 서버 실행 중: http://0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
