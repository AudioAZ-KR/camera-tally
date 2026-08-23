"""
탈리 중계 서버
- HTTP  : web/ 폴더의 탈리 페이지 서빙
- WS    : /ws  브릿지(ATEM 상태 송신) <-> 스마트폰(수신) 중계
실행: ./venv/bin/python server.py  (기본 포트 8080)
"""
import asyncio, json, os, sys, http, mimetypes
from websockets.asyncio.server import serve
from websockets.http11 import Response
from websockets.datastructures import Headers

PORT = int(sys.argv[1]) if len(sys.argv) > 1 else int(os.environ.get("PORT", 8080))
WEB_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "web")

rooms = {}          # room -> set(websocket)
state = {}          # room -> {"program": n, "preview": n, "online": bool}

async def broadcast(room):
    msg = json.dumps({"type": "tally", **state.get(room, {"program": 0, "preview": 0, "online": False})})
    dead = []
    for ws in rooms.get(room, set()):
        try:
            await ws.send(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        rooms[room].discard(ws)

async def handler(ws):
    room = None
    is_bridge = False
    try:
        async for raw in ws:
            try:
                data = json.loads(raw)
            except Exception:
                continue
            t = data.get("type")
            if t == "join":
                room = str(data.get("room", "")).strip().upper() or "DEFAULT"
                is_bridge = data.get("role") == "bridge"
                rooms.setdefault(room, set()).add(ws)
                if is_bridge:
                    state[room] = {"program": 0, "preview": 0, "online": True}
                    print(f"[bridge] joined room {room}")
                    await broadcast(room)
                else:
                    print(f"[phone ] joined room {room} ({len(rooms[room])} clients)")
                    await ws.send(json.dumps({"type": "tally", **state.get(room, {"program": 0, "preview": 0, "online": False})}))
            elif t == "tally" and is_bridge and room:
                state[room] = {"program": int(data.get("program", 0)),
                               "preview": int(data.get("preview", 0)), "online": True}
                print(f"[{room}] PGM={state[room]['program']} PVW={state[room]['preview']}")
                await broadcast(room)
            elif t == "ping":
                await ws.send('{"type":"pong"}')
    finally:
        if room:
            rooms.get(room, set()).discard(ws)
            if is_bridge:
                state[room] = {"program": 0, "preview": 0, "online": False}
                print(f"[bridge] left room {room}")
                await broadcast(room)

def process_request(connection, request):
    path = request.path.split("?")[0]
    if path == "/ws":
        return None  # WebSocket 업그레이드
    if path == "/":
        path = "/index.html"
    if path == "/bridge.py":
        fp = os.path.join(os.path.dirname(WEB_DIR), "bridge.py")
    else:
        fp = os.path.normpath(os.path.join(WEB_DIR, path.lstrip("/")))
    if not (fp.startswith(WEB_DIR) or path == "/bridge.py") or not os.path.isfile(fp):
        return connection.respond(http.HTTPStatus.NOT_FOUND, "not found")
    ctype = mimetypes.guess_type(fp)[0] or "application/octet-stream"
    with open(fp, "rb") as f:
        body = f.read()
    if ctype.startswith("text/") or "javascript" in ctype:
        ctype += "; charset=utf-8"
    headers = Headers([("Content-Type", ctype), ("Content-Length", str(len(body))),
                       ("Cache-Control", "no-cache"), ("Connection", "close")])
    return Response(200, "OK", headers, body)

async def main():
    async with serve(handler, "0.0.0.0", PORT, process_request=process_request):
        print(f"탈리 서버 실행 중: http://0.0.0.0:{PORT}  (ws://…/ws)")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
