"""
탈리 중계 서버 (aiohttp)
- HTTP : web/ 폴더의 탈리 페이지 서빙, /bridge.py 다운로드
- WS   : /ws  브릿지(ATEM 상태 송신) <-> 스마트폰(수신) 중계, 방 코드별 격리
- 접속 카메라 명단(roster)을 추적해 호스트(브릿지)로 전송
- 호스트 공지 메시지(msg)·타이머(timer)를 방 단위로 보관하고 폰에 브로드캐스트 (늦게 접속한 폰도 현재 상태 수신)
실행: python server.py  (PORT 환경변수, 기본 8080)
"""
import asyncio, json, os, time
from aiohttp import web, WSMsgType

BASE = os.path.dirname(os.path.abspath(__file__))
WEB_DIR = os.path.join(BASE, "web")
PORT = int(os.environ.get("PORT", 8080))

rooms: dict[str, set] = {}     # room -> set(ws)  (탈리 브로드캐스트 대상: 폰 + 브릿지)
bridges: dict[str, set] = {}   # room -> set(ws)  (호스트/브릿지 연결)
cams: dict[str, dict] = {}     # room -> {ws: cam_number}  (접속한 폰)
seen: dict = {}                # ws -> 마지막 수신 시각 (응답 없는 폰 정리용)
STALE_SEC = 25                 # 이 시간 동안 아무 메시지(ping 포함)가 없으면 접속 해제로 간주
state: dict[str, dict] = {}    # room -> {"program","preview","online"}
notes: dict[str, dict] = {}    # room -> {"text","ts"}              (공지 메시지)
timers: dict[str, dict] = {}   # room -> {"running","end","remain","target"} (카운트다운 + 목표 시각, 서버 시각 기준 ms)
OFFLINE = {"program": 0, "preview": 0, "online": False}

def now_ms(): return int(time.time() * 1000)

def msg_msg(room):
    n = notes.get(room, {"text": "", "ts": 0})
    return json.dumps({"type": "msg", "text": n["text"], "ts": n["ts"]})

EMPTY_TIMER = {"running": False, "end": 0, "remain": 0, "target": 0}

def timer_msg(room):
    t = {**EMPTY_TIMER, **timers.get(room, {})}
    return json.dumps({"type": "timer", "running": t["running"], "end": t["end"], "remain": t["remain"],
                       "target": t["target"], "now": now_ms()})

def tally_msg(room):
    return json.dumps({"type": "tally", **state.get(room, OFFLINE)})

def roster(room):
    return sorted(set(c for c in cams.get(room, {}).values() if c))

async def broadcast(room, msg=None):
    msg = msg or tally_msg(room)
    for ws in list(rooms.get(room, ())):
        try:
            await ws.send_str(msg)
        except Exception:
            rooms[room].discard(ws)

async def broadcast_roster(room):
    msg = json.dumps({"type": "roster", "cams": roster(room)})
    for ws in list(bridges.get(room, ())):
        try:
            await ws.send_str(msg)
        except Exception:
            bridges[room].discard(ws)

async def ws_handler(request):
    ws = web.WebSocketResponse(heartbeat=10)
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
            seen[ws] = time.time()
            t = data.get("type")
            if t == "leave":                     # 폰이 명시적으로 나감 (번호 변경/페이지 닫기) → 즉시 현황 갱신
                break
            if t == "join":
                room = str(data.get("room", "")).strip().upper() or "DEFAULT"
                is_bridge = data.get("role") == "bridge"
                rooms.setdefault(room, set()).add(ws)
                if is_bridge:
                    bridges.setdefault(room, set()).add(ws)
                    state[room] = {"program": 0, "preview": 0, "online": True}
                    print(f"[bridge] joined {room}", flush=True)
                    await broadcast(room)
                    await ws.send_str(json.dumps({"type": "roster", "cams": roster(room)}))
                    await ws.send_str(msg_msg(room)); await ws.send_str(timer_msg(room))
                else:
                    cam = int(data.get("cam", 0) or 0)
                    cams.setdefault(room, {})[ws] = cam
                    print(f"[phone ] joined {room} cam={cam} ({len(cams[room])} cams)", flush=True)
                    await ws.send_str(tally_msg(room))
                    await ws.send_str(msg_msg(room))
                    await ws.send_str(timer_msg(room))
                    await broadcast_roster(room)
            elif t == "tally" and is_bridge and room:
                state[room] = {"program": int(data.get("program", 0)),
                               "preview": int(data.get("preview", 0)), "online": True}
                print(f"[{room}] PGM={state[room]['program']} PVW={state[room]['preview']}", flush=True)
                await broadcast(room)
            elif t == "msg" and is_bridge and room:
                text = str(data.get("text", ""))[:200]
                notes[room] = {"text": text, "ts": now_ms()}
                print(f"[{room}] MSG: {text}", flush=True)
                await broadcast(room, msg_msg(room))
            elif t == "timer" and is_bridge and room:
                act = data.get("action"); cur = {**EMPTY_TIMER, **timers.get(room, {})}
                tgt = cur["target"]
                if act == "set":      # 새 카운트다운 설정(정지 상태)
                    cur = {"running": False, "end": 0, "remain": max(0, int(data.get("seconds", 0))) * 1000, "target": tgt}
                elif act == "start" and not cur["running"] and cur["remain"] > 0:
                    cur = {"running": True, "end": now_ms() + cur["remain"], "remain": cur["remain"], "target": tgt}
                elif act == "pause" and cur["running"]:
                    cur = {"running": False, "end": 0, "remain": max(0, cur["end"] - now_ms()), "target": tgt}
                elif act == "reset":
                    cur = {"running": False, "end": 0, "remain": max(0, int(data.get("seconds", 0))) * 1000, "target": tgt}
                elif act == "target":         # 목표 시각 설정 (epoch ms, 0이면 해제)
                    cur["target"] = max(0, int(data.get("target_ms", 0)))
                timers[room] = cur
                print(f"[{room}] TIMER {act}: {cur}", flush=True)
                await broadcast(room, timer_msg(room))
            elif t == "ping":
                await ws.send_str('{"type":"pong"}')
    finally:
        seen.pop(ws, None)
        if room:
            rooms.get(room, set()).discard(ws)
            if is_bridge:
                bridges.get(room, set()).discard(ws)
                state[room] = dict(OFFLINE)
                print(f"[bridge] left {room}", flush=True)
                await broadcast(room)
            else:
                cams.get(room, {}).pop(ws, None)
                print(f"[phone ] left {room}", flush=True)
                await broadcast_roster(room)
    return ws

async def reaper(app):
    """응답 없는 폰을 주기적으로 정리해 접속 현황을 최신으로 유지"""
    while True:
        await asyncio.sleep(5)
        now = time.time()
        for room, d in list(cams.items()):
            stale = [w for w in list(d) if now - seen.get(w, 0) > STALE_SEC]
            for w in stale:
                d.pop(w, None); rooms.get(room, set()).discard(w); seen.pop(w, None)
                try: await w.close()
                except Exception: pass
            if stale:
                print(f"[{room}] 응답 없는 폰 {len(stale)}대 정리", flush=True)
                await broadcast_roster(room)

async def start_bg(app):
    app["reaper"] = asyncio.create_task(reaper(app))

async def index(request):
    return web.FileResponse(os.path.join(WEB_DIR, "index.html"), headers={"Cache-Control": "no-cache"})

async def bridge_file(request):
    return web.FileResponse(os.path.join(BASE, "bridge.py"),
                            headers={"Content-Type": "text/x-python; charset=utf-8",
                                     "Content-Disposition": "attachment; filename=bridge.py"})

async def health(request):
    return web.json_response({"ok": True, "rooms": len(rooms)})

def make_app():
    """aiohttp Application은 이벤트 루프에 묶이므로, 내장 서버(호스트 앱)에서는 시작할 때마다 새로 만든다"""
    a = web.Application()
    a.on_startup.append(start_bg)
    a.router.add_get("/", index)
    a.router.add_get("/ws", ws_handler)
    a.router.add_get("/bridge.py", bridge_file)
    a.router.add_get("/health", health)
    a.router.add_static("/", WEB_DIR, show_index=False)
    return a

app = make_app()

if __name__ == "__main__":
    print(f"탈리 서버 실행 중: http://0.0.0.0:{PORT}", flush=True)
    web.run_app(app, host="0.0.0.0", port=PORT, print=None)
