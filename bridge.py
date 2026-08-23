"""
ATEM 브릿지: ATEM Mini의 PGM/PVW 상태를 탈리 서버로 전송
실행: ./venv/bin/python bridge.py --atem 192.168.10.240 --server ws://localhost:8080/ws --room STUDIO
테스트(ATEM 없이): ./venv/bin/python bridge.py --sim --room STUDIO
"""
import argparse, asyncio, json, random, time
from websockets.asyncio.client import connect

ap = argparse.ArgumentParser()
ap.add_argument("--atem", default="192.168.10.240", help="ATEM Mini IP")
ap.add_argument("--server", default="ws://localhost:8080/ws")
ap.add_argument("--room", default="STUDIO")
ap.add_argument("--sim", action="store_true", help="ATEM 없이 시뮬레이션 (3초마다 랜덤 전환)")
args = ap.parse_args()

async def read_state_sim():
    pgm, pvw = 1, 2
    while True:
        yield pgm, pvw
        await asyncio.sleep(3)
        pgm, pvw = pvw, random.choice([i for i in range(1, 5) if i != pvw])

async def read_state_atem():
    import PyATEMMax
    sw = PyATEMMax.ATEMMax()
    sw.connect(args.atem)
    print(f"ATEM {args.atem} 연결 시도…")
    while not sw.waitForConnection(infinite=False, timeout=3):
        print("ATEM 연결 대기 중…")
    print("ATEM 연결됨")
    last = None
    while True:
        if not sw.connected:
            print("ATEM 연결 끊김, 재연결…")
            sw.connect(args.atem); sw.waitForConnection()
        pgm = int(sw.programInput[0].videoSource.value)
        pvw = int(sw.previewInput[0].videoSource.value)
        if (pgm, pvw) != last:
            last = (pgm, pvw)
            yield pgm, pvw
        await asyncio.sleep(0.05)

async def main():
    while True:
        try:
            async with connect(args.server) as ws:
                await ws.send(json.dumps({"type": "join", "role": "bridge", "room": args.room}))
                print(f"서버 연결됨: {args.server}  방={args.room}")
                src = read_state_sim() if args.sim else read_state_atem()
                async for pgm, pvw in src:
                    print(f"PGM={pgm} PVW={pvw}")
                    await ws.send(json.dumps({"type": "tally", "program": pgm, "preview": pvw}))
        except Exception as e:
            print(f"오류: {e} — 3초 후 재시도")
            await asyncio.sleep(3)

asyncio.run(main())
