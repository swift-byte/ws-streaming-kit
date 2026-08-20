import asyncio, websockets

async def handler(ws):
    path = ws.request.path
    try:
        if path == "/echo":
            async for m in ws:
                await ws.send(m)
        elif path == "/close1000":
            await ws.send(b"bye")
            await asyncio.sleep(0.2)
            await ws.close(code=1000)
        elif path == "/close1011":
            await ws.send(b"err")
            await asyncio.sleep(0.2)
            await ws.close(code=1011, reason="boom")
        elif path == "/cookie":
            await ws.send((ws.request.headers.get("Cookie") or "NONE").encode())
            async for m in ws:
                pass
        elif path == "/push3":
            await ws.send(b"one")
            await ws.send(b"two")
            await ws.send("three")
            await asyncio.sleep(5)
            await ws.close(code=1000)
        elif path == "/sink":
            got = []
            async for m in ws:
                b = m if isinstance(m, bytes) else m.encode()
                got.append(b.hex())
                if b == b"\x31":
                    await ws.send("GOT:" + ",".join(got))
                    await asyncio.sleep(0.3)
                    await ws.close(code=1000)
                    return
        elif path == "/msg-then-silent":
            await asyncio.sleep(0.3)
            await ws.send(b"hello")
            await asyncio.sleep(3.5)
            await ws.close(code=1000)
        elif path == "/push-after-3":
            await asyncio.sleep(3.2)
            await ws.send(b"late")
            await asyncio.sleep(0.3)
            await ws.close(code=1000)
        elif path == "/burst-close":
            for i in range(5):
                await ws.send(bytes([i]))
            await ws.close(code=1000)
        elif path == "/silent":
            await asyncio.sleep(30)
        else:
            await ws.close(code=1008)
    except websockets.ConnectionClosed:
        pass

async def raw_silent(reader, writer):
    # TCP-акцепт без WS-рукопожатия — для теста таймаута
    try:
        await asyncio.sleep(30)
    finally:
        writer.close()

async def main():
    srv = await asyncio.start_server(raw_silent, "127.0.0.1", 8902)
    async with websockets.serve(handler, "127.0.0.1", 8901):
        async with srv:
            await asyncio.Future()

asyncio.run(main())
