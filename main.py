import os
import json
import asyncio
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

ALLOWED_ORIGINS = {
    "https://matt-cstech-ftw.github.io",
    "http://localhost",
    "http://127.0.0.1",
}

async def health_check(request):
    return web.json_response({"status": "ok", "service": "omnifeed-relay"})

async def websocket_handler(request):
    origin = request.headers.get("Origin", "")
    if origin and not any(origin.startswith(allowed) for allowed in ALLOWED_ORIGINS):
        return web.Response(status=403, text="Forbidden")

    ws = web.WebSocketResponse()
    await ws.prepare(request)

    client = None
    task = None
    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("action") == "connect":
                    handle = data.get("handle", "").replace("@", "").strip()
                    if not handle:
                        continue

                    if client and client.connected:
                        await client.disconnect()

                    client = TikTokLiveClient(unique_id=handle)

                    @client.on(ConnectEvent)
                    async def on_connect(event: ConnectEvent):
                        try:
                            await ws.send_json({
                                "event": "connected",
                                "handle": handle
                            })
                        except Exception:
                            pass

                    @client.on(CommentEvent)
                    async def on_comment(event: CommentEvent):
                        try:
                            await ws.send_json({
                                "event": "chat",
                                "user": event.user.nickname or event.user.unique_id,
                                "comment": event.comment,
                                "isMod": event.user.is_moderator,
                                "host": handle
                            })
                        except Exception:
                            pass

                    @client.on(GiftEvent)
                    async def on_gift(event: GiftEvent):
                        if event.gift.streakable and not event.repeat_end:
                            return
                        try:
                            await ws.send_json({
                                "event": "gift",
                                "user": event.user.nickname or event.user.unique_id,
                                "giftName": event.gift.name,
                                "count": event.repeat_count,
                                "diamondCount": event.gift.diamond_count,
                                "host": handle
                            })
                        except Exception:
                            pass

                    task = asyncio.create_task(client.start())

            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        if task:
            task.cancel()
        if client and client.connected:
            await client.disconnect()

    return ws

def create_app():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/ws", websocket_handler)
    # Catch-all to allow connecting directly to the root websocket URL
    async def route_selector(request):
        if request.headers.get("Upgrade", "").lower() == "websocket":
            return await websocket_handler(request)
        return await health_check(request)
    app.router.add_route("*", "/{tail:.*}", route_selector)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(create_app(), host="0.0.0.0", port=port)
