import os
import json
import asyncio
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

async def health_check(request):
    return web.json_response({"status": "ok", "service": "omnifeed-relay"})

async def websocket_handler(request):
    # Allow connections from github.io, localhost, or direct app clients
    origin = request.headers.get("Origin", "")
    if origin and ("github.io" not in origin and "localhost" not in origin and "127.0.0.1" not in origin):
        return web.Response(status=403, text="Forbidden")

    ws = web.WebSocketResponse(heartbeat=25.0)
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
    # Health checks on GET /
    app.router.add_get("/", health_check)
    # WebSocket route on /ws
    app.router.add_get("/ws", websocket_handler)
    # Direct fallback for root connections
    app.router.add_get("", websocket_handler)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(create_app(), host="0.0.0.0", port=port)
