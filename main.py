import os
import asyncio
import json
import http
import websockets
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

ALLOWED_ORIGINS = {
    "https://matt-cstech-ftw.github.io",
    "http://localhost",
    "http://127.0.0.1",
}

async def process_request(connection, request):
    # Respond with 200 OK to plain HTTP pings (UptimeRobot, health checks)
    if request.headers.get("Upgrade", "").lower() != "websocket":
        response_body = b'{"status": "ok", "service": "omnifeed-relay"}'
        headers = [
            ("Content-Type", "application/json"),
            ("Content-Length", str(len(response_body))),
            ("Connection", "close"),
        ]
        return connection.respond(http.HTTPStatus.OK, headers, response_body)

    # Origin guard for WebSockets
    origin = request.headers.get("Origin", "")
    if origin and not any(origin.startswith(allowed) for allowed in ALLOWED_ORIGINS):
        return connection.respond(http.HTTPStatus.FORBIDDEN, [("Connection", "close")], b"Forbidden")

    return None

async def handler(websocket):
    client = None
    task = None
    try:
        async for message in websocket:
            data = json.loads(message)
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
                        await websocket.send(json.dumps({
                            "event": "connected",
                            "handle": handle
                        }))
                    except Exception:
                        pass

                @client.on(CommentEvent)
                async def on_comment(event: CommentEvent):
                    try:
                        await websocket.send(json.dumps({
                            "event": "chat",
                            "user": event.user.nickname or event.user.unique_id,
                            "comment": event.comment,
                            "isMod": event.user.is_moderator,
                            "host": handle
                        }))
                    except Exception:
                        pass

                @client.on(GiftEvent)
                async def on_gift(event: GiftEvent):
                    if event.gift.streakable and not event.repeat_end:
                        return
                    try:
                        await websocket.send(json.dumps({
                            "event": "gift",
                            "user": event.user.nickname or event.user.unique_id,
                            "giftName": event.gift.name,
                            "count": event.repeat_count,
                            "diamondCount": event.gift.diamond_count,
                            "host": handle
                        }))
                    except Exception:
                        pass

                task = asyncio.create_task(client.start())

    except (websockets.exceptions.ConnectionClosed, asyncio.CancelledError):
        pass
    finally:
        if task:
            task.cancel()
        if client and client.connected:
            await client.disconnect()

async def main():
    port = int(os.environ.get("PORT", 8080))
    async with websockets.serve(
        handler,
        "0.0.0.0",
        port,
        process_request=process_request,
    ):
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
