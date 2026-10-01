import os
import re
import json
import asyncio
import aiohttp
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

AUTH_KEY = os.environ.get("RELAY_AUTH_KEY", "#hogcranked")

async def health_check(request):
    return web.json_response({"status": "ok", "service": "omnifeed-relay"})

async def check_cohosts(handle: str, ws: web.WebSocketResponse):
    """Probes TikTok webcast API without CORS barriers and informs the frontend."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            # 1. Resolve room ID
            async with session.get(f"https://www.tiktok.com/@{handle}/live") as resp:
                text = await resp.text()
                room_match = re.search(r'"roomId":"(\d+)"', text)
                if not room_match:
                    return
                room_id = room_match.group(1)

            # 2. Get room info to extract co-hosts / battle rivals
            async with session.get(f"https://webcast.tiktok.com/webcast/room/info/?room_id={room_id}&aid=1988") as resp:
                if resp.status != 200:
                    return
                data = await resp.json()
                room_data = data.get("data", data)
                owner_id = str(room_data.get("owner", {}).get("id") or room_data.get("owner_user_id", ""))
                
                link_mic = room_data.get("link_mic") or room_data.get("linkMic", {})
                battle_scores = link_mic.get("battle_scores") or link_mic.get("battleScores") or []
                
                rival_ids = []
                for score in battle_scores:
                    uid = str(score.get("user_id") or score.get("userId") or "")
                    if uid and uid != owner_id:
                        rival_ids.append(uid)

                # Search text for user handles matching rival user IDs
                body_str = json.dumps(room_data)
                for rival_id in rival_ids:
                    rival_handle = None
                    idx = body_str.find(rival_id)
                    if idx != -1:
                        start = max(0, idx - 1200)
                        end = min(len(body_str), idx + 1200)
                        slice_str = body_str[start:end]
                        matches = re.findall(r'"(?:display_id|displayId|unique_id|uniqueId)":\s*"([^"]+)"', slice_str)
                        for m in matches:
                            if m and not m.isdigit() and m.lower() != handle.lower():
                                rival_handle = m
                                break

                    if rival_handle:
                        await ws.send_json({
                            "event": "cohost_detected",
                            "handle": rival_handle
                        })
    except Exception:
        pass

async def websocket_handler(request):
    origin = request.headers.get("Origin", "")
    if origin and ("github.io" not in origin and "localhost" not in origin and "127.0.0.1" not in origin):
        return web.Response(status=403, text="Forbidden")

    token = request.query.get("token", "")
    if token != AUTH_KEY:
        return web.Response(status=401, text="Unauthorized: Invalid Secret Key")

    ws = web.WebSocketResponse(heartbeat=25.0)
    await ws.prepare(request)

    client = None
    task = None
    probe_task = None
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
                            await ws.send_json({"event": "connected", "handle": handle})
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
                    # Kick off background co-host probe on the server
                    probe_task = asyncio.create_task(check_cohosts(handle, ws))

            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        if probe_task:
            probe_task.cancel()
        if task:
            task.cancel()
        if client and client.connected:
            await client.disconnect()

    return ws

def create_app():
    app = web.Application()
    app.router.add_get("/", health_check)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("", websocket_handler)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(create_app(), host="0.0.0.0", port=port)
