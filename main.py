import os
import re
import json
import asyncio
import aiohttp
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent, CustomEvent

AUTH_KEY = os.environ.get("RELAY_AUTH_KEY", "#hogcranked")

async def health_check(request):
    return web.json_response({"status": "ok", "service": "omnifeed-relay"})

async def resolve_handle_from_user_id(session: aiohttp.ClientSession, user_id: str) -> str | None:
    try:
        url = f"https://webcast.tiktok.com/webcast/user/profile/?user_id={user_id}&aid=1988"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                body = await resp.text()
                try:
                    parsed = json.loads(body)
                    user_obj = parsed.get("data", {}).get("user") or parsed.get("user")
                    if isinstance(user_obj, dict):
                        h = user_obj.get("display_id") or user_obj.get("unique_id") or user_obj.get("uniqueId")
                        if h and not str(h).isdigit():
                            return str(h)
                except Exception:
                    pass
                m = re.search(r'"(?:display_id|unique_id|uniqueId)":\s*"([^"]+)"', body)
                if m and not m.group(1).isdigit():
                    return m.group(1)
    except Exception:
        pass

    try:
        url = f"https://www.tiktok.com/@{user_id}"
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                body = await resp.text()
                m = re.search(r'"uniqueId":"([^"]+)"', body)
                if m:
                    return m.group(1)
    except Exception:
        pass

    return None

async def monitor_cohosts(client: TikTokLiveClient, handle: str, ws: web.WebSocketResponse):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    prompted_hosts = set()
    clean_lower = handle.lower().replace("@", "").strip()

    for _ in range(30):
        if client.connected and getattr(client, "room_id", None):
            break
        await asyncio.sleep(0.5)

    room_id = getattr(client, "room_id", None)
    if not room_id:
        return

    async with aiohttp.ClientSession(headers=headers) as session:
        while client.connected and not ws.closed:
            try:
                url = f"https://webcast.tiktok.com/webcast/room/info/?room_id={room_id}&aid=1988"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
                    if resp.status == 200:
                        raw_body = await resp.text()
                        parsed = json.loads(raw_body)
                        data = parsed.get("data") if isinstance(parsed.get("data"), dict) else parsed

                        owner_id = str(data.get("owner", {}).get("id") or data.get("owner_user_id") or "")
                        link_mic = data.get("link_mic") or data.get("linkMic") or {}
                        battle_scores = link_mic.get("battle_scores") or link_mic.get("battleScores") or []

                        rival_user_ids = []
                        if isinstance(battle_scores, list):
                            for score_entry in battle_scores:
                                if isinstance(score_entry, dict):
                                    u_id = str(score_entry.get("user_id") or score_entry.get("userId") or "")
                                    if u_id and u_id != owner_id:
                                        rival_user_ids.append(u_id)

                        candidates = []
                        for rival_id in rival_user_ids:
                            candidate_handle = None
                            idx = raw_body.find(rival_id)
                            if idx != -1:
                                start = max(0, idx - 1200)
                                end = min(len(raw_body), idx + 1200)
                                slice_str = raw_body[start:end]
                                matches = re.findall(r'"(?:display_id|displayId|unique_id|uniqueId)":\s*"([^"]+)"', slice_str)
                                for c in matches:
                                    if c and not c.isdigit() and c.lower() != clean_lower:
                                        candidate_handle = c
                                        break

                            if not candidate_handle:
                                candidate_handle = await resolve_handle_from_user_id(session, rival_id)

                            if candidate_handle and candidate_handle.lower() != clean_lower:
                                if candidate_handle.lower() not in [c.lower() for c in candidates]:
                                    candidates.append(candidate_handle)

                        for cohost in candidates:
                            if cohost.lower() not in prompted_hosts:
                                prompted_hosts.add(cohost.lower())
                                await ws.send_json({
                                    "event": "cohost_detected",
                                    "handle": cohost
                                })

            except Exception:
                pass

            await asyncio.sleep(6)

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
    known_cohosts = set()

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("action") == "connect":
                    handle = data.get("handle", "").replace("@", "").strip()
                    if not handle:
                        continue

                    if probe_task:
                        probe_task.cancel()
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

                    # Catch raw unparsed events to scrape LinkLayer directly
                    @client.on(CustomEvent)
                    async def on_custom(event: CustomEvent):
                        try:
                            raw_bytes = getattr(event, "raw_data", b"")
                            if b"WebcastLinkLayerMessage" in raw_bytes:
                                text_slice = raw_bytes.decode("latin1", errors="ignore")
                                user_matches = re.findall(r'([a-zA-Z0-9_\.]{3,24})', text_slice)
                                for u in user_matches:
                                    if u.lower() != handle.lower() and u.lower() not in known_cohosts:
                                        if len(u) >= 4 and not u.isdigit() and "Webcast" not in u:
                                            known_cohosts.add(u.lower())
                                            await ws.send_json({
                                                "event": "cohost_detected",
                                                "handle": u
                                            })
                        except Exception:
                            pass

                    task = asyncio.create_task(client.start())
                    probe_task = asyncio.create_task(monitor_cohosts(client, handle, ws))

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
