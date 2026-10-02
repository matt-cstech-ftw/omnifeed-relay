import os
import re
import json
import asyncio
import resource
import aiohttp
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

AUTH_KEY = os.environ.get("RELAY_AUTH_KEY", "#hogcranked")
ADMIN_KEY = os.environ.get("ADMIN_KEY", "Flock@1017")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_ALERT_WEBHOOK", "")

RESOLVED_CACHE = {}
ACTIVE_CLIENTS = set()
ACTIVE_TRACKED_HANDLES = set()

# Alert state trackers (0: normal, 1: 15+ users, 2: 25+ users)
CURRENT_TIER = 0
LAST_RAM_ALERT = 0

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}

async def send_discord_alert(title: str, description: str, color: int):
    if not DISCORD_WEBHOOK_URL:
        return
    payload = {
        "username": "OmniFeed Sentinel",
        "avatar_url": "https://raw.githubusercontent.com/twitter/twemoji/master/assets/72x72/1f50a.png",
        "embeds": [{
            "title": title,
            "description": description,
            "color": color,
            "footer": {"text": "OmniFeed Relay Telemetry • Render"}
        }]
    }
    try:
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=4))
    except Exception:
        pass

async def evaluate_system_health():
    global CURRENT_TIER, LAST_RAM_ALERT

    active_count = len(ACTIVE_CLIENTS)
    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    now = asyncio.get_event_loop().time()

    # 1. RAM Usage Check (Threshold: 420MB out of 512MB)
    if mem_mb >= 420 and (now - LAST_RAM_ALERT > 600):
        LAST_RAM_ALERT = now
        asyncio.create_task(send_discord_alert(
            "🚨 Critical Relay Memory Usage",
            f"**RAM:** `{mem_mb:.1f} MB / 512 MB`\n"
            f"**Active Clients:** `{active_count}`\n"
            f"Approaching Render container memory ceiling.",
            0xFF0033
        ))

    # 2. Concurrency Escalation & De-escalation
    if active_count >= 25 and CURRENT_TIER < 2:
        CURRENT_TIER = 2
        asyncio.create_task(send_discord_alert(
            "🔥 Peak Concurrency Warning (25+ Users)",
            f"**Active Clients:** `{active_count}`\n"
            f"**Tracked TikTok Streams:** `{len(ACTIVE_TRACKED_HANDLES)}`\n"
            f"**RAM Usage:** `{mem_mb:.1f} MB`",
            0xFF5500
        ))
    elif active_count >= 15 and CURRENT_TIER < 1:
        CURRENT_TIER = 1
        asyncio.create_task(send_discord_alert(
            "⚠️ Elevated Concurrency Notice (15+ Users)",
            f"**Active Clients:** `{active_count}`\n"
            f"**Tracked TikTok Streams:** `{len(ACTIVE_TRACKED_HANDLES)}`\n"
            f"**RAM Usage:** `{mem_mb:.1f} MB`",
            0xFFCC00
        ))
    elif active_count <= 10 and CURRENT_TIER > 0:
        CURRENT_TIER = 0
        asyncio.create_task(send_discord_alert(
            "✅ Traffic Returned to Normal",
            f"**Active Clients:** `{active_count}`\n"
            f"**RAM Stable:** `{mem_mb:.1f} MB`",
            0x00E676
        ))

async def health_check(request):
    return web.json_response({"status": "ok", "service": "omnifeed-relay"}, headers=CORS_HEADERS)

async def stats_options_handler(request):
    return web.Response(status=204, headers=CORS_HEADERS)

async def stats_handler(request):
    token = request.query.get("token", "")
    if token != ADMIN_KEY:
        return web.Response(status=401, text="Unauthorized: Invalid Admin Token", headers=CORS_HEADERS)

    mem_mb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024
    return web.json_response({
        "status": "ok",
        "concurrent_ws_clients": len(ACTIVE_CLIENTS),
        "active_tiktok_streams": len(ACTIVE_TRACKED_HANDLES),
        "cached_user_ids": len(RESOLVED_CACHE),
        "memory_usage_mb": round(mem_mb, 2),
        "tracked_handles": list(ACTIVE_TRACKED_HANDLES)
    }, headers=CORS_HEADERS)

async def resolve_handle_from_user_id(session: aiohttp.ClientSession, user_id: str) -> str | None:
    if user_id in RESOLVED_CACHE:
        return RESOLVED_CACHE[user_id]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        url = f"https://webcast.tiktok.com/webcast/user/profile/?user_id={user_id}&aid=1988"
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                body = await resp.text()
                try:
                    parsed = json.loads(body)
                    user_obj = parsed.get("data", {}).get("user") or parsed.get("user")
                    if isinstance(user_obj, dict):
                        h = user_obj.get("display_id") or user_obj.get("unique_id") or user_obj.get("uniqueId")
                        if h and not str(h).isdigit():
                            RESOLVED_CACHE[user_id] = str(h)
                            return str(h)
                except Exception:
                    pass
                m = re.search(r'"(?:display_id|unique_id|uniqueId)":\s*"([^"]+)"', body)
                if m and not m.group(1).isdigit():
                    RESOLVED_CACHE[user_id] = m.group(1)
                    return m.group(1)
    except Exception:
        pass

    try:
        url = f"https://www.tiktok.com/@{user_id}"
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                body = await resp.text()
                m = re.search(r'"uniqueId":"([^"]+)"', body)
                if m:
                    RESOLVED_CACHE[user_id] = m.group(1)
                    return m.group(1)
    except Exception:
        pass

    return None

def find_client_room_id(client: TikTokLiveClient) -> str | None:
    for attr in ("_room_id", "room_id"):
        val = getattr(client, attr, None)
        if val:
            return str(val)
    room = getattr(client, "room", None)
    if room:
        val = getattr(room, "id", None) or getattr(room, "room_id", None)
        if val:
            return str(val)
        if isinstance(room, dict):
            val = room.get("id") or room.get("room_id")
            if val:
                return str(val)
    room_info = getattr(client, "room_info", None)
    if isinstance(room_info, dict):
        val = room_info.get("id") or room_info.get("room_id")
        if val:
            return str(val)
    return None

async def monitor_cohosts(client: TikTokLiveClient, handle: str, ws: web.WebSocketResponse):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    prompted_hosts = set()
    clean_lower = handle.lower().replace("@", "").strip()

    for _ in range(30):
        if client.connected:
            break
        await asyncio.sleep(0.5)

    room_id = None
    for _ in range(25):
        room_id = find_client_room_id(client)
        if room_id:
            break
        await asyncio.sleep(0.5)

    print(f"[COHOST-MONITOR] Initialized for @{clean_lower}. Room ID: {room_id}", flush=True)
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

                        rival_user_ids = set()

                        rival_anchor_id = str(link_mic.get("rival_anchor_id") or link_mic.get("rivalAnchorId") or "")
                        if rival_anchor_id and rival_anchor_id not in ("0", owner_id):
                            rival_user_ids.add(rival_anchor_id)

                        def extract_uids(item):
                            if isinstance(item, dict):
                                for k in ("user_id", "id", "userId", "uid", "anchor_id"):
                                    v = str(item.get(k) or "")
                                    if v and v not in ("0", owner_id) and v.isdigit():
                                        rival_user_ids.add(v)
                                if "user" in item and isinstance(item["user"], dict):
                                    extract_uids(item["user"])
                            elif isinstance(item, list):
                                for x in item:
                                    extract_uids(x)
                            elif isinstance(item, (int, str)):
                                s = str(item)
                                if s and s not in ("0", owner_id) and s.isdigit():
                                    rival_user_ids.add(s)

                        extract_uids(link_mic.get("linked_user_list"))
                        extract_uids(link_mic.get("show_user_list"))
                        extract_uids(link_mic.get("battle_scores"))

                        for rival_id in rival_user_ids:
                            candidate_handle = RESOLVED_CACHE.get(rival_id)

                            if not candidate_handle:
                                idx = raw_body.find(rival_id)
                                if idx != -1:
                                    start = max(0, idx - 1200)
                                    end = min(len(raw_body), idx + 1200)
                                    slice_str = raw_body[start:end]
                                    matches = re.findall(r'"(?:display_id|displayId|unique_id|uniqueId)":\s*"([^"]+)"', slice_str)
                                    for c in matches:
                                        if c and not c.isdigit() and c.lower() != clean_lower:
                                            candidate_handle = c
                                            RESOLVED_CACHE[rival_id] = c
                                            break

                            if not candidate_handle:
                                candidate_handle = await resolve_handle_from_user_id(session, rival_id)

                            if candidate_handle and candidate_handle.lower() != clean_lower:
                                if candidate_handle.lower() not in prompted_hosts:
                                    prompted_hosts.add(candidate_handle.lower())
                                    print(f"[COHOST-DETECTED] Emitting @{candidate_handle}", flush=True)
                                    await ws.send_json({
                                        "event": "cohost_detected",
                                        "handle": candidate_handle
                                    })

            except Exception as e:
                print(f"[COHOST-PROBE ERROR] {e}", flush=True)

            await asyncio.sleep(10)

async def websocket_handler(request):
    origin = request.headers.get("Origin", "")
    if origin and ("github.io" not in origin and "localhost" not in origin and "127.0.0.1" not in origin):
        return web.Response(status=403, text="Forbidden", headers=CORS_HEADERS)

    token = request.query.get("token", "")
    if token != AUTH_KEY:
        return web.Response(status=401, text="Unauthorized: Invalid Secret Key", headers=CORS_HEADERS)

    ws = web.WebSocketResponse(heartbeat=25.0)
    await ws.prepare(request)

    ACTIVE_CLIENTS.add(ws)
    asyncio.create_task(evaluate_system_health())

    current_handle = None
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

                    if current_handle and current_handle in ACTIVE_TRACKED_HANDLES:
                        ACTIVE_TRACKED_HANDLES.discard(current_handle)

                    current_handle = handle.lower()
                    ACTIVE_TRACKED_HANDLES.add(current_handle)

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

                    task = asyncio.create_task(client.start())
                    probe_task = asyncio.create_task(monitor_cohosts(client, handle, ws))

            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        ACTIVE_CLIENTS.discard(ws)
        asyncio.create_task(evaluate_system_health())

        if current_handle and current_handle in ACTIVE_TRACKED_HANDLES:
            ACTIVE_TRACKED_HANDLES.discard(current_handle)
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
    app.router.add_route("OPTIONS", "/stats", stats_options_handler)
    app.router.add_get("/stats", stats_handler)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("", websocket_handler)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(create_app(), host="0.0.0.0", port=port)
