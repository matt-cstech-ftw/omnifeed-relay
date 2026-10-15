import os
import re
import json
import hmac
import hashlib
import asyncio
import resource
import aiohttp
from aiohttp import web
from TikTokLive import TikTokLiveClient
from TikTokLive.events import ConnectEvent, CommentEvent, GiftEvent

ADMIN_KEY = os.environ.get("ADMIN_KEY", "default-admin-key")
DISCORD_WEBHOOK_URL = os.environ.get("DISCORD_ALERT_WEBHOOK", "")
DISCORD_ADMIN_USER_ID = os.environ.get("DISCORD_ADMIN_USER_ID", "710499360298893335")

BLOCKLIST_FILE = "optout_blacklist.json"
ACTIVE_BLOCKLIST = set()

def load_blocklist():
    global ACTIVE_BLOCKLIST
    if os.path.exists(BLOCKLIST_FILE):
        try:
            with open(BLOCKLIST_FILE, "r") as f:
                data = json.load(f)
                ACTIVE_BLOCKLIST = set(data if isinstance(data, list) else [])
        except Exception:
            pass

def save_blocklist():
    try:
        with open(BLOCKLIST_FILE, "w") as f:
            json.dump(list(ACTIVE_BLOCKLIST), f)
    except Exception:
        pass

load_blocklist()

RESOLVED_CACHE = {}
ACTIVE_CLIENTS = set()
WS_HOST_MAP = {}  # ws -> handle

RESOLVE_SEMAPHORE = asyncio.Semaphore(2)

MAX_CLIENTS_CAP = 30
RAM_CEILING_MB = 430.0

LIFETIME_CONNECTIONS = 0
PEAK_CONCURRENT_USERS = 0

CURRENT_TIER = 0
LAST_RAM_ALERT = 0

CORS_HEADERS = {
    "Access-Control-Allow-Origin": "*",
    "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
    "Access-Control-Allow-Headers": "Content-Type, Authorization",
}

def generate_action_token(handle: str, action: str) -> str:
    payload = f"{handle.lower()}:{action}:{ADMIN_KEY}".encode()
    return hmac.new(ADMIN_KEY.encode(), payload, hashlib.sha256).hexdigest()

def verify_action_token(handle: str, action: str, token: str) -> bool:
    expected = generate_action_token(handle, action)
    return hmac.compare_digest(expected, token)

def get_current_rss_mb() -> float:
    try:
        with open("/proc/self/statm") as f:
            return (int(f.read().split()[1]) * 4096) / (1024 * 1024)
    except Exception:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024

async def send_discord_alert(title: str, description: str, color: int, ping_admin: bool = False):
    if not DISCORD_WEBHOOK_URL:
        return
    content = f"🚨 <@{DISCORD_ADMIN_USER_ID}> **New Opt-Out Request!**" if ping_admin else None
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
    if content:
        payload["content"] = content

    try:
        async with aiohttp.ClientSession() as session:
            await session.post(DISCORD_WEBHOOK_URL, json=payload, timeout=aiohttp.ClientTimeout(total=4))
    except Exception:
        pass

async def daily_report_worker(app):
    global PEAK_CONCURRENT_USERS
    try:
        while True:
            await asyncio.sleep(86400)
            mem_mb = get_current_rss_mb()
            active_now = len(ACTIVE_CLIENTS)

            await send_discord_alert(
                "📊 Daily OmniFeed Usage Report",
                f"**Peak Concurrent Users:** `{PEAK_CONCURRENT_USERS}`\n"
                f"**Current Active Clients:** `{active_now}`\n"
                f"**Total Connections Handled:** `{LIFETIME_CONNECTIONS}`\n"
                f"**Current RAM Usage:** `{mem_mb:.1f} MB / 512 MB`\n"
                f"**Cached User IDs:** `{len(RESOLVED_CACHE)}`\n"
                f"**Globally Blocked Creators:** `{len(ACTIVE_BLOCKLIST)}`",
                0x00E5FF
            )
            PEAK_CONCURRENT_USERS = active_now
    except asyncio.CancelledError:
        pass

async def evaluate_system_health():
    global CURRENT_TIER, LAST_RAM_ALERT, PEAK_CONCURRENT_USERS

    active_count = len(ACTIVE_CLIENTS)
    if active_count > PEAK_CONCURRENT_USERS:
        PEAK_CONCURRENT_USERS = active_count

    mem_mb = get_current_rss_mb()
    now = asyncio.get_event_loop().time()

    if mem_mb >= 420 and (now - LAST_RAM_ALERT > 600):
        LAST_RAM_ALERT = now
        asyncio.create_task(send_discord_alert(
            "🚨 Critical Relay Memory Usage",
            f"**RAM:** `{mem_mb:.1f} MB / 512 MB`\n"
            f"**Active Clients:** `{active_count}`\n"
            f"Approaching Render container memory ceiling.",
            0xFF0033
        ))

    if active_count >= 25 and CURRENT_TIER < 2:
        CURRENT_TIER = 2
        asyncio.create_task(send_discord_alert(
            "🔥 Peak Concurrency Warning (25+ Users)",
            f"**Active Clients:** `{active_count}`\n"
            f"**Active TikTok Streams:** `{len(set(WS_HOST_MAP.values()))}`\n"
            f"**RAM Usage:** `{mem_mb:.1f} MB`",
            0xFF5500
        ))
    elif active_count >= 15 and CURRENT_TIER < 1:
        CURRENT_TIER = 1
        asyncio.create_task(send_discord_alert(
            "⚠️ Elevated Concurrency Notice (15+ Users)",
            f"**Active Clients:** `{active_count}`\n"
            f"**Active TikTok Streams:** `{len(set(WS_HOST_MAP.values()))}`\n"
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

async def test_alert_handler(request):
    token = request.query.get("token", "")
    if token != ADMIN_KEY:
        return web.Response(status=401, text="Unauthorized: Invalid Admin Token", headers=CORS_HEADERS)

    mem_mb = get_current_rss_mb()
    await send_discord_alert(
        "🧪 OmniFeed Webhook Test",
        f"Discord webhook connection is verified and operational!\n"
        f"**Active Clients:** `{len(ACTIVE_CLIENTS)}`\n"
        f"**RAM Usage:** `{mem_mb:.1f} MB / 512 MB`\n"
        f"**Peak Today:** `{PEAK_CONCURRENT_USERS}`\n"
        f"**Blocked Creators:** `{len(ACTIVE_BLOCKLIST)}`",
        0x00E5FF
    )
    return web.json_response({"status": "ok", "message": "Test alert dispatched to Discord"}, headers=CORS_HEADERS)

async def stats_options_handler(request):
    return web.Response(status=204, headers=CORS_HEADERS)

async def stats_handler(request):
    token = request.query.get("token", "")
    if token != ADMIN_KEY:
        return web.Response(status=401, text="Unauthorized: Invalid Admin Token", headers=CORS_HEADERS)

    active_streams = list(set(WS_HOST_MAP.values()))
    mem_mb = get_current_rss_mb()
    return web.json_response({
        "status": "ok",
        "concurrent_ws_clients": len(ACTIVE_CLIENTS),
        "active_tiktok_streams": len(active_streams),
        "cached_user_ids": len(RESOLVED_CACHE),
        "blocked_creators": list(ACTIVE_BLOCKLIST),
        "peak_today": PEAK_CONCURRENT_USERS,
        "lifetime_connections": LIFETIME_CONNECTIONS,
        "memory_usage_mb": round(mem_mb, 2),
        "tracked_handles": active_streams
    }, headers=CORS_HEADERS)

async def optout_form_handler(request):
    html = """<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>OmniFeed Creator Opt-Out</title>
  <style>
    body { background: #0f0f14; color: #f0f0f5; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; padding: 16px; }
    .box { background: #161620; border: 1px solid #262636; border-radius: 12px; padding: 28px; max-width: 380px; width: 100%; box-shadow: 0 10px 30px rgba(0,0,0,0.5); }
    h2 { margin: 0 0 8px 0; font-size: 20px; color: #00e5ff; }
    p { font-size: 13px; line-height: 1.45; color: #a0a0b2; margin: 0 0 20px 0; }
    label { font-size: 11px; font-weight: 700; color: #d0d0dd; text-transform: uppercase; letter-spacing: 0.5px; }
    input[type="text"] { width: 100%; box-sizing: border-box; background: #0a0a0f; border: 1px solid #333344; color: #fff; padding: 10px 12px; border-radius: 6px; font-size: 14px; margin: 6px 0 18px 0; outline: none; }
    input[type="text"]:focus { border-color: #00e5ff; }
    button { width: 100%; background: #00e5ff; color: #050508; font-weight: 800; border: none; padding: 12px; border-radius: 6px; font-size: 13px; cursor: pointer; text-transform: uppercase; letter-spacing: 0.5px; }
    button:hover { background: #26ecff; }
  </style>
</head>
<body>
  <div class="box">
    <h2>Creator Opt-Out</h2>
    <p>OmniFeed only aggregates public broadcast telemetry. If you prefer your live stream chat and telemetry not be readable inside OmniFeed aggregator views, enter your handle below to request global exclusion.</p>
    <form method="POST" action="/optout">
      <label>TikTok Username / Handle</label>
      <input type="text" name="handle" placeholder="@yourhandle" required autofocus autocomplete="off" />
      <button type="submit">Submit Opt-Out Request</button>
    </form>
  </div>
</body>
</html>"""
    return web.Response(text=html, content_type="text/html", headers=CORS_HEADERS)

async def optout_submit_handler(request):
    form_data = await request.post()
    raw_handle = form_data.get("handle", "").strip()
    clean_handle = raw_handle.replace("@", "").strip().lower()

    if not clean_handle:
        return web.Response(text="Invalid handle provided.", status=400, content_type="text/plain")

    approve_token = generate_action_token(clean_handle, "approve")
    reject_token = generate_action_token(clean_handle, "reject")
    base_url = "https://omnifeed-relay.onrender.com"

    approve_url = f"{base_url}/optout/action?action=approve&handle={clean_handle}&token={approve_token}"
    reject_url = f"{base_url}/optout/action?action=reject&handle={clean_handle}&token={reject_token}"

    description = (
        f"**Streamer Handle:** `@{clean_handle}`\n\n"
        f"Review creator channel before approving:\n"
        f"[🔗 Open TikTok Profile](https://www.tiktok.com/@{clean_handle})\n\n"
        f"**Admin Actions:**\n"
        f"👉 [✅ Approve & Block Globally]({approve_url})\n"
        f"👉 [❌ Dismiss / Reject Request]({reject_url})"
    )

    asyncio.create_task(send_discord_alert(
        f"Opt-Out Request: @{clean_handle}",
        description,
        0xFF0055,
        ping_admin=True
    ))

    resp_html = f"""<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>Request Received</title>
  <style>
    body {{ background: #0f0f14; color: #f0f0f5; font-family: sans-serif; display: flex; justify-content: center; align-items: center; min-height: 100vh; margin: 0; }}
    .card {{ background: #161620; border: 1px solid #262636; border-radius: 12px; padding: 28px; max-width: 360px; text-align: center; }}
    h3 {{ color: #00e5ff; margin-top: 0; }}
    p {{ font-size: 13px; color: #a0a0b2; line-height: 1.4; }}
  </style>
</head>
<body>
  <div class="card">
    <h3>Request Received</h3>
    <p>Your opt-out request for <strong>@{clean_handle}</strong> has been logged for review. Exclusion takes effect globally upon approval.</p>
  </div>
</body>
</html>"""
    return web.Response(text=resp_html, content_type="text/html", headers=CORS_HEADERS)

async def optout_action_handler(request):
    action = request.query.get("action", "").lower()
    handle = request.query.get("handle", "").replace("@", "").strip().lower()
    token = request.query.get("token", "")

    if not handle or not action or not token:
        return web.Response(text="Missing parameters", status=400, content_type="text/plain")

    if not verify_action_token(handle, action, token):
        return web.Response(text="Unauthorized: Invalid or expired action token", status=403, content_type="text/plain")

    if action == "approve":
        ACTIVE_BLOCKLIST.add(handle)
        save_blocklist()
        msg = f"@{handle} is now globally blocked on OmniFeed."
    else:
        ACTIVE_BLOCKLIST.discard(handle)
        save_blocklist()
        msg = f"Opt-out request for @{handle} was dismissed / removed."

    return web.Response(text=f"<h3>{msg}</h3>", content_type="text/html")

async def resolve_handle_from_user_id(session: aiohttp.ClientSession, user_id: str) -> str | None:
    if user_id in RESOLVED_CACHE:
        return RESOLVED_CACHE[user_id]

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
    }
    try:
        url = f"https://webcast.tiktok.com/webcast/user/profile/?user_id={user_id}&aid=1988"
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
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
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=3)) as resp:
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

    if not room_id:
        return

    async with aiohttp.ClientSession(headers=headers) as session:
        while client.connected and not ws.closed:
            try:
                url = f"https://webcast.tiktok.com/webcast/room/info/?room_id={room_id}&aid=1988"
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=4)) as resp:
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

                        async def resolve_candidate(uid):
                            if uid in RESOLVED_CACHE:
                                return RESOLVED_CACHE[uid]
                            idx = raw_body.find(uid)
                            if idx != -1:
                                start = max(0, idx - 1200)
                                end = min(len(raw_body), idx + 1200)
                                slice_str = raw_body[start:end]
                                matches = re.findall(r'"(?:display_id|displayId|unique_id|uniqueId)":\s*"([^"]+)"', slice_str)
                                for c in matches:
                                    if c and not c.isdigit() and c.lower() != clean_lower:
                                        RESOLVED_CACHE[uid] = c
                                        return c
                            async with RESOLVE_SEMAPHORE:
                                return await resolve_handle_from_user_id(session, uid)

                        resolved_handles = await asyncio.gather(*(resolve_candidate(uid) for uid in rival_user_ids), return_exceptions=True)

                        for candidate_handle in resolved_handles:
                            if isinstance(candidate_handle, str) and candidate_handle and candidate_handle.lower() != clean_lower:
                                cand_clean = candidate_handle.lower()
                                if cand_clean not in prompted_hosts and cand_clean not in ACTIVE_BLOCKLIST:
                                    prompted_hosts.add(cand_clean)
                                    await ws.send_json({
                                        "event": "cohost_detected",
                                        "handle": candidate_handle
                                    })

            except Exception:
                pass

            await asyncio.sleep(5)

async def websocket_handler(request):
    global LIFETIME_CONNECTIONS

    origin = request.headers.get("Origin", "")
    if origin and ("github.io" not in origin and "localhost" not in origin and "127.0.0.1" not in origin):
        return web.Response(status=403, text="Forbidden", headers=CORS_HEADERS)

    current_ram = get_current_rss_mb()
    if len(ACTIVE_CLIENTS) >= MAX_CLIENTS_CAP or current_ram >= RAM_CEILING_MB:
        return web.json_response(
            {"error": "at_capacity", "message": "Server at capacity"},
            status=503,
            headers=CORS_HEADERS
        )

    ws = web.WebSocketResponse(heartbeat=25.0)
    await ws.prepare(request)

    ACTIVE_CLIENTS.add(ws)
    LIFETIME_CONNECTIONS += 1
    asyncio.create_task(evaluate_system_health())

    client = None
    task = None
    probe_task = None

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                data = json.loads(msg.data)
                if data.get("action") == "connect":
                    handle = data.get("handle", "").replace("@", "").strip().lower()
                    if not handle:
                        continue

                    if handle in ACTIVE_BLOCKLIST:
                        await ws.send_json({
                            "event": "error",
                            "code": 403,
                            "message": f"@{handle} has requested not to be aggregated on OmniFeed."
                        })
                        continue

                    WS_HOST_MAP[ws] = handle

                    if probe_task:
                        probe_task.cancel()
                    if task:
                        task.cancel()
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

                    async def run_tiktok(target_handle: str):
                        try:
                            await client.start()
                        except asyncio.CancelledError:
                            pass
                        except Exception:
                            WS_HOST_MAP.pop(ws, None)
                            if probe_task and not probe_task.done():
                                probe_task.cancel()
                            if not ws.closed:
                                try:
                                    await ws.send_json({
                                        "event": "error",
                                        "code": 404,
                                        "message": f"@{target_handle} is offline or could not be found."
                                    })
                                except Exception:
                                    pass

                    task = asyncio.create_task(run_tiktok(handle))
                    probe_task = asyncio.create_task(monitor_cohosts(client, handle, ws))

            elif msg.type == web.WSMsgType.ERROR:
                break
    except Exception:
        pass
    finally:
        ACTIVE_CLIENTS.discard(ws)
        WS_HOST_MAP.pop(ws, None)
        asyncio.create_task(evaluate_system_health())

        if probe_task:
            probe_task.cancel()
        if task:
            task.cancel()
        if client and client.connected:
            await client.disconnect()

    return ws

async def start_background_tasks(app):
    app["daily_report_task"] = asyncio.create_task(daily_report_worker(app))

async def cleanup_background_tasks(app):
    app["daily_report_task"].cancel()
    await app["daily_report_task"]

def create_app():
    app = web.Application()
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    app.router.add_get("/", health_check)
    app.router.add_get("/test-alert", test_alert_handler)
    app.router.add_route("OPTIONS", "/stats", stats_options_handler)
    app.router.add_get("/stats", stats_handler)
    app.router.add_get("/optout", optout_form_handler)
    app.router.add_post("/optout", optout_submit_handler)
    app.router.add_get("/optout/action", optout_action_handler)
    app.router.add_get("/ws", websocket_handler)
    app.router.add_get("", websocket_handler)
    return app

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    web.run_app(create_app(), host="0.0.0.0", port=port)