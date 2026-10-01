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

async def resolve_handle_from_user_id(session: aiohttp.ClientSession, user_id: str) -> str | None:
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
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=4)) as resp:
            if resp.status == 200:
                body = await resp.text()
                m = re.search(r'"uniqueId":"([^"]+)"', body)
                if m:
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

    print(f"[COHOST-MONITOR] Resolved Room ID for @{clean_lower}: {room_id}", flush=True)
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

                        # Detailed diagnostic snapshot of link_mic values
                        print(f"--- [DIAGNOSTIC SNAPSHOT] ---", flush=True)
                        print(f"owner_id: {owner_id}", flush=True)
                        print(f"rival_anchor_id: {link_mic.get('rival_anchor_id')}", flush=True)
                        print(f"linked_user_list: {json.dumps(link_mic.get('linked_user_list'))}", flush=True)
                        print(f"show_user_list: {json.dumps(link_mic.get('show_user_list'))}", flush=True)
                        print(f"channel_info: {json.dumps(link_mic.get('channel_info'))}", flush=True)
                        print(f"battle_scores: {json.dumps(link_mic.get('battle_scores'))}", flush=True)

                        rival_user_ids = set()

                        # 1. Direct rival anchor ID
                        rival_anchor_id = str(link_mic.get("rival_anchor_id") or link_mic.get("rivalAnchorId") or "")
                        if rival_anchor_id and rival_anchor_id not in ("0", owner_id):
                            rival_user_ids.add(rival_anchor_id)

                        # 2. Linked user list recursive check
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
                        extract_uids(link_mic.get("channel_info"))
                        extract_uids(link_mic.get("battle_scores"))

                        print(f"[COHOST-POLL] Extracted Rival IDs: {rival_user_ids}", flush=True)

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

                        print(f"[COHOST-POLL] Resolved candidates: {candidates}", flush=True)

                        for cohost in candidates:
                            if cohost.lower() not in prompted_hosts:
                                prompted_hosts.add(cohost.lower())
                                print(f"[COHOST-DETECTED] Emitting @{cohost}", flush=True)
                                await ws.send_json({
                                    "event": "cohost_detected",
                                    "handle": cohost
                                })

            except Exception as e:
                print(f"[COHOST-PROBE ERROR] {e}", flush=True)

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
