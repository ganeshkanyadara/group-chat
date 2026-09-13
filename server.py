import asyncio
import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from aiohttp import web, WSMsgType

# Automatically load .env file if present (even without python-dotenv package)
def _load_env():
    for base in [os.path.dirname(os.path.abspath(__file__)), os.getcwd()]:
        env_file = os.path.join(base, ".env")
        if os.path.exists(env_file):
            try:
                with open(env_file, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#") or "=" not in line:
                            continue
                        k, v = line.split("=", 1)
                        k, v = k.strip(), v.strip().strip("'\"")
                        if k and k not in os.environ:
                            os.environ[k] = v
            except Exception:
                pass

_load_env()

from database import (
    init_db,
    store_message,
    load_history_raw,
    load_public_key,
    check_db_health,
    register_online_user,
    remove_online_user,
    clear_backend_users,
    get_all_online_users,
    record_cluster_event,
    get_cluster_events,
    DB_PATH,
    get_db_path,
)
from crypto import (
    encrypt_message,
    decrypt_message,
    sign_message,
    verify_signature,
    construct_canonical_payload,
    KeyManager,
)
from metrics import metrics_tracker

# ============================================================
# CONFIGURATION
# ============================================================

BACKEND_ID = os.getenv("BACKEND_ID", "backend-1")
HOST = os.getenv("BACKEND_HOST") or os.getenv("CHAT_HOST", "0.0.0.0")
PORT = int(os.getenv("BACKEND_PORT") or os.getenv("PORT") or os.getenv("CHAT_PORT", "4000"))
MAX_USERS = int(os.getenv("MAX_USERS", "5000"))
ROOM_ID = os.getenv("ROOM_ID", "main")
HISTORY_LIMIT = int(os.getenv("HISTORY_LIMIT", "50"))

BACKEND_ID_KEY = web.AppKey("backend_id", str)
DB_PATH_KEY = web.AppKey("db_path", str)


def get_app_backend_id(app: web.Application) -> str:
    return app.get(BACKEND_ID_KEY) or app.get("backend_id") or BACKEND_ID


def get_app_db_path(app: web.Application) -> str:
    return app.get(DB_PATH_KEY) or app.get("db_path") or DB_PATH


# State tracking: websocket connection -> user info dict
connected_users = {}

# Set of known message IDs to prevent duplicate broadcasts
seen_message_ids = set()

# Last event ID seen from cluster_events
last_cluster_event_id = 0

# Key Manager instance
key_manager = KeyManager(db_path=DB_PATH)


# ============================================================
# WEBSOCKET HELPERS
# ============================================================

async def send_json(websocket, data: dict):
    """Send JSON message to a single WebSocket client."""
    msg = json.dumps(data)
    if hasattr(websocket, "send_str"):
        await websocket.send_str(msg)
    elif hasattr(websocket, "send"):
        await websocket.send(msg)


async def broadcast(data: dict):
    """Broadcast JSON message to all currently connected WebSocket clients on this instance."""
    if not connected_users:
        return

    msg = json.dumps(data)
    tasks = []
    for ws in list(connected_users.keys()):
        try:
            if hasattr(ws, "send_str"):
                tasks.append(ws.send_str(msg))
            elif hasattr(ws, "send"):
                tasks.append(ws.send(msg))
        except Exception:
            pass
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def send_user_list(app: web.Application | None = None):
    """Broadcast current list of online users combining all systems in the cluster."""
    app_db_path = get_app_db_path(app) if app else DB_PATH
    backend_id = get_app_backend_id(app) if app else BACKEND_ID
    users = None
    try:
        users = get_all_online_users(db_path=app_db_path)
    except Exception:
        pass
    if users is None:
        users = [{"username": user["username"], "backend_id": backend_id} for user in connected_users.values()]
    await broadcast({"type": "user_list", "users": users})


# ============================================================
# CHAT HISTORY LOADER & VERIFIER
# ============================================================

_feed_cache: dict[str, dict] = {}


def get_processed_history(room_id: str = ROOM_ID, limit: int = 100000, db_path: str = DB_PATH) -> list[dict]:
    """
    Retrieve encrypted messages from SQLite, decrypt ciphertext using AES-GCM,
    and verify digital signatures against the canonical payload.
    Uses in-memory cache to avoid re-decrypting and re-verifying known messages.
    Returns list of message dicts formatted for client consumption.
    """
    raw_records = load_history_raw(room_id=room_id, limit=limit, db_path=db_path)
    history = []
    cached_pub_keys = {}

    for record in raw_records:
        msg_id_str = str(record["message_id"])
        if msg_id_str in _feed_cache:
            history.append(_feed_cache[msg_id_str])
            continue

        sender_id = record["sender_id"]
        ciphertext = record["ciphertext"]
        nonce = record["nonce"]
        signature = record["signature"]
        timestamp = record["timestamp"]

        # 1. AES-GCM Decryption
        try:
            plaintext = decrypt_message(ciphertext, nonce)
            decrypted_ok = True
        except Exception:
            plaintext = "[message could not be decrypted - ciphertext tampered or key mismatch]"
            decrypted_ok = False

        # 2. Signature Verification
        verified = False
        if decrypted_ok:
            try:
                canonical_payload = construct_canonical_payload(
                    room_id=room_id,
                    sender_id=sender_id,
                    message=plaintext,
                    timestamp=timestamp,
                )
                if sender_id not in cached_pub_keys:
                    cached_pub_keys[sender_id] = key_manager.get_public_key_bytes(sender_id, db_path=db_path)
                pub_bytes = cached_pub_keys[sender_id]

                if pub_bytes and verify_signature(pub_bytes, canonical_payload, signature):
                    verified = True
            except Exception:
                verified = False

        item = {
            "message_id": msg_id_str,
            "id": msg_id_str,
            "client-name": sender_id,
            "client_name": sender_id,
            "username": sender_id,
            "user": sender_id,
            "msg": plaintext,
            "message": plaintext,
            "text": plaintext,
            "timestamp": timestamp,
            "created_at": timestamp,
            "verified": verified,
        }
        _feed_cache[msg_id_str] = item
        history.append(item)

    return history


# ============================================================
# REQUEST METRICS MIDDLEWARE
# ============================================================

@web.middleware
async def metrics_middleware(request: web.Request, handler):
    """
    Middleware that:
    1. Increments active request count.
    2. Records start time using a monotonic clock.
    3. Processes the request.
    4. Updates latency statistics and decrements active request count in a finally block.
    5. Injects X-Backend-Id header.
    """
    metrics_tracker.increment_active()
    start_time = time.monotonic()
    backend_id = get_app_backend_id(request.app)
    status_code = 500
    try:
        response = await handler(request)
        if isinstance(response, web.StreamResponse):
            status_code = response.status
            response.headers["X-Backend-Id"] = backend_id
        return response
    except web.HTTPException as ex:
        status_code = ex.status
        ex.headers["X-Backend-Id"] = backend_id
        raise
    except Exception:
        status_code = 500
        raise
    finally:
        elapsed_ms = (time.monotonic() - start_time) * 1000.0
        metrics_tracker.record_request(elapsed_ms)
        metrics_tracker.decrement_active()

        # Only log errors or slow requests (> 1000ms) to avoid stdout locking under heavy concurrency
        if status_code >= 400 or elapsed_ms > 1000:
            ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
            print(f"[{ts}] [{backend_id}] {request.method} {request.path} -> {status_code} ({elapsed_ms:.2f}ms)")


# ============================================================
# HTTP CONTROLLER HANDLERS
# ============================================================

async def post_message_handler(request: web.Request) -> web.Response:
    """
    POST /message
    Accepts:
    {
        "client-name": "client1",
        "msg": "Hello world",
        "message_id": "optional-uuid"
    }
    Validates, encrypts with AES-256-GCM, signs with Ed25519,
    and idempotently persists the record to SQLite.
    """
    backend_id = get_app_backend_id(request.app)
    db_path = get_app_db_path(request.app)

    content_type = request.content_type or ""
    data = None
    if "application/json" in content_type:
        try:
            data = await request.json()
        except Exception:
            return web.json_response(
                {"status": "error", "error": "Invalid JSON format in request body"},
                status=400,
            )
    elif "application/x-www-form-urlencoded" in content_type or "multipart/form-data" in content_type:
        try:
            post_data = await request.post()
            data = dict(post_data)
        except Exception:
            return web.json_response(
                {"status": "error", "error": "Invalid form data in request body"},
                status=400,
            )
    else:
        # Auto-detect: try JSON, then try form data
        try:
            data = await request.json()
        except Exception:
            try:
                post_data = await request.post()
                if post_data:
                    data = dict(post_data)
                else:
                    data = dict(request.query)
            except Exception:
                return web.json_response(
                    {"status": "error", "error": "Unsupported request body format"},
                    status=400,
                )

    if not isinstance(data, dict):
        return web.json_response(
            {"status": "error", "error": "Request body must be a JSON object or form data"},
            status=400,
        )

    client_name = data.get("client-name") or data.get("client_name") or data.get("username")
    msg_text = data.get("msg") or data.get("message")
    client_supplied_id = data.get("message_id") or data.get("id") or data.get("msg_id")

    # Validate inputs
    if not client_name or not str(client_name).strip():
        return web.json_response(
            {"status": "error", "error": "Missing required field: 'client-name'"},
            status=400,
        )

    if msg_text is None or not str(msg_text).strip():
        return web.json_response(
            {"status": "error", "error": "Missing required field: 'msg'"},
            status=400,
        )

    client_name = str(client_name).strip()
    msg_text = str(msg_text).strip()[:1000]

    # Assign globally unique ID (client supplied or UUID4)
    if client_supplied_id and str(client_supplied_id).strip():
        message_id = str(client_supplied_id).strip()
    else:
        message_id = str(uuid.uuid4())

    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    try:
        # 1. Asymmetric keypair & Digital Signature
        user_priv_key, _ = key_manager.get_or_create_user_keypair(client_name, db_path=db_path)
        canonical_payload = construct_canonical_payload(
            room_id=ROOM_ID,
            sender_id=client_name,
            message=msg_text,
            timestamp=timestamp,
        )
        signature_bytes = sign_message(user_priv_key, canonical_payload)

        # 2. Symmetric AES-256-GCM Encryption
        ciphertext_bytes, nonce_bytes = encrypt_message(msg_text)

        # 3. Idempotent SQLite Persistence (PRIMARY KEY / ON CONFLICT DO NOTHING)
        persisted_id, was_created = await asyncio.to_thread(
            store_message,
            room_id=ROOM_ID,
            sender_id=client_name,
            ciphertext=ciphertext_bytes,
            nonce=nonce_bytes,
            signature=signature_bytes,
            timestamp=timestamp,
            message_id=message_id,
            return_created=True,
            db_path=db_path,
        )

        # 4. Broadcast to local WebSocket clients if newly created
        if was_created:
            seen_message_ids.add(str(persisted_id))
            _feed_cache[str(persisted_id)] = {
                "message_id": str(persisted_id),
                "id": str(persisted_id),
                "client-name": client_name,
                "client_name": client_name,
                "username": client_name,
                "user": client_name,
                "msg": msg_text,
                "message": msg_text,
                "text": msg_text,
                "timestamp": timestamp,
                "created_at": timestamp,
                "verified": True,
            }
            if connected_users:
                asyncio.create_task(broadcast({
                    "type": "message",
                    "message_id": persisted_id,
                    "username": client_name,
                    "message": msg_text,
                    "timestamp": timestamp,
                    "verified": True,
                }))

        return web.json_response(
            {
                "status": "success",
                "message_id": persisted_id,
                "id": persisted_id,
                "created": was_created,
                "duplicate": not was_created,
                "client-name": client_name,
                "backend_id": backend_id,
            },
            status=201 if was_created else 200,
        )

    except Exception as e:
        print(f"[{backend_id}] [ERROR] Failed to process POST /message: {e}")
        return web.json_response(
            {"status": "error", "error": "Internal database or processing error"},
            status=500,
        )


async def get_feed_handler(request: web.Request) -> web.Response:
    """
    GET /feed
    Retrieves decrypted, verified messages from the shared persistent database.
    Supports ?limit= query parameter, defaulting to 100000 (all messages).
    """
    backend_id = get_app_backend_id(request.app)
    db_path = get_app_db_path(request.app)

    limit_param = request.query.get("limit") or request.query.get("count") or request.query.get("n")
    limit = int(limit_param) if limit_param and limit_param.isdigit() else 100000

    try:
        history_records = await asyncio.to_thread(get_processed_history, room_id=ROOM_ID, limit=limit, db_path=db_path)
        return web.json_response({
            "status": "success",
            "messages": history_records,
            "count": len(history_records),
            "backend_id": backend_id,
        }, status=200)
    except Exception as e:
        print(f"[{backend_id}] [ERROR] Failed to fetch feed: {e}")
        return web.json_response(
            {"status": "error", "error": "Failed to retrieve feed from shared database"},
            status=500,
        )


async def get_health_handler(request: web.Request) -> web.Response:
    """
    GET /health
    Verifies that this backend application is running and the database connection is active.
    """
    backend_id = get_app_backend_id(request.app)
    db_path = get_app_db_path(request.app)

    db_ok = await asyncio.to_thread(check_db_health, db_path, 2.0)
    if db_ok:
        return web.json_response({
            "status": "healthy",
            "database": "connected",
            "backend_id": backend_id,
        }, status=200)
    else:
        return web.json_response({
            "status": "unhealthy",
            "database": "disconnected",
            "backend_id": backend_id,
        }, status=503)


async def get_metrics_handler(request: web.Request) -> web.Response:
    """
    GET /metrics
    Internal performance monitoring endpoint for the Load Balancer.
    Provides CPU %, memory %, active request count, average latency, and uptime.
    """
    backend_id = get_app_backend_id(request.app)
    db_path = get_app_db_path(request.app)

    db_ok = check_db_health(db_path)
    metrics_data = metrics_tracker.get_metrics(backend_id=backend_id, is_healthy=db_ok)
    return web.json_response(metrics_data, status=200)




# ============================================================
# WEBSOCKET & INDEX HANDLERS
# ============================================================

async def websocket_handler(request: web.Request) -> web.WebSocketResponse:
    """Real-time WebSocket handler preserving existing chat capabilities."""
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    ip = request.remote or "unknown"
    if "X-Forwarded-For" in request.headers:
        ip = request.headers["X-Forwarded-For"].split(",")[0].strip()

    username = None

    try:
        # 1. Join handshake
        msg = await ws.receive()
        if msg.type != WSMsgType.TEXT:
            await send_json(ws, {"type": "error", "message": "Join request required."})
            await ws.close()
            return ws

        try:
            auth_data = json.loads(msg.data)
        except (json.JSONDecodeError, TypeError):
            await send_json(ws, {"type": "error", "message": "Invalid JSON format."})
            await ws.close()
            return ws

        if auth_data.get("type") not in ("authenticate", "join"):
            await send_json(ws, {"type": "error", "message": "Join request required."})
            await ws.close()
            return ws

        requested_name = str(auth_data.get("username", "")).strip()
        if not requested_name:
            await send_json(ws, {"type": "error", "message": "Username cannot be empty."})
            await ws.close()
            return ws

        if len(requested_name) > 30:
            await send_json(ws, {"type": "error", "message": "Username cannot exceed 30 characters."})
            await ws.close()
            return ws

        app = request.app
        app_backend_id = get_app_backend_id(app)
        app_db_path = get_app_db_path(app)

        # Disallow duplicate usernames locally and across the cluster
        is_duplicate = any(user["username"].lower() == requested_name.lower() for user in connected_users.values())
        if not is_duplicate:
            try:
                cluster_users = get_all_online_users(db_path=app_db_path, prune_seconds=30)
                if cluster_users:
                    is_duplicate = any(
                        str(u.get("username", "")).strip().lower() == requested_name.lower()
                        for u in cluster_users
                    )
            except Exception:
                pass

        if is_duplicate:
            print(f"[{app_backend_id}] [REJECT] Duplicate username attempt: '{requested_name}' from {ip}")
            await send_json(ws, {
                "type": "error",
                "error_code": "DUPLICATE_USERNAME",
                "message": f"Username '{requested_name}' is already taken. Please choose a different username."
            })
            await ws.close()
            return ws

        username = requested_name

        if len(connected_users) >= MAX_USERS:
            await send_json(ws, {"type": "error", "message": f"Chat room full. Maximum {MAX_USERS} users allowed."})
            await ws.close()
            return ws

        # 1. Register presence in shared cluster database
        try:
            register_online_user(username, app_backend_id, db_path=app_db_path)
        except Exception as e:
            print(f"[{app_backend_id}] [PRESENCE WARNING] Failed to register {username}: {e}")

        # 2. Keypair registration
        private_key, _pub_bytes = key_manager.get_or_create_user_keypair(username)
        connected_users[ws] = {
            "username": username,
            "ip": ip,
            "private_key": private_key,
        }

        print(f"[{app_backend_id}] [JOIN] {username} connected from {ip} | Online: {len(connected_users)}/{MAX_USERS}")
        await send_json(ws, {
            "type": "authenticated",
            "username": username,
            "backend_id": app_backend_id
        })

        # 3. History delivery
        history_messages = get_processed_history(ROOM_ID, HISTORY_LIMIT, db_path=app_db_path)
        if history_messages:
            await send_json(ws, {
                "type": "history",
                "messages": history_messages,
            })

        try:
            record_cluster_event("system", f"{username} joined the chat", sender_node=app_backend_id, db_path=app_db_path)
        except Exception:
            pass

        await broadcast({"type": "system", "message": f"{username} joined the chat"})
        await send_user_list(app)

        # 4. Message loop
        async for msg in ws:
            if msg.type == WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                except (json.JSONDecodeError, TypeError):
                    continue

                if data.get("type") != "message":
                    continue

                msg_text = str(data.get("message", "")).strip()
                if not msg_text:
                    continue

                msg_text = msg_text[:1000]
                timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

                canonical_payload = construct_canonical_payload(
                    room_id=ROOM_ID,
                    sender_id=username,
                    message=msg_text,
                    timestamp=timestamp,
                )

                user_private_key = connected_users[ws]["private_key"]
                signature_bytes = sign_message(user_private_key, canonical_payload)
                ciphertext_bytes, nonce_bytes = encrypt_message(msg_text)

                msg_id = store_message(
                    room_id=ROOM_ID,
                    sender_id=username,
                    ciphertext=ciphertext_bytes,
                    nonce=nonce_bytes,
                    signature=signature_bytes,
                    timestamp=timestamp,
                    db_path=app_db_path,
                )

                seen_message_ids.add(str(msg_id))

                await broadcast({
                    "type": "message",
                    "message_id": msg_id,
                    "username": username,
                    "message": msg_text,
                    "timestamp": timestamp,
                    "verified": True,
                })
            elif msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED):
                break

    except Exception as e:
        print(f"[{BACKEND_ID}] [ERROR] Session exception for {username or ip}: {e}")
    finally:
        if ws in connected_users:
            user_info = connected_users.pop(ws)
            left_user = user_info["username"]
            print(f"[{app_backend_id}] [LEAVE] {left_user} disconnected | Online: {len(connected_users)}/{MAX_USERS}")
            try:
                remove_online_user(left_user, app_backend_id, db_path=app_db_path)
            except Exception:
                pass
            try:
                record_cluster_event("system", f"{left_user} left the chat", sender_node=app_backend_id, db_path=app_db_path)
            except Exception:
                pass
            await broadcast({"type": "system", "message": f"{left_user} left the chat"})
            await send_user_list(app)

    return ws


async def get_users_handler(request: web.Request) -> web.Response:
    """Returns cluster-wide online users combining all 3 backend systems."""
    app_db_path = get_app_db_path(request.app)
    backend_id = get_app_backend_id(request.app)
    users = None
    try:
        users = get_all_online_users(db_path=app_db_path)
    except Exception:
        pass
    if users is None:
        users = [{"username": u["username"], "backend_id": backend_id} for u in connected_users.values()]
    return web.json_response({
        "status": "success",
        "backend_id": backend_id,
        "total_online": len(users),
        "users": users,
    })


async def index_or_ws_handler(request: web.Request) -> web.StreamResponse:
    """Handles both WebSocket upgrades and general HTTP metadata on /."""
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await websocket_handler(request)

    # If browser requests HTML UI
    accept = request.headers.get("Accept", "")
    if "text/html" in accept:
        client_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "client")
        index_file = os.path.join(client_dir, "index.html")
        if os.path.exists(index_file):
            return web.FileResponse(index_file)

    backend_id = request.app.get(BACKEND_ID_KEY, BACKEND_ID)
    return web.json_response({
        "service": "secure-group-chat-backend",
        "backend_id": backend_id,
        "status": "running",
        "endpoints": {
            "post_message": "POST /message",
            "get_feed": "GET /feed",
            "get_health": "GET /health",
            "get_metrics": "GET /metrics",
            "get_users": "GET /users",
            "websocket": f"ws://{HOST}:{PORT}/",
        },
    })


async def cluster_sync_loop(app: web.Application):
    """
    Real-time cross-node sync loop:
    1. Synchronizes messages across all nodes via the shared DB in real time.
    2. Synchronizes join/leave events across all nodes via cluster events.
    3. Heartbeats local users and updates the combined online user list.
    """
    global last_cluster_event_id
    app_backend_id = get_app_backend_id(app)
    app_db_path = get_app_db_path(app)
    last_users_json = ""
    heartbeat_counter = 0

    # Pre-populate seen_message_ids from DB on startup so we don't re-broadcast history
    try:
        init_history = load_history_raw(ROOM_ID, limit=100, db_path=app_db_path)
        for r in init_history:
            seen_message_ids.add(str(r["message_id"]))
    except Exception:
        pass

    try:
        while True:
            await asyncio.sleep(0.4)
            heartbeat_counter += 1

            # A. REAL-TIME CROSS-NODE MESSAGE SYNC
            # If this instance has connected WebSocket clients, poll for new messages from other nodes
            if connected_users:
                try:
                    recent_raw = load_history_raw(ROOM_ID, limit=15, db_path=app_db_path)
                    new_records = [r for r in recent_raw if str(r["message_id"]) not in seen_message_ids]
                    cached_sync_keys = {}
                    for r in new_records:
                        msg_id = str(r["message_id"])
                        seen_message_ids.add(msg_id)
                        if len(seen_message_ids) > 2000:
                            seen_message_ids.clear()
                            for rec in recent_raw:
                                seen_message_ids.add(str(rec["message_id"]))

                        sender_id = r["sender_id"]
                        timestamp = r["timestamp"]
                        try:
                            plaintext = decrypt_message(r["ciphertext"], r["nonce"])
                            decrypted_ok = True
                        except Exception:
                            plaintext = "[message could not be decrypted - ciphertext tampered or key mismatch]"
                            decrypted_ok = False

                        verified = False
                        if decrypted_ok:
                            try:
                                canonical_payload = construct_canonical_payload(
                                    room_id=ROOM_ID,
                                    sender_id=sender_id,
                                    message=plaintext,
                                    timestamp=timestamp,
                                )
                                if sender_id not in cached_sync_keys:
                                    cached_sync_keys[sender_id] = key_manager.get_public_key_bytes(sender_id, db_path=app_db_path)
                                pub_bytes = cached_sync_keys[sender_id]
                                if pub_bytes and verify_signature(pub_bytes, canonical_payload, r["signature"]):
                                    verified = True
                            except Exception:
                                verified = False

                        await broadcast({
                            "type": "message",
                            "message_id": msg_id,
                            "username": sender_id,
                            "message": plaintext,
                            "timestamp": timestamp,
                            "verified": verified,
                        })
                except Exception:
                    pass

            # B. REAL-TIME CROSS-NODE SYSTEM EVENTS (join / leave)
            try:
                events = get_cluster_events(since_id=last_cluster_event_id, db_path=app_db_path)
                for ev in events:
                    if ev["id"] > last_cluster_event_id:
                        last_cluster_event_id = ev["id"]
                    # Only broadcast events originating from other nodes
                    if ev.get("sender_node") != app_backend_id:
                        await broadcast({"type": "system", "message": ev["message"]})
            except Exception:
                pass

            # C. PERIODIC PRESENCE & HEARTBEAT (every ~2 seconds)
            if heartbeat_counter >= 5:
                heartbeat_counter = 0
                local_users = [u["username"] for u in list(connected_users.values())]
                for u in local_users:
                    try:
                        register_online_user(u, app_backend_id, db_path=app_db_path)
                    except Exception:
                        pass
                try:
                    cluster_users = get_all_online_users(db_path=app_db_path)
                    if cluster_users is not None:
                        current_json = json.dumps([{"u": x.get("username"), "b": x.get("backend_id")} for x in cluster_users], sort_keys=True)
                        if current_json != last_users_json:
                            last_users_json = current_json
                            await broadcast({"type": "user_list", "users": cluster_users})
                except Exception:
                    pass
    except asyncio.CancelledError:
        pass


async def start_background_tasks(app: web.Application):
    """Startup and cleanup context for cluster sync loop, presence, and heartbeat."""
    app_backend_id = get_app_backend_id(app)
    app_db_path = get_app_db_path(app)
    try:
        clear_backend_users(app_backend_id, db_path=app_db_path)
    except Exception:
        pass

    task = asyncio.create_task(cluster_sync_loop(app))
    yield
    task.cancel()
    try:
        await task
    except (asyncio.CancelledError, Exception):
        pass
    try:
        clear_backend_users(app_backend_id, db_path=app_db_path)
    except Exception:
        pass


# ============================================================
# APPLICATION FACTORY & STARTUP
# ============================================================

def create_app(backend_id: str | None = None, db_path: str | None = None) -> web.Application:
    """Create and configure the aiohttp application with routes, middleware, and instance configuration."""
    app_backend_id = backend_id or os.getenv("BACKEND_ID", BACKEND_ID)
    app_db_path = db_path or os.getenv("DATABASE_URL") or os.getenv("CHAT_DB_PATH", DB_PATH)
    app = web.Application(middlewares=[metrics_middleware])
    app[BACKEND_ID_KEY] = app_backend_id
    app[DB_PATH_KEY] = app_db_path

    app.router.add_post("/message", post_message_handler)
    app.router.add_get("/feed", get_feed_handler)
    app.router.add_get("/health", get_health_handler)
    app.router.add_get("/metrics", get_metrics_handler)
    app.router.add_get("/users", get_users_handler)
    app.router.add_get("/online", get_users_handler)
    app.router.add_get("/", index_or_ws_handler)
    app.router.add_get("/ws", websocket_handler)

    client_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "client")
    if os.path.exists(client_dir):
        style_f = os.path.join(client_dir, "style.css")
        app_f = os.path.join(client_dir, "app.js")
        if os.path.exists(style_f):
            async def serve_style(request):
                return web.FileResponse(style_f)
            app.router.add_get("/style.css", serve_style)
        if os.path.exists(app_f):
            async def serve_app(request):
                return web.FileResponse(app_f)
            app.router.add_get("/app.js", serve_app)
        app.router.add_static("/client", client_dir)

    app.cleanup_ctx.append(start_background_tasks)
    return app



def main():
    # Initialize Database tables and migrations (non-fatal if remote DB is starting up)
    try:
        init_db(DB_PATH)
    except Exception as e:
        print(f"[WARN] Database initialization warning on startup: {e}")

    print("=" * 60)
    print(f"   SECURE GROUP CHAT BACKEND [{BACKEND_ID}]")
    print("=" * 60)
    print(f"HTTP & WebSocket listening on http://{HOST}:{PORT}")
    print(f"Database: {DB_PATH}")
    print(f"Encryption: AES-256-GCM")
    print(f"Digital Signatures: Ed25519")
    print("Endpoints:")
    print("  POST /message - Create/store chat message")
    print("  GET  /feed    - Fetch persistent chat history")
    print("  GET  /health  - Health & database check")
    print("  GET  /metrics - Load balancer performance metrics")
    print("=" * 60)

    app = create_app()
    web.run_app(app, host=HOST, port=PORT, access_log=None)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n[STOP] Backend [{BACKEND_ID}] stopped by user.")
        sys.exit(0)
