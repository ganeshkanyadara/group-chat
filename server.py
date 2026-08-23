import asyncio
import json
import os
import sys
from datetime import datetime, timezone
import websockets

from database import (
    init_db,
    store_message,
    load_history_raw,
    load_public_key,
    DB_PATH,
)
from crypto import (
    encrypt_message,
    decrypt_message,
    sign_message,
    verify_signature,
    construct_canonical_payload,
    KeyManager,
)

# ============================================================
# CONFIGURATION
# ============================================================

HOST = os.getenv("CHAT_HOST", "0.0.0.0")
PORT = int(os.getenv("CHAT_PORT", "4000"))
MAX_USERS = 5000
ROOM_ID = "main"
HISTORY_LIMIT = 50

# State tracking: websocket connection -> user info dict
connected_users = {}

# Key Manager instance
key_manager = KeyManager(db_path=DB_PATH)


# ============================================================
# WEBSOCKET HELPERS
# ============================================================

async def send_json(websocket, data: dict):
    """Send JSON message to a single WebSocket client."""
    await websocket.send(json.dumps(data))


async def broadcast(data: dict):
    """Broadcast JSON message to all currently connected WebSocket clients."""
    if not connected_users:
        return

    message = json.dumps(data)
    await asyncio.gather(
        *(ws.send(message) for ws in connected_users),
        return_exceptions=True,
    )


async def send_user_list():
    """Broadcast current list of online users."""
    users = [{"username": user["username"]} for user in connected_users.values()]
    await broadcast({"type": "user_list", "users": users})


# ============================================================
# CHAT HISTORY LOADER & VERIFIER
# ============================================================

def get_processed_history(room_id: str = ROOM_ID, limit: int = HISTORY_LIMIT) -> list[dict]:
    """
    Retrieve encrypted messages from SQLite, decrypt ciphertext using AES-GCM,
    and verify digital signatures against the canonical payload.
    
    Returns list of message dicts formatted for the client UI.
    """
    raw_records = load_history_raw(room_id=room_id, limit=limit, db_path=DB_PATH)
    history = []

    for record in raw_records:
        msg_id = record["message_id"]
        sender_id = record["sender_id"]
        ciphertext = record["ciphertext"]
        nonce = record["nonce"]
        signature = record["signature"]
        timestamp = record["timestamp"]

        # 1. AES-GCM Decryption
        try:
            plaintext = decrypt_message(ciphertext, nonce)
            decrypted_ok = True
        except Exception as e:
            print(f"[SECURITY WARNING] Failed to decrypt message_id {msg_id} from {sender_id}: {e}")
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
                pub_bytes = key_manager.get_public_key_bytes(sender_id)

                if pub_bytes and verify_signature(pub_bytes, canonical_payload, signature):
                    verified = True
                else:
                    print(f"[SECURITY ALERT] Digital signature verification FAILED for message_id {msg_id} from {sender_id}!")
            except Exception as e:
                print(f"[SECURITY ALERT] Signature check error for message_id {msg_id}: {e}")

        history.append({
            "message_id": msg_id,
            "username": sender_id,
            "message": plaintext,
            "timestamp": timestamp,
            "verified": verified,
        })

    return history


# ============================================================
# CLIENT WEBSOCKET HANDLER
# ============================================================

async def chat(websocket):
    ip = websocket.remote_address[0] if websocket.remote_address else "unknown"
    username = None

    try:
        # ----------------------------------------------------
        # 1. Open Access Join Handshake
        # ----------------------------------------------------
        raw_auth = await websocket.recv()
        try:
            auth_data = json.loads(raw_auth)
        except (json.JSONDecodeError, TypeError):
            await send_json(websocket, {"type": "error", "message": "Invalid JSON format."})
            await websocket.close()
            return

        if auth_data.get("type") not in ("authenticate", "join"):
            await send_json(websocket, {"type": "error", "message": "Join request required."})
            await websocket.close()
            return

        requested_name = str(auth_data.get("username", "")).strip()
        if not requested_name:
            import random
            requested_name = f"Guest_{random.randint(1000, 9999)}"

        # Ensure unique display name per active connection
        username = requested_name
        counter = 1
        while any(user["username"] == username for user in connected_users.values()):
            username = f"{requested_name}_{counter}"
            counter += 1

        # Check maximum capacity
        if len(connected_users) >= MAX_USERS:
            print(f"[REJECTED] Room full limit ({MAX_USERS}) reached | IP: {ip}")
            await send_json(websocket, {"type": "error", "message": f"Chat room full. Maximum {MAX_USERS} users allowed."})
            await websocket.close()
            return

        # ----------------------------------------------------
        # 2. Register Session & Keypair Setup
        # ----------------------------------------------------
        private_key, _pub_bytes = key_manager.get_or_create_user_keypair(username)
        connected_users[websocket] = {
            "username": username,
            "ip": ip,
            "private_key": private_key,
        }

        print(f"[JOIN] {username} connected from {ip} | Online: {len(connected_users)}/{MAX_USERS}")

        # Send authentication success frame
        await send_json(websocket, {"type": "authenticated", "username": username})

        # ----------------------------------------------------
        # 3. Send Persistent Chat History
        # ----------------------------------------------------
        history_messages = get_processed_history(ROOM_ID, HISTORY_LIMIT)
        if history_messages:
            await send_json(websocket, {
                "type": "history",
                "messages": history_messages,
            })
            print(f"[HISTORY] Restored {len(history_messages)} messages for {username}")

        # Broadcast join notification & user list
        await broadcast({"type": "system", "message": f"{username} joined the chat"})
        await send_user_list()

        # ----------------------------------------------------
        # 4. Real-time Message Loop
        # ----------------------------------------------------
        async for raw_msg in websocket:
            try:
                data = json.loads(raw_msg)
            except (json.JSONDecodeError, TypeError):
                continue

            if data.get("type") != "message":
                continue

            msg_text = str(data.get("message", "")).strip()
            if not msg_text:
                continue

            # Validate message size (max 1000 characters)
            msg_text = msg_text[:1000]

            # Generate UTC ISO 8601 timestamp
            timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            # Construct canonical payload for signature
            canonical_payload = construct_canonical_payload(
                room_id=ROOM_ID,
                sender_id=username,
                message=msg_text,
                timestamp=timestamp,
            )

            # Digital signature using sender's Ed25519 private key
            user_private_key = connected_users[websocket]["private_key"]
            signature_bytes = sign_message(user_private_key, canonical_payload)

            # AES-256-GCM Encryption
            ciphertext_bytes, nonce_bytes = encrypt_message(msg_text)

            # Store encrypted representation in SQLite
            store_message(
                room_id=ROOM_ID,
                sender_id=username,
                ciphertext=ciphertext_bytes,
                nonce=nonce_bytes,
                signature=signature_bytes,
                timestamp=timestamp,
                db_path=DB_PATH,
            )

            print(f"[MESSAGE] {username}: {msg_text} (Encrypted & Signed)")

            # Broadcast message with verified status
            await broadcast({
                "type": "message",
                "username": username,
                "message": msg_text,
                "timestamp": timestamp,
                "verified": True,
            })

    except websockets.exceptions.ConnectionClosed:
        pass
    except Exception as e:
        print(f"[ERROR] Session exception for {username or ip}: {e}")
    finally:
        # Cleanup disconnected user
        if websocket in connected_users:
            user_info = connected_users.pop(websocket)
            left_user = user_info["username"]
            print(f"[LEAVE] {left_user} disconnected | Online: {len(connected_users)}/{MAX_USERS}")
            await broadcast({"type": "system", "message": f"{left_user} left the chat"})
            await send_user_list()


# ============================================================
# MAIN ENTRYPOINT
# ============================================================

async def main():
    # Initialize Database tables
    init_db(DB_PATH)

    print("=" * 60)
    print("         REAL-TIME GROUP CHAT  (persistent + open access)")
    print("=" * 60)
    print(f"WebSocket server listening on ws://{HOST}:{PORT}")
    print(f"Maximum capacity: {MAX_USERS} users")
    print(f"Database: {DB_PATH}")
    print(f"Encryption: AES-256-GCM")
    print(f"Digital Signatures: Ed25519")
    print("Access Mode: Public (No authentication required)")
    print("\nServer is running. Waiting for connections...\n")

    async with websockets.serve(chat, HOST, PORT):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[STOP] Server stopped by user.")
        sys.exit(0)
