import sqlite3
import os
import uuid
import json
import base64
import time
import urllib.request
import urllib.parse
import urllib.error
import http.client
import threading


def get_db_path() -> str:
    url = os.getenv("DATABASE_URL")
    if url:
        if url.startswith("sqlite:///"):
            return url[len("sqlite:///"):]
        elif url.startswith("sqlite://"):
            return url[len("sqlite://"):]
        return url.strip()
    return os.getenv("CHAT_DB_PATH", "chat.db")


DB_PATH = get_db_path()


def is_remote_db(db_path: str | None = None) -> bool:
    """Check if database destination is a remote HTTP/HTTPS database microservice."""
    target = db_path or get_db_path()
    return target.startswith("http://") or target.startswith("https://")


_thread_local_http = threading.local()


def _get_http_connection(netloc: str, timeout: float = 10.0) -> http.client.HTTPConnection:
    conn_map = getattr(_thread_local_http, "conns", None)
    if conn_map is None:
        conn_map = {}
        _thread_local_http.conns = conn_map
    conn = conn_map.get(netloc)
    if conn is None:
        conn = http.client.HTTPConnection(netloc, timeout=timeout)
        conn_map[netloc] = conn
    return conn


def _http_request(url: str, method: str = "GET", data: dict | None = None, timeout: float = 10.0) -> dict | None:
    """Perform persistent HTTP/1.1 JSON request to remote DB microservice reusing TCP connections."""
    parsed = urllib.parse.urlparse(url)
    netloc = parsed.netloc
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    body_bytes = json.dumps(data).encode("utf-8") if data is not None else None

    for attempt in range(2):
        conn = _get_http_connection(netloc, timeout=timeout)
        try:
            conn.request(method, path, body=body_bytes, headers=headers)
            resp = conn.getresponse()
            if resp.status == 404:
                resp.read()
                return None
            content = resp.read().decode("utf-8")
            if not content:
                return {}
            return json.loads(content)
        except (http.client.RemoteDisconnected, BrokenPipeError, ConnectionResetError, http.client.CannotSendRequest, http.client.BadStatusLine):
            # Stale keep-alive connection, close and retry once with a fresh socket
            try:
                conn.close()
            except Exception:
                pass
            if hasattr(_thread_local_http, "conns"):
                _thread_local_http.conns.pop(netloc, None)
            if attempt == 1:
                # Fallback to standard urlopen on final attempt
                req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
                with urllib.request.urlopen(req, timeout=timeout) as fb_resp:
                    fb_content = fb_resp.read().decode("utf-8")
                    return json.loads(fb_content) if fb_content else {}
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            if hasattr(_thread_local_http, "conns"):
                _thread_local_http.conns.pop(netloc, None)
            raise


def get_db_connection(db_path: str | None = None, timeout: float = 30.0) -> sqlite3.Connection:
    """Return an SQLite connection configured with WAL and busy timeouts for concurrent access."""
    target_path = db_path or get_db_path()
    con = sqlite3.connect(target_path, timeout=timeout)
    try:
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA busy_timeout=30000;")
        con.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return con


def check_db_health(db_path: str | None = None, timeout: float = 2.0) -> bool:
    """Lightweight check verifying database connectivity (local file or remote HTTP server)."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/health", timeout=timeout)
            return res is not None and res.get("status") == "healthy"
        except Exception:
            return False

    try:
        con = sqlite3.connect(target_path, timeout=timeout)
        cur = con.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        con.close()
        return True
    except Exception:
        return False


def init_db(db_path: str = DB_PATH) -> None:
    """Initialize database tables for messages and public keys with WAL and safe migrations."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        _http_request(f"{target_path.rstrip('/')}/init", method="POST")
        return

    con = get_db_connection(target_path)
    cur = con.cursor()

    # Check if messages table already exists and if message_id is INTEGER
    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages';")
    table_exists = cur.fetchone() is not None

    if table_exists:
        cur.execute("PRAGMA table_info(messages);")
        columns = cur.fetchall()
        msg_id_col = next((c for c in columns if c[1] == "message_id"), None)
        if msg_id_col and "INT" in str(msg_id_col[2]).upper():
            # Migrate to TEXT PRIMARY KEY so UUIDs and string IDs are supported
            cur.execute("ALTER TABLE messages RENAME TO messages_old;")
            cur.execute("""
                CREATE TABLE messages (
                    message_id  TEXT PRIMARY KEY,
                    room_id     TEXT NOT NULL,
                    sender_id   TEXT NOT NULL,
                    ciphertext  BLOB NOT NULL,
                    nonce       BLOB NOT NULL,
                    signature   BLOB NOT NULL,
                    timestamp   TEXT NOT NULL
                );
            """)
            cur.execute("""
                INSERT INTO messages (message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp)
                SELECT CAST(message_id AS TEXT), room_id, sender_id, ciphertext, nonce, signature, timestamp
                FROM messages_old;
            """)
            cur.execute("DROP TABLE messages_old;")
    else:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS messages (
                message_id  TEXT PRIMARY KEY,
                room_id     TEXT NOT NULL,
                sender_id   TEXT NOT NULL,
                ciphertext  BLOB NOT NULL,
                nonce       BLOB NOT NULL,
                signature   BLOB NOT NULL,
                timestamp   TEXT NOT NULL
            );
        """)

    # Table storing raw public keys for sender signature verification
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_keys (
            username    TEXT PRIMARY KEY,
            public_key  BLOB NOT NULL
        );
    """)

    # Table storing multi-node online presence
    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_users (
            username    TEXT PRIMARY KEY,
            backend_id  TEXT NOT NULL,
            last_seen   INTEGER NOT NULL
        );
    """)

    # Table storing multi-node cluster events (join/leave/system broadcasts)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type  TEXT NOT NULL,
            message     TEXT NOT NULL,
            sender_node TEXT NOT NULL,
            timestamp   INTEGER NOT NULL
        );
    """)

    con.commit()
    con.close()


def save_public_key(username: str, public_key_bytes: bytes, db_path: str = DB_PATH) -> None:
    """Store or update user's raw Ed25519 public key bytes in SQLite (local or remote HTTP)."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        pub_b64 = base64.b64encode(public_key_bytes).decode("utf-8")
        _http_request(
            f"{target_path.rstrip('/')}/keys",
            method="POST",
            data={"username": username, "public_key": pub_b64},
        )
        return

    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute(
        """
        INSERT INTO user_keys (username, public_key)
        VALUES (?, ?)
        ON CONFLICT(username) DO UPDATE SET public_key = excluded.public_key
        """,
        (username, sqlite3.Binary(public_key_bytes)),
    )

    con.commit()
    con.close()


def load_public_key(username: str, db_path: str = DB_PATH) -> bytes | None:
    """Load user's raw public key bytes from SQLite (local or remote HTTP)."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        encoded_user = urllib.parse.quote(username)
        res = _http_request(f"{target_path.rstrip('/')}/keys/{encoded_user}", method="GET")
        if res and "public_key" in res:
            return base64.b64decode(res["public_key"])
        return None

    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute(
        "SELECT public_key FROM user_keys WHERE username = ?",
        (username,),
    )

    row = cur.fetchone()
    con.close()

    return row[0] if row else None


def store_message(
    room_id: str,
    sender_id: str,
    ciphertext: bytes,
    nonce: bytes,
    signature: bytes,
    timestamp: str,
    message_id: str | None = None,
    return_created: bool = False,
    db_path: str = DB_PATH,
) -> str | tuple[str, bool]:
    """
    Insert an encrypted, signed message record into SQLite database (local or remote HTTP).
    Plaintext MUST NEVER be stored.
    Enforces uniqueness using PRIMARY KEY and ON CONFLICT DO NOTHING.
    If message_id is omitted, a UUID is automatically generated.
    """
    target_path = db_path or get_db_path()
    if not message_id:
        message_id = str(uuid.uuid4())
    else:
        message_id = str(message_id).strip()

    if is_remote_db(target_path):
        payload = {
            "message_id": message_id,
            "room_id": room_id,
            "sender_id": sender_id,
            "ciphertext": base64.b64encode(ciphertext).decode("utf-8"),
            "nonce": base64.b64encode(nonce).decode("utf-8"),
            "signature": base64.b64encode(signature).decode("utf-8"),
            "timestamp": timestamp,
        }
        res = _http_request(f"{target_path.rstrip('/')}/messages", method="POST", data=payload)
        created = res.get("created", True) if res else True
        if return_created:
            return message_id, created
        return message_id

    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute(
        """
        INSERT INTO messages (message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(message_id) DO NOTHING
        """,
        (
            message_id,
            room_id,
            sender_id,
            sqlite3.Binary(ciphertext),
            sqlite3.Binary(nonce),
            sqlite3.Binary(signature),
            timestamp,
        ),
    )

    created = cur.rowcount > 0
    con.commit()
    con.close()

    if return_created:
        return message_id, created
    return message_id


def load_history_raw(room_id: str, limit: int = 100000, db_path: str = DB_PATH) -> list[dict]:
    """
    Fetch the last `limit` encrypted messages for `room_id` (local or remote HTTP).
    Returns list of dicts with raw binary fields.
    """
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        url = f"{target_path.rstrip('/')}/messages?room_id={urllib.parse.quote(room_id)}&limit={int(limit)}"
        res = _http_request(url, method="GET")
        if not res or "messages" not in res:
            return []
        records = []
        for m in res["messages"]:
            records.append({
                "message_id": m["message_id"],
                "room_id": m["room_id"],
                "sender_id": m["sender_id"],
                "ciphertext": base64.b64decode(m["ciphertext"]),
                "nonce": base64.b64decode(m["nonce"]),
                "signature": base64.b64decode(m["signature"]),
                "timestamp": m["timestamp"],
            })
        return records

    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp, rowid
        FROM messages
        WHERE room_id = ?
        ORDER BY rowid DESC
        LIMIT ?
        """,
        (room_id, limit),
    )

    rows = cur.fetchall()
    con.close()

    records = []
    for row in reversed(rows):
        records.append({
            "message_id": row[0],
            "room_id": row[1],
            "sender_id": row[2],
            "ciphertext": row[3],
            "nonce": row[4],
            "signature": row[5],
            "timestamp": row[6],
        })

    return records


def get_message_by_id(message_id: str | int, db_path: str = DB_PATH) -> dict | None:
    """Fetch a single message by message_id (local or remote HTTP)."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        url = f"{target_path.rstrip('/')}/messages/{urllib.parse.quote(str(message_id))}"
        res = _http_request(url, method="GET")
        if not res or "message" not in res:
            return None
        m = res["message"]
        return {
            "message_id": m["message_id"],
            "room_id": m["room_id"],
            "sender_id": m["sender_id"],
            "ciphertext": base64.b64decode(m["ciphertext"]),
            "nonce": base64.b64decode(m["nonce"]),
            "signature": base64.b64decode(m["signature"]),
            "timestamp": m["timestamp"],
        }

    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
        FROM messages
        WHERE message_id = ?
        """,
        (str(message_id),),
    )

    row = cur.fetchone()
    con.close()

    if not row:
        return None

    return {
        "message_id": row[0],
        "room_id": row[1],
        "sender_id": row[2],
        "ciphertext": row[3],
        "nonce": row[4],
        "signature": row[5],
        "timestamp": row[6],
    }


def tamper_message(
    message_id: str | int,
    new_ciphertext: bytes | None = None,
    new_signature: bytes | None = None,
    db_path: str = DB_PATH,
) -> bool:
    """
    Tamper helper for testing/demonstration purposes.
    Modifies stored ciphertext or signature directly in SQLite database (local or remote).
    """
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        payload = {"message_id": str(message_id)}
        if new_ciphertext is not None:
            payload["ciphertext"] = base64.b64encode(new_ciphertext).decode("utf-8")
        if new_signature is not None:
            payload["signature"] = base64.b64encode(new_signature).decode("utf-8")
        res = _http_request(f"{target_path.rstrip('/')}/tamper", method="POST", data=payload)
        return res.get("modified", False) if res else False

    con = get_db_connection(target_path)
    cur = con.cursor()

    if new_ciphertext is not None:
        cur.execute(
            "UPDATE messages SET ciphertext = ? WHERE message_id = ?",
            (sqlite3.Binary(new_ciphertext), str(message_id)),
        )

    if new_signature is not None:
        cur.execute(
            "UPDATE messages SET signature = ? WHERE message_id = ?",
            (sqlite3.Binary(new_signature), str(message_id)),
        )

    modified = cur.rowcount > 0
    con.commit()
    con.close()
    return modified


# ============================================================
# MULTI-NODE PRESENCE / ONLINE USERS
# ============================================================

def register_online_user(username: str, backend_id: str, db_path: str = DB_PATH) -> bool:
    """Register or heartbeat an online user session across the cluster."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/heartbeat",
            method="POST",
            data={"username": username, "backend_id": backend_id},
        )
        return bool(res and res.get("status") == "success")

    now = int(time.time())
    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute(
        """
        INSERT INTO active_users (username, backend_id, last_seen)
        VALUES (?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET backend_id = excluded.backend_id, last_seen = excluded.last_seen
        """,
        (username, backend_id, now),
    )
    con.commit()
    con.close()
    return True


def remove_online_user(username: str, backend_id: str | None = None, db_path: str = DB_PATH) -> bool:
    """Mark a user as offline across the cluster."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/offline",
            method="POST",
            data={"username": username, "backend_id": backend_id or ""},
        )
        return bool(res and res.get("status") == "success")

    con = get_db_connection(target_path)
    cur = con.cursor()
    if backend_id:
        cur.execute("DELETE FROM active_users WHERE username = ? AND backend_id = ?", (username, backend_id))
    else:
        cur.execute("DELETE FROM active_users WHERE username = ?", (username,))
    con.commit()
    con.close()
    return True


def clear_backend_users(backend_id: str, db_path: str = DB_PATH) -> bool:
    """Clear all online users associated with a given backend node (e.g. upon node restart)."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/clear_backend",
            method="POST",
            data={"backend_id": backend_id},
        )
        return bool(res and res.get("status") == "success")

    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute("DELETE FROM active_users WHERE backend_id = ?", (backend_id,))
    con.commit()
    con.close()
    return True


def get_all_online_users(db_path: str = DB_PATH, prune_seconds: int = 60) -> list[dict] | None:
    """Retrieve all currently active users across the cluster."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/users", method="GET")
            if res and res.get("status") == "success" and "users" in res:
                return res["users"]
        except Exception:
            pass
        return None

    now = int(time.time())
    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute("DELETE FROM active_users WHERE (? - last_seen) > ?", (now, prune_seconds))
    cur.execute("SELECT username, backend_id, last_seen FROM active_users ORDER BY username ASC")
    rows = cur.fetchall()
    con.commit()
    con.close()
    return [{"username": r[0], "backend_id": r[1], "last_seen": r[2]} for r in rows]


def record_cluster_event(event_type: str, message: str, sender_node: str, db_path: str = DB_PATH) -> bool:
    """Record a system/presence event in the shared database so other nodes receive it."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        try:
            res = _http_request(
                f"{target_path.rstrip('/')}/events",
                method="POST",
                data={"event_type": event_type, "message": message, "sender_node": sender_node},
            )
            return bool(res and res.get("status") == "success")
        except Exception:
            return False

    now = int(time.time())
    try:
        con = get_db_connection(target_path)
        cur = con.cursor()
        cur.execute(
            "INSERT INTO system_events (event_type, message, sender_node, timestamp) VALUES (?, ?, ?, ?)",
            (event_type, message, sender_node, now),
        )
        con.commit()
        con.close()
        return True
    except Exception:
        return False


def get_cluster_events(since_id: int, db_path: str = DB_PATH) -> list[dict]:
    """Retrieve new cluster events since the given event ID."""
    target_path = db_path or get_db_path()
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/events?since_id={since_id}", method="GET")
            if res and res.get("status") == "success" and "events" in res:
                return res["events"]
        except Exception:
            pass
        return []

    try:
        con = get_db_connection(target_path)
        cur = con.cursor()
        cur.execute(
            "SELECT id, event_type, message, sender_node, timestamp FROM system_events WHERE id > ? ORDER BY id ASC LIMIT 50",
            (since_id,),
        )
        rows = cur.fetchall()
        con.close()
        return [{"id": r[0], "event_type": r[1], "message": r[2], "sender_node": r[3], "timestamp": r[4]} for r in rows]
    except Exception:
        return []


