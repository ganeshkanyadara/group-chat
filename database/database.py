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
import contextlib
import urllib3


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


def is_postgres_db(db_path: str | None = None) -> bool:
    """Check if database destination is a PostgreSQL server."""
    target = db_path or get_db_path()
    return target.startswith("postgresql://") or target.startswith("postgres://")


def is_remote_db(db_path: str | None = None) -> bool:
    """Check if database destination is a remote HTTP/HTTPS database microservice."""
    target = db_path or get_db_path()
    return target.startswith("http://") or target.startswith("https://")


# ============================================================
# POSTGRESQL CONNECTION POOL (Multi-Node High Concurrency)
# ============================================================

_pg_pool = None
_pg_lock = threading.Lock()


def _get_pg_pool(dsn: str):
    global _pg_pool
    if _pg_pool is None:
        with _pg_lock:
            if _pg_pool is None:
                import psycopg2
                from psycopg2.pool import ThreadedConnectionPool
                # Lean connection pool for 512MB container constraint (18 conns * 3 nodes = 54 total)
                _pg_pool = ThreadedConnectionPool(
                    minconn=5,
                    maxconn=18,
                    dsn=dsn,
                )
    return _pg_pool


@contextlib.contextmanager
def get_pg_conn(dsn: str | None = None):
    target = dsn or get_db_path()
    pool = _get_pg_pool(target)
    conn = None
    start = time.monotonic()
    while conn is None:
        try:
            conn = pool.getconn()
        except Exception:
            if (time.monotonic() - start) > 5.0:
                raise
            time.sleep(0.005)
    try:
        yield conn
    finally:
        if conn is not None:
            pool.putconn(conn)


# ============================================================
# HTTP / SQLITE CONNECTION POOLING & HEALTH CACHING
# ============================================================

_pool_manager = None
_pool_lock = threading.Lock()
_last_health_check_time = 0.0
_last_health_status = True
_health_lock = threading.Lock()


def _get_pool() -> urllib3.PoolManager:
    global _pool_manager
    if _pool_manager is None:
        with _pool_lock:
            if _pool_manager is None:
                retries = urllib3.Retry(
                    total=3,
                    backoff_factor=0.01,
                    status_forcelist=[500, 502, 503, 504],
                    raise_on_status=False,
                )
                _pool_manager = urllib3.PoolManager(
                    maxsize=200,
                    retries=retries,
                    timeout=urllib3.Timeout(connect=2.0, read=5.0),
                )
    return _pool_manager


def _record_db_success():
    global _last_health_check_time, _last_health_status
    with _health_lock:
        _last_health_check_time = time.time()
        _last_health_status = True


def _http_request(url: str, method: str = "GET", data: dict | None = None, timeout: float = 5.0) -> dict | None:
    """Perform pooled HTTP/1.1 JSON request to remote DB microservice reusing persistent TCP connections."""
    pool = _get_pool()
    headers = {
        "Content-Type": "application/json",
        "Connection": "keep-alive",
    }
    body_bytes = json.dumps(data).encode("utf-8") if data is not None else None

    try:
        resp = pool.request(
            method,
            url,
            body=body_bytes,
            headers=headers,
            timeout=timeout,
        )
        if resp.status == 404:
            return None
        if resp.status >= 500:
            raise RuntimeError(f"Remote DB error: HTTP {resp.status}")
        content = resp.data.decode("utf-8")
        _record_db_success()
        if not content:
            return {}
        return json.loads(content)
    except Exception:
        # Retry with standard urlopen as ultimate fail-safe
        try:
            req = urllib.request.Request(url, data=body_bytes, headers=headers, method=method)
            with urllib.request.urlopen(req, timeout=timeout) as fb_resp:
                fb_content = fb_resp.read().decode("utf-8")
                _record_db_success()
                return json.loads(fb_content) if fb_content else {}
        except Exception:
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
    """Lightweight check verifying database connectivity (PostgreSQL, remote HTTP server, or local SQLite)."""
    global _last_health_check_time, _last_health_status
    target_path = db_path or get_db_path()

    now = time.time()
    with _health_lock:
        if (now - _last_health_check_time) < 3.0 and _last_health_status:
            return True

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        try:
            with get_pg_conn(target_path) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1;")
                    cur.fetchone()
            _record_db_success()
            with _health_lock:
                _last_health_check_time = now
                _last_health_status = True
            return True
        except Exception:
            with _health_lock:
                _last_health_check_time = now
                _last_health_status = False
            return False

    # 2. Remote HTTP Microservice
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/health", timeout=timeout)
            healthy = res is not None and res.get("status") == "healthy"
            with _health_lock:
                _last_health_check_time = now
                _last_health_status = healthy
            return healthy
        except Exception:
            return False

    # 3. Local SQLite
    try:
        con = sqlite3.connect(target_path, timeout=timeout)
        cur = con.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        con.close()
        _record_db_success()
        return True
    except Exception:
        return False


def init_db(db_path: str = DB_PATH) -> None:
    """Initialize database tables for messages, keys, active users, and system events."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS messages (
                        message_id  TEXT PRIMARY KEY,
                        room_id     TEXT NOT NULL,
                        sender_id   TEXT NOT NULL,
                        ciphertext  BYTEA NOT NULL,
                        nonce       BYTEA NOT NULL,
                        signature   BYTEA NOT NULL,
                        timestamp   TEXT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS user_keys (
                        username    TEXT PRIMARY KEY,
                        public_key  BYTEA NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS active_users (
                        username    TEXT PRIMARY KEY,
                        backend_id  TEXT NOT NULL,
                        last_seen   BIGINT NOT NULL
                    );
                    CREATE TABLE IF NOT EXISTS system_events (
                        id          BIGSERIAL PRIMARY KEY,
                        event_type  TEXT NOT NULL,
                        message     TEXT NOT NULL,
                        sender_node TEXT NOT NULL,
                        timestamp   BIGINT NOT NULL
                    );
                    CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room_id, timestamp);
                """)
            conn.commit()
        return

    # 2. Remote HTTP
    if is_remote_db(target_path):
        try:
            _http_request(f"{target_path.rstrip('/')}/init", method="POST", timeout=2.0)
        except Exception:
            pass
        return

    # 3. Local SQLite
    con = get_db_connection(target_path)
    cur = con.cursor()

    cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='messages';")
    table_exists = cur.fetchone() is not None

    if table_exists:
        cur.execute("PRAGMA table_info(messages);")
        columns = cur.fetchall()
        msg_id_col = next((c for c in columns if c[1] == "message_id"), None)
        if msg_id_col and "INT" in str(msg_id_col[2]).upper():
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

    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_keys (
            username    TEXT PRIMARY KEY,
            public_key  BLOB NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS active_users (
            username    TEXT PRIMARY KEY,
            backend_id  TEXT NOT NULL,
            last_seen   INTEGER NOT NULL
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS system_events (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            event_type  TEXT NOT NULL,
            message     TEXT NOT NULL,
            sender_node TEXT NOT NULL,
            timestamp   INTEGER NOT NULL
        );
    """)

    cur.execute("CREATE INDEX IF NOT EXISTS idx_messages_room_ts ON messages(room_id, timestamp);")

    con.commit()
    con.close()


def save_public_key(username: str, public_key_bytes: bytes, db_path: str = DB_PATH) -> None:
    """Store or update user's raw Ed25519 public key bytes."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        import psycopg2
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO user_keys (username, public_key)
                    VALUES (%s, %s)
                    ON CONFLICT(username) DO UPDATE SET public_key = EXCLUDED.public_key;
                    """,
                    (username, psycopg2.Binary(public_key_bytes)),
                )
            conn.commit()
        return

    # 2. Remote HTTP
    if is_remote_db(target_path):
        pub_b64 = base64.b64encode(public_key_bytes).decode("utf-8")
        _http_request(
            f"{target_path.rstrip('/')}/keys",
            method="POST",
            data={"username": username, "public_key": pub_b64},
        )
        return

    # 3. Local SQLite
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
    """Load user's raw public key bytes."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT public_key FROM user_keys WHERE username = %s;", (username,))
                row = cur.fetchone()
                return bytes(row[0]) if row else None

    # 2. Remote HTTP
    if is_remote_db(target_path):
        encoded_user = urllib.parse.quote(username)
        res = _http_request(f"{target_path.rstrip('/')}/keys/{encoded_user}", method="GET")
        if res and "public_key" in res:
            return base64.b64decode(res["public_key"])
        return None

    # 3. Local SQLite
    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute("SELECT public_key FROM user_keys WHERE username = ?", (username,))
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
    """Insert an encrypted, signed message record idempotently into database."""
    target_path = db_path or get_db_path()
    if not message_id:
        message_id = str(uuid.uuid4())
    else:
        message_id = str(message_id).strip()

    # 1. PostgreSQL (Native binary insertion with zero encoding overhead)
    if is_postgres_db(target_path):
        import psycopg2
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO messages (message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (message_id) DO NOTHING
                    RETURNING message_id;
                    """,
                    (
                        message_id,
                        room_id,
                        sender_id,
                        psycopg2.Binary(ciphertext),
                        psycopg2.Binary(nonce),
                        psycopg2.Binary(signature),
                        timestamp,
                    ),
                )
                row = cur.fetchone()
                created = (row is not None)
            conn.commit()

        if return_created:
            return message_id, created
        return message_id

    # 2. Remote HTTP
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

    # 3. Local SQLite
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
    """Fetch the last `limit` encrypted messages for `room_id` in chronological order."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
                    FROM messages
                    WHERE room_id = %s
                    ORDER BY timestamp ASC
                    LIMIT %s;
                    """,
                    (room_id, limit),
                )
                rows = cur.fetchall()
            return [
                {
                    "message_id": r[0],
                    "room_id": r[1],
                    "sender_id": r[2],
                    "ciphertext": bytes(r[3]),
                    "nonce": bytes(r[4]),
                    "signature": bytes(r[5]),
                    "timestamp": r[6],
                }
                for r in rows
            ]

    # 2. Remote HTTP
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

    # 3. Local SQLite
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
    """Fetch a single message by message_id."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
                    FROM messages
                    WHERE message_id = %s;
                    """,
                    (str(message_id),),
                )
                r = cur.fetchone()
                if not r:
                    return None
                return {
                    "message_id": r[0],
                    "room_id": r[1],
                    "sender_id": r[2],
                    "ciphertext": bytes(r[3]),
                    "nonce": bytes(r[4]),
                    "signature": bytes(r[5]),
                    "timestamp": r[6],
                }

    # 2. Remote HTTP
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

    # 3. Local SQLite
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
    """Modify stored ciphertext or signature for testing integrity verifications."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        import psycopg2
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                modified = False
                if new_ciphertext is not None:
                    cur.execute("UPDATE messages SET ciphertext = %s WHERE message_id = %s;", (psycopg2.Binary(new_ciphertext), str(message_id)))
                    modified = modified or (cur.rowcount > 0)
                if new_signature is not None:
                    cur.execute("UPDATE messages SET signature = %s WHERE message_id = %s;", (psycopg2.Binary(new_signature), str(message_id)))
                    modified = modified or (cur.rowcount > 0)
            conn.commit()
            return modified

    # 2. Remote HTTP
    if is_remote_db(target_path):
        payload = {"message_id": str(message_id)}
        if new_ciphertext is not None:
            payload["ciphertext"] = base64.b64encode(new_ciphertext).decode("utf-8")
        if new_signature is not None:
            payload["signature"] = base64.b64encode(new_signature).decode("utf-8")
        res = _http_request(f"{target_path.rstrip('/')}/tamper", method="POST", data=payload)
        return res.get("modified", False) if res else False

    # 3. Local SQLite
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

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        now = int(time.time())
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO active_users (username, backend_id, last_seen)
                    VALUES (%s, %s, %s)
                    ON CONFLICT(username) DO UPDATE SET backend_id = EXCLUDED.backend_id, last_seen = EXCLUDED.last_seen;
                    """,
                    (username, backend_id, now),
                )
            conn.commit()
        return True

    # 2. Remote HTTP
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/heartbeat",
            method="POST",
            data={"username": username, "backend_id": backend_id},
        )
        return bool(res and res.get("status") == "success")

    # 3. Local SQLite
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

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                if backend_id:
                    cur.execute("DELETE FROM active_users WHERE username = %s AND backend_id = %s;", (username, backend_id))
                else:
                    cur.execute("DELETE FROM active_users WHERE username = %s;", (username,))
            conn.commit()
        return True

    # 2. Remote HTTP
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/offline",
            method="POST",
            data={"username": username, "backend_id": backend_id or ""},
        )
        return bool(res and res.get("status") == "success")

    # 3. Local SQLite
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
    """Clear all online users associated with a given backend node."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM active_users WHERE backend_id = %s;", (backend_id,))
            conn.commit()
        return True

    # 2. Remote HTTP
    if is_remote_db(target_path):
        res = _http_request(
            f"{target_path.rstrip('/')}/users/clear_backend",
            method="POST",
            data={"backend_id": backend_id},
        )
        return bool(res and res.get("status") == "success")

    # 3. Local SQLite
    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute("DELETE FROM active_users WHERE backend_id = ?", (backend_id,))
    con.commit()
    con.close()
    return True


def get_all_online_users(db_path: str = DB_PATH, prune_seconds: int = 60) -> list[dict] | None:
    """Retrieve all currently active users across the cluster."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        now = int(time.time())
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM active_users WHERE last_seen < %s;", (now - prune_seconds,))
                cur.execute("SELECT username, backend_id, last_seen FROM active_users ORDER BY username ASC;")
                rows = cur.fetchall()
            conn.commit()
            return [{"username": r[0], "backend_id": r[1], "last_seen": r[2]} for r in rows]

    # 2. Remote HTTP
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/users", method="GET")
            if res and res.get("status") == "success" and "users" in res:
                return res["users"]
        except Exception:
            pass
        return None

    # 3. Local SQLite
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
    """Record a system/presence event in the shared database."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        now = int(time.time())
        try:
            with get_pg_conn(target_path) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "INSERT INTO system_events (event_type, message, sender_node, timestamp) VALUES (%s, %s, %s, %s);",
                        (event_type, message, sender_node, now),
                    )
                conn.commit()
            return True
        except Exception:
            return False

    # 2. Remote HTTP
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

    # 3. Local SQLite
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

    # 1. PostgreSQL
    if is_postgres_db(target_path):
        try:
            with get_pg_conn(target_path) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "SELECT id, event_type, message, sender_node, timestamp FROM system_events WHERE id > %s ORDER BY id ASC LIMIT 50;",
                        (since_id,),
                    )
                    rows = cur.fetchall()
            return [{"id": r[0], "event_type": r[1], "message": r[2], "sender_node": r[3], "timestamp": r[4]} for r in rows]
        except Exception:
            return []

    # 2. Remote HTTP
    if is_remote_db(target_path):
        try:
            res = _http_request(f"{target_path.rstrip('/')}/events?since_id={since_id}", method="GET")
            if res and res.get("status") == "success" and "events" in res:
                return res["events"]
        except Exception:
            pass
        return []

    # 3. Local SQLite
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


def clear_database(db_path: str = DB_PATH) -> None:
    """Reset / empty all database tables for a fresh benchmark run."""
    target_path = db_path or get_db_path()

    # 1. PostgreSQL (Ultra-fast instantaneous table truncate)
    if is_postgres_db(target_path):
        with get_pg_conn(target_path) as conn:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE messages, user_keys, active_users, system_events RESTART IDENTITY;")
            conn.commit()
        return

    # 2. Local SQLite
    con = get_db_connection(target_path)
    cur = con.cursor()
    cur.execute("DELETE FROM messages;")
    cur.execute("DELETE FROM user_keys;")
    cur.execute("DELETE FROM active_users;")
    cur.execute("DELETE FROM system_events;")
    con.commit()
    con.close()
