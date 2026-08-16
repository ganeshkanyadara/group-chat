import sqlite3
import os

DB_PATH = os.getenv("CHAT_DB_PATH", "chat.db")


def init_db(db_path: str = DB_PATH) -> None:
    """Initialize database tables for messages and public keys if they do not exist."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    # Table storing binary encrypted ciphertext, nonce, and signature
    cur.execute("""
        CREATE TABLE IF NOT EXISTS messages (
            message_id  INTEGER PRIMARY KEY AUTOINCREMENT,
            room_id     TEXT    NOT NULL,
            sender_id   TEXT    NOT NULL,
            ciphertext  BLOB    NOT NULL,
            nonce       BLOB    NOT NULL,
            signature   BLOB    NOT NULL,
            timestamp   TEXT    NOT NULL
        );
    """)

    # Table storing raw public keys for sender signature verification
    cur.execute("""
        CREATE TABLE IF NOT EXISTS user_keys (
            username    TEXT PRIMARY KEY,
            public_key  BLOB NOT NULL
        );
    """)

    con.commit()
    con.close()


def save_public_key(username: str, public_key_bytes: bytes, db_path: str = DB_PATH) -> None:
    """Store or update user's raw Ed25519 public key bytes in SQLite."""
    con = sqlite3.connect(db_path)
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
    """Load user's raw public key bytes from SQLite."""
    con = sqlite3.connect(db_path)
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
    db_path: str = DB_PATH,
) -> int:
    """
    Insert an encrypted, signed message record into SQLite database.
    Plaintext MUST NEVER be stored.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        INSERT INTO messages (room_id, sender_id, ciphertext, nonce, signature, timestamp)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            room_id,
            sender_id,
            sqlite3.Binary(ciphertext),
            sqlite3.Binary(nonce),
            sqlite3.Binary(signature),
            timestamp,
        ),
    )

    msg_id = cur.lastrowid
    con.commit()
    con.close()
    return msg_id


def load_history_raw(room_id: str, limit: int = 50, db_path: str = DB_PATH) -> list[dict]:
    """
    Fetch the last `limit` encrypted messages for `room_id`.
    Returns list of dicts with raw binary fields.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
        FROM messages
        WHERE room_id = ?
        ORDER BY message_id DESC
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


def get_message_by_id(message_id: int, db_path: str = DB_PATH) -> dict | None:
    """Fetch a single message by message_id."""
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    cur.execute(
        """
        SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
        FROM messages
        WHERE message_id = ?
        """,
        (message_id,),
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
    message_id: int,
    new_ciphertext: bytes | None = None,
    new_signature: bytes | None = None,
    db_path: str = DB_PATH,
) -> bool:
    """
    Tamper helper for testing/demonstration purposes.
    Modifies stored ciphertext or signature directly in SQLite database.
    """
    con = sqlite3.connect(db_path)
    cur = con.cursor()

    if new_ciphertext is not None:
        cur.execute(
            "UPDATE messages SET ciphertext = ? WHERE message_id = ?",
            (sqlite3.Binary(new_ciphertext), message_id),
        )

    if new_signature is not None:
        cur.execute(
            "UPDATE messages SET signature = ? WHERE message_id = ?",
            (sqlite3.Binary(new_signature), message_id),
        )

    modified = cur.rowcount > 0
    con.commit()
    con.close()
    return modified
