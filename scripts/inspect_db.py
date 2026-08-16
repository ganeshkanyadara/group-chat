#!/usr/bin/env python3
"""
Database Inspection Utility for Group Chat Evaluation & Demonstration.

Inspects SQLite database records directly without decrypting them,
proving that plaintext is NOT stored in the database.
"""

import sys
import sqlite3
import base64
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import DB_PATH


def main():
    db_file = DB_PATH
    if len(sys.argv) > 1:
        db_file = sys.argv[1]

    if not Path(db_file).exists():
        print(f"[ERROR] Database file '{db_file}' not found.")
        sys.exit(1)

    print("=======================================================")
    print("        Group Chat — SQLite Database Inspector         ")
    print("=======================================================")
    print(f"Database Path: {db_file}\n")

    con = sqlite3.connect(db_file)
    cur = con.cursor()

    # Query public keys
    cur.execute("SELECT username, public_key FROM user_keys")
    user_keys = cur.fetchall()

    print(f"--- STORED PUBLIC KEYS ({len(user_keys)} users) ---")
    if not user_keys:
        print("  (No user keys stored yet)")
    else:
        for username, pub_bytes in user_keys:
            pub_b64 = base64.b64encode(pub_bytes).decode("utf-8") if isinstance(pub_bytes, bytes) else str(pub_bytes)
            print(f"  Username: {username:<12} | Public Key (b64): {pub_b64[:32]}...")

    print("\n" + "=" * 55)

    # Query messages
    cur.execute("""
        SELECT message_id, room_id, sender_id, ciphertext, nonce, signature, timestamp
        FROM messages
        ORDER BY message_id ASC
    """)
    messages = cur.fetchall()

    print(f"--- STORED MESSAGES ({len(messages)} total records) ---")
    print("Note: The database stores ONLY binary ciphertext, nonce, and signature.")
    print("NO PLAINTEXT IS STORED IN SQLITE.\n")

    if not messages:
        print("  (No messages stored yet. Run the server and send a message!)")
    else:
        for msg in messages:
            msg_id, room_id, sender_id, ciphertext, nonce, signature, timestamp = msg
            ct_b64 = base64.b64encode(ciphertext).decode("utf-8") if isinstance(ciphertext, bytes) else str(ciphertext)
            nonce_b64 = base64.b64encode(nonce).decode("utf-8") if isinstance(nonce, bytes) else str(nonce)
            sig_b64 = base64.b64encode(signature).decode("utf-8") if isinstance(signature, bytes) else str(signature)

            print(f"Record #{msg_id} | Room: {room_id} | Sender: {sender_id} | Time: {timestamp}")
            print(f"  Ciphertext (b64): {ct_b64[:48]}... ({len(ciphertext)} bytes)")
            print(f"  Nonce      (b64): {nonce_b64} ({len(nonce)} bytes)")
            print(f"  Signature  (b64): {sig_b64[:48]}... ({len(signature)} bytes)")
            print("-" * 55)

    con.close()


if __name__ == "__main__":
    main()
