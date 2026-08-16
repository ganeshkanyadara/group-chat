#!/usr/bin/env python3
"""
Tamper Demonstration Utility for Group Chat Evaluation.

Intentionally alters stored ciphertext or digital signatures in SQLite `chat.db`
to demonstrate signature verification failure and tamper detection.
"""

import sys
import sqlite3
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import DB_PATH, get_message_by_id, tamper_message


def main():
    print("=======================================================")
    print("        Group Chat — Tamper Demonstration Tool          ")
    print("=======================================================")

    if not Path(DB_PATH).exists():
        print(f"[ERROR] Database '{DB_PATH}' does not exist. Send some messages first!")
        sys.exit(1)

    con = sqlite3.connect(DB_PATH)
    cur = con.cursor()
    cur.execute("SELECT message_id, sender_id, timestamp FROM messages ORDER BY message_id DESC")
    rows = cur.fetchall()
    con.close()

    if not rows:
        print("[ERROR] No messages found in database to tamper with.")
        sys.exit(1)

    print("\nAvailable messages in database:")
    for msg_id, sender, ts in rows:
        print(f"  [{msg_id}] Sender: {sender:<10} | Time: {ts}")

    target_id = rows[0][0]  # Default to latest message
    if len(sys.argv) > 1:
        try:
            target_id = int(sys.argv[1])
        except ValueError:
            pass

    record = get_message_by_id(target_id, db_path=DB_PATH)
    if not record:
        print(f"[ERROR] Message ID {target_id} not found.")
        sys.exit(1)

    print(f"\nSelected Message ID #{target_id} from '{record['sender_id']}'.")
    print("Tampering Options:")
    print("  1. Flip byte in Ciphertext (Triggers AES-GCM & Signature failure)")
    print("  2. Flip byte in Signature (Triggers Digital Signature failure)")

    choice = "1"
    if len(sys.argv) > 2:
        choice = sys.argv[2]
    elif sys.stdin.isatty():
        entered = input("Enter choice (1/2) [default=1]: ").strip()
        if entered:
            choice = entered

    if choice == "2":
        orig_sig = bytearray(record["signature"])
        orig_sig[0] ^= 0xFF  # Flip bits in signature
        tamper_message(target_id, new_signature=bytes(orig_sig), db_path=DB_PATH)
        print(f"\n[TAMPERED] Signature for Message #{target_id} corrupted.")
        print("Now reconnect/refresh the client. The signature check (✓/✗) will display UNVERIFIED (✗)!")
    else:
        orig_ct = bytearray(record["ciphertext"])
        orig_ct[0] ^= 0xFF  # Flip bits in ciphertext
        tamper_message(target_id, new_ciphertext=bytes(orig_ct), db_path=DB_PATH)
        print(f"\n[TAMPERED] Ciphertext for Message #{target_id} corrupted.")
        print("Now reconnect/refresh the client. Decryption will fail and trigger a security alert!")


if __name__ == "__main__":
    main()
