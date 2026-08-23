#!/usr/bin/env python3
"""
Key Generation Script for Group Chat.

Generates:
1. Symmetric AES-256 key for message encryption (.env / keys/chat_encryption.key)
2. Ed25519 asymmetric signing key pairs for all authorized users (keys/<user>_private.pem)
3. Persists public keys to SQLite database (`user_keys` table)
"""

import sys
import os
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from crypto import get_encryption_key, KeyManager
from database import init_db, DB_PATH


def main():
    print("=======================================================")
    print("        Group Chat — Key Setup & Generation           ")
    print("=======================================================")

    # 1. Initialize DB
    init_db(DB_PATH)

    # 2. Generate/Check AES-256 encryption key
    aes_key = get_encryption_key()
    print(f"[AES-256] Key generated/loaded ({len(aes_key)} bytes).")
    print(f"          Hex representation: {aes_key.hex()}")

    # 3. Create .env if not present
    env_file = Path(".env")
    if not env_file.exists():
        env_file.write_text(f"# Group Chat Security Environment Configuration\nCHAT_ENCRYPTION_KEY={aes_key.hex()}\n")
        print("[.env] Created default .env file with CHAT_ENCRYPTION_KEY.")

    # 4. Generate Ed25519 user keypairs (keys are created automatically on user join)
    km = KeyManager(db_path=DB_PATH)
    sample_users = ["ganesh", "venu", "guest_1"]
    print("\nGenerating/Verifying sample Ed25519 keypairs:")
    for username in sample_users:
        priv_key, pub_bytes = km.get_or_create_user_keypair(username)
        pem_path = km.get_private_key_path(username)
        print(f"  - User '{username}':")
        print(f"      Private key file: {pem_path}")
        print(f"      Public key (hex):  {pub_bytes.hex()[:32]}...")

    print("\n[SUCCESS] Encryption and signing infrastructure initialized successfully.")


if __name__ == "__main__":
    main()
