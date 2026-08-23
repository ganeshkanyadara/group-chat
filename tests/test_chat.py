import os
import sys
import unittest
import tempfile
import sqlite3
import base64
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import (
    init_db,
    store_message,
    load_history_raw,
    tamper_message,
    save_public_key,
    load_public_key,
)
from crypto import (
    encrypt_message,
    decrypt_message,
    sign_message,
    verify_signature,
    construct_canonical_payload,
    KeyManager,
    get_encryption_key,
)


class TestGroupChatSecurityAndPersistence(unittest.TestCase):

    def setUp(self):
        """Set up a temporary directory and test SQLite database for isolated test runs."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.test_dir.name, "test_chat.db")
        self.keys_dir = Path(self.test_dir.name) / "keys"

        # Initialize DB and KeyManager
        init_db(self.db_path)
        self.key_mgr = KeyManager(keys_dir=self.keys_dir, db_path=self.db_path)
        self.encryption_key = get_encryption_key()

    def tearDown(self):
        self.test_dir.cleanup()

    def test_1_normal_message_flow(self):
        """Test 1: Send a message, store it, decrypt it, and verify signature."""
        room_id = "main"
        sender_id = "ganesh"
        msg_text = "Hello, this is a test message!"
        timestamp = "2026-08-16T12:00:00Z"

        priv_key, pub_bytes = self.key_mgr.get_or_create_user_keypair(sender_id)
        canonical = construct_canonical_payload(room_id, sender_id, msg_text, timestamp)
        sig_bytes = sign_message(priv_key, canonical)
        ct_bytes, nonce_bytes = encrypt_message(msg_text, key=self.encryption_key)

        msg_id = store_message(
            room_id=room_id,
            sender_id=sender_id,
            ciphertext=ct_bytes,
            nonce=nonce_bytes,
            signature=sig_bytes,
            timestamp=timestamp,
            db_path=self.db_path,
        )

        self.assertIsNotNone(msg_id)
        records = load_history_raw(room_id, limit=10, db_path=self.db_path)
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["sender_id"], sender_id)

    def test_2_persistence(self):
        """Test 2: Verify messages persist in SQLite and survive simulated reconnects."""
        room_id = "main"
        sender = "venu"
        msg = "Persistent message check"
        ts = "2026-08-16T12:05:00Z"

        priv_key, _ = self.key_mgr.get_or_create_user_keypair(sender)
        canonical = construct_canonical_payload(room_id, sender, msg, ts)
        sig = sign_message(priv_key, canonical)
        ct, nonce = encrypt_message(msg, key=self.encryption_key)

        store_message(room_id, sender, ct, nonce, sig, ts, db_path=self.db_path)

        # Simulate reconnect by reloading raw history from DB
        history = load_history_raw(room_id, limit=5, db_path=self.db_path)
        self.assertEqual(len(history), 1)

        decrypted = decrypt_message(history[0]["ciphertext"], history[0]["nonce"], key=self.encryption_key)
        self.assertEqual(decrypted, msg)

    def test_3_encryption_no_plaintext_stored(self):
        """Test 3: Verify the SQLite database contains ciphertext and NOT plaintext."""
        room_id = "main"
        sender = "venkat"
        secret_plaintext = "Meet me at 5 PM. Secret password is 1234"
        ts = "2026-08-16T12:10:00Z"

        priv_key, _ = self.key_mgr.get_or_create_user_keypair(sender)
        canonical = construct_canonical_payload(room_id, sender, secret_plaintext, ts)
        sig = sign_message(priv_key, canonical)
        ct, nonce = encrypt_message(secret_plaintext, key=self.encryption_key)

        store_message(room_id, sender, ct, nonce, sig, ts, db_path=self.db_path)

        # Inspect raw SQLite file content as string
        con = sqlite3.connect(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT ciphertext FROM messages")
        raw_ct = cur.fetchone()[0]
        con.close()

        # Ensure plaintext string is nowhere in the raw ciphertext bytes
        self.assertNotIn(secret_plaintext.encode("utf-8"), raw_ct)
        self.assertNotEqual(raw_ct, secret_plaintext.encode("utf-8"))

    def test_4_decryption(self):
        """Test 4: Verify ciphertext correctly decrypts back to original plaintext."""
        plaintext = "AES-GCM Encryption Decryption Test"
        ct, nonce = encrypt_message(plaintext, key=self.encryption_key)
        decrypted = decrypt_message(ct, nonce, key=self.encryption_key)
        self.assertEqual(decrypted, plaintext)

    def test_5_signature_verification(self):
        """Test 5: Verify valid Ed25519 signature passes verification."""
        sender = "dheemanth"
        room = "main"
        msg = "Signed payload message"
        ts = "2026-08-16T12:15:00Z"

        priv_key, pub_bytes = self.key_mgr.get_or_create_user_keypair(sender)
        canonical = construct_canonical_payload(room, sender, msg, ts)
        sig = sign_message(priv_key, canonical)

        is_valid = verify_signature(pub_bytes, canonical, sig)
        self.assertTrue(is_valid)

    def test_6_tampering_detection(self):
        """Test 6: Modify stored message ciphertext or signature and verify verification fails."""
        room = "main"
        sender = "ganesh"
        msg = "Original authentic message"
        ts = "2026-08-16T12:20:00Z"

        priv_key, pub_bytes = self.key_mgr.get_or_create_user_keypair(sender)
        canonical = construct_canonical_payload(room, sender, msg, ts)
        sig = sign_message(priv_key, canonical)
        ct, nonce = encrypt_message(msg, key=self.encryption_key)

        msg_id = store_message(room, sender, ct, nonce, sig, ts, db_path=self.db_path)

        # Corrupt signature in SQLite
        corrupted_sig = bytearray(sig)
        corrupted_sig[0] ^= 0xFF
        tamper_message(msg_id, new_signature=bytes(corrupted_sig), db_path=self.db_path)

        history = load_history_raw(room, limit=1, db_path=self.db_path)
        tampered_sig = history[0]["signature"]

        # Signature verification must fail
        self.assertFalse(verify_signature(pub_bytes, canonical, tampered_sig))

    def test_7_dynamic_user_keypair_generation(self):
        """Test 7: Verify dynamic keypair generation and signing for any new guest user."""
        any_user = "guest_user_9999"
        priv_key, pub_bytes = self.key_mgr.get_or_create_user_keypair(any_user)
        canonical = construct_canonical_payload("main", any_user, "Hello from open user", "2026-08-16T12:25:00Z")
        sig = sign_message(priv_key, canonical)
        self.assertTrue(verify_signature(pub_bytes, canonical, sig))

    def test_8_multiple_users(self):
        """Test 8: Verify multiple arbitrary users can sign and verify messages independently."""
        users = ["user_alpha", "user_beta", "user_gamma", "user_delta"]
        for u in users:
            priv, pub = self.key_mgr.get_or_create_user_keypair(u)
            canonical = construct_canonical_payload("main", u, f"Hello from {u}", "2026-08-16T12:30:00Z")
            sig = sign_message(priv, canonical)
            self.assertTrue(verify_signature(pub, canonical, sig))


if __name__ == "__main__":
    unittest.main()
