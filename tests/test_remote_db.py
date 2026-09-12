import asyncio
import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from http.server import ThreadingHTTPServer
import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import db_server
from database import (
    init_db,
    check_db_health,
    store_message,
    load_history_raw,
    save_public_key,
    load_public_key,
)
import server


class TestRemoteDatabaseServer(unittest.IsolatedAsyncioTestCase):

    @classmethod
    def setUpClass(cls):
        cls.test_dir = tempfile.TemporaryDirectory()
        cls.db_file = os.path.join(cls.test_dir.name, "remote_test.db")
        cls.port = 9123
        cls.remote_url = f"http://127.0.0.1:{cls.port}"

        # Configure db_server
        db_server.DB_PATH = cls.db_file
        db_server.init_database()

        # Start db_server in background thread
        cls.httpd = ThreadingHTTPServer(("127.0.0.1", cls.port), db_server.DBRequestHandler)
        cls.server_thread = threading.Thread(target=cls.httpd.serve_forever, daemon=True)
        cls.server_thread.start()

    @classmethod
    def tearDownClass(cls):
        cls.httpd.shutdown()
        cls.httpd.server_close()
        cls.test_dir.cleanup()

    async def asyncSetUp(self):
        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()

    def test_1_direct_remote_db_crud(self):
        """Test database.py functions using remote HTTP database URL."""
        # 1. Health check
        self.assertTrue(check_db_health(self.remote_url))

        # 2. Key storage
        save_public_key("remote_user", b"dummy_pub_key_32bytes_12345678", db_path=self.remote_url)
        loaded_key = load_public_key("remote_user", db_path=self.remote_url)
        self.assertEqual(loaded_key, b"dummy_pub_key_32bytes_12345678")

        # 3. Message store
        mid, created = store_message(
            room_id="main",
            sender_id="remote_user",
            ciphertext=b"encrypted_content_bytes",
            nonce=b"12byte_nonce",
            signature=b"64byte_sig",
            timestamp="2026-09-12T12:00:00Z",
            message_id="REMOTE-MSG-001",
            return_created=True,
            db_path=self.remote_url,
        )
        self.assertEqual(mid, "REMOTE-MSG-001")
        self.assertTrue(created)

        # 4. Duplicate store (idempotent)
        mid2, created2 = store_message(
            room_id="main",
            sender_id="remote_user",
            ciphertext=b"encrypted_content_bytes",
            nonce=b"12byte_nonce",
            signature=b"64byte_sig",
            timestamp="2026-09-12T12:00:00Z",
            message_id="REMOTE-MSG-001",
            return_created=True,
            db_path=self.remote_url,
        )
        self.assertEqual(mid2, "REMOTE-MSG-001")
        self.assertFalse(created2)

        # 5. History retrieval
        records = load_history_raw(room_id="main", limit=5, db_path=self.remote_url)
        self.assertGreaterEqual(len(records), 1)
        self.assertEqual(records[0]["ciphertext"], b"encrypted_content_bytes")

    async def test_2_backend_servers_with_remote_db(self):
        """
        Start 2 backend instances (ports 8201, 8202) pointing DATABASE_URL to http://127.0.0.1:9123
        and verify full cross-backend communication!
        """
        keys_dir = Path(self.test_dir.name) / "keys"
        server.key_manager = server.KeyManager(keys_dir=keys_dir, db_path=self.remote_url)

        # Backend 2
        app2 = server.create_app(backend_id="backend-2", db_path=self.remote_url)
        runner2 = aiohttp.web.AppRunner(app2, access_log=None)
        await runner2.setup()
        site2 = aiohttp.web.TCPSite(runner2, "127.0.0.1", 8201)
        await site2.start()

        # Backend 3
        app3 = server.create_app(backend_id="backend-3", db_path=self.remote_url)
        runner3 = aiohttp.web.AppRunner(app3, access_log=None)
        await runner3.setup()
        site3 = aiohttp.web.TCPSite(runner3, "127.0.0.1", 8202)
        await site3.start()

        try:
            # 1. Check health
            async with self.session.get("http://127.0.0.1:8201/health") as r:
                self.assertEqual(r.status, 200)
                data = await r.json()
                self.assertEqual(data["status"], "healthy")
                self.assertEqual(data["database"], "connected")

            # 2. Post message to Backend 2
            post_payload = {
                "client-name": "sys2_client",
                "msg": "Written to Remote SQLite Server on System 1!",
                "message_id": "SYS1-REMOTE-999",
            }
            async with self.session.post("http://127.0.0.1:8201/message", json=post_payload) as r:
                self.assertEqual(r.status, 201)
                data = await r.json()
                self.assertEqual(data["backend_id"], "backend-2")
                self.assertTrue(data["created"])

            # 3. Read feed directly from Backend 3
            async with self.session.get("http://127.0.0.1:8202/feed") as r:
                self.assertEqual(r.status, 200)
                feed_data = await r.json()
                self.assertEqual(feed_data["backend_id"], "backend-3")
                matching = [m for m in feed_data["messages"] if m["message_id"] == "SYS1-REMOTE-999"]
                self.assertEqual(len(matching), 1)
                self.assertEqual(matching[0]["msg"], "Written to Remote SQLite Server on System 1!")
                self.assertTrue(matching[0]["verified"])

            # 4. Test duplicate submission to Backend 3
            async with self.session.post("http://127.0.0.1:8202/message", json=post_payload) as r:
                self.assertEqual(r.status, 200)
                data = await r.json()
                self.assertFalse(data["created"])

        finally:
            await runner2.cleanup()
            await runner3.cleanup()


if __name__ == "__main__":
    unittest.main()
