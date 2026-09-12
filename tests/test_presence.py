import unittest
import tempfile
import threading
import time
import os
import aiohttp
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop
from aiohttp import web

import db_server
from database import init_db, register_online_user, remove_online_user, get_all_online_users
from server import create_app


class TestClusterPresence(AioHTTPTestCase):

    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.db_file = os.path.join(cls.temp_dir.name, "presence_test.db")
        cls.db_port = 19125
        cls.db_url = f"http://127.0.0.1:{cls.db_port}"

        db_server.DB_PATH = cls.db_file
        db_server.PORT = cls.db_port
        db_server.HOST = "127.0.0.1"
        db_server.init_database()

        cls.http_server = db_server.ThreadingHTTPServer(("127.0.0.1", cls.db_port), db_server.DBRequestHandler)
        cls.server_thread = threading.Thread(target=cls.http_server.serve_forever, daemon=True)
        cls.server_thread.start()
        time.sleep(0.2)

    @classmethod
    def tearDownClass(cls):
        cls.http_server.shutdown()
        cls.http_server.server_close()
        cls.temp_dir.cleanup()

    async def get_application(self):
        return create_app(backend_id="backend-presence-2", db_path=self.db_url)

    @unittest_run_loop
    async def test_combined_online_users_across_backends(self):
        # 1. Simulate Alice connecting to Backend 2
        register_online_user("Alice", "backend-presence-2", db_path=self.db_url)

        # 2. Simulate Bob connecting to Backend 3
        register_online_user("Bob", "backend-presence-3", db_path=self.db_url)

        # 3. Query GET /users from Backend 2
        resp = await self.client.get("/users")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["total_online"], 2)

        usernames = [u["username"] for u in data["users"]]
        self.assertIn("Alice", usernames)
        self.assertIn("Bob", usernames)

        # 4. Simulate Alice disconnecting from Backend 2
        remove_online_user("Alice", "backend-presence-2", db_path=self.db_url)

        # 5. Query GET /online again
        resp2 = await self.client.get("/online")
        self.assertEqual(resp2.status, 200)
        data2 = await resp2.json()
        self.assertEqual(data2["total_online"], 1)
        self.assertEqual(data2["users"][0]["username"], "Bob")
        self.assertEqual(data2["users"][0]["backend_id"], "backend-presence-3")


if __name__ == "__main__":
    unittest.main()
