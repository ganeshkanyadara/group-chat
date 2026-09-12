import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
from aiohttp.test_utils import AioHTTPTestCase, unittest_run_loop, TestClient, TestServer
from aiohttp import web

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import (
    init_db,
    check_db_health,
    load_history_raw,
    get_db_connection,
)
from metrics import metrics_tracker
import server


class TestAssessmentBackend(AioHTTPTestCase):

    async def get_application(self):
        """Create an aiohttp test application with isolated DB and key paths."""
        self.test_dir = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.test_dir.name, "shared_test_chat.db")
        self.keys_dir = Path(self.test_dir.name) / "keys"

        # Configure environment for this test instance
        server.DB_PATH = self.db_path
        server.BACKEND_ID = "backend-test-instance"
        server.key_manager = server.KeyManager(keys_dir=self.keys_dir, db_path=self.db_path)

        # Initialize SQLite database
        init_db(self.db_path)

        return server.create_app()

    async def tearDownAsync(self):
        await super().tearDownAsync()
        if hasattr(self, "test_dir"):
            self.test_dir.cleanup()

    @unittest_run_loop
    async def test_1_post_message_succeeds(self):
        """Requirement 1 & 4: POST /message succeeds and returns 201 with unique message ID."""
        payload = {
            "client-name": "alice",
            "msg": "Hello from assessment test 1",
        }
        resp = await self.client.post("/message", json=payload)
        self.assertEqual(resp.status, 201)
        data = await resp.json()
        self.assertEqual(data["status"], "success")
        self.assertTrue(data["created"])
        self.assertIn("message_id", data)
        self.assertEqual(data["client-name"], "alice")
        self.assertEqual(data["backend_id"], "backend-test-instance")

    @unittest_run_loop
    async def test_2_get_feed_succeeds_and_retrieves_messages(self):
        """Requirement 2 & 8: GET /feed retrieves decrypted, verified messages from shared database."""
        # 1. Post two messages
        await self.client.post("/message", json={"client-name": "alice", "msg": "First message"})
        await self.client.post("/message", json={"client-name": "bob", "msg": "Second message"})

        # 2. Retrieve feed
        resp = await self.client.get("/feed")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["status"], "success")
        self.assertEqual(data["count"], 2)
        self.assertEqual(len(data["messages"]), 2)

        # Verify messages content and structure
        msgs = data["messages"]
        self.assertEqual(msgs[0]["client-name"], "alice")
        self.assertEqual(msgs[0]["msg"], "First message")
        self.assertTrue(msgs[0]["verified"])

        self.assertEqual(msgs[1]["client-name"], "bob")
        self.assertEqual(msgs[1]["msg"], "Second message")
        self.assertTrue(msgs[1]["verified"])

    @unittest_run_loop
    async def test_3_message_persistence_and_encryption(self):
        """Requirement 3, 6 & 7: Messages are persisted in SQLite, ciphertext is stored (not plaintext)."""
        secret = "super-secret-text-12345"
        resp = await self.client.post("/message", json={"client-name": "charlie", "msg": secret})
        self.assertEqual(resp.status, 201)
        data = await resp.json()
        msg_id = data["message_id"]

        # Directly inspect SQLite file
        records = load_history_raw(room_id=server.ROOM_ID, limit=5, db_path=self.db_path)
        self.assertEqual(len(records), 1)
        record = records[0]

        # Plaintext must NOT appear in raw database bytes
        self.assertNotIn(secret.encode("utf-8"), record["ciphertext"])
        self.assertNotEqual(secret.encode("utf-8"), record["ciphertext"])
        self.assertEqual(record["message_id"], msg_id)

    @unittest_run_loop
    async def test_4_message_unique_id_assigned(self):
        """Requirement 4 & 5: When client does not supply ID, server assigns a unique UUID."""
        resp1 = await self.client.post("/message", json={"client-name": "user1", "msg": "msg 1"})
        resp2 = await self.client.post("/message", json={"client-name": "user2", "msg": "msg 2"})
        id1 = (await resp1.json())["message_id"]
        id2 = (await resp2.json())["message_id"]
        self.assertNotEqual(id1, id2)
        self.assertTrue(len(id1) >= 16)

    @unittest_run_loop
    async def test_5_duplicate_message_id_idempotent(self):
        """Requirement 5 & 18: Repeated submission with same message ID does not create duplicate records."""
        custom_id = "IDEMPOTENT-MSG-001"
        payload = {
            "client-name": "dave",
            "msg": "Idempotent test message",
            "message_id": custom_id,
        }

        # Request 1: initial insert
        resp1 = await self.client.post("/message", json=payload)
        self.assertEqual(resp1.status, 201)
        data1 = await resp1.json()
        self.assertEqual(data1["message_id"], custom_id)
        self.assertTrue(data1["created"])

        # Request 2: duplicate retry with same message ID
        resp2 = await self.client.post("/message", json=payload)
        self.assertEqual(resp2.status, 200)
        data2 = await resp2.json()
        self.assertEqual(data2["message_id"], custom_id)
        self.assertFalse(data2["created"])  # Not newly created, safely ignored

        # Verify database has exactly ONE record
        records = load_history_raw(room_id=server.ROOM_ID, limit=10, db_path=self.db_path)
        matching = [r for r in records if r["message_id"] == custom_id]
        self.assertEqual(len(matching), 1)

    @unittest_run_loop
    async def test_6_concurrent_duplicate_requests(self):
        """Requirement 5, 18 & 25: Concurrent duplicate requests result in exactly ONE database record."""
        concurrent_id = "CONCURRENT-ID-777"
        payload = {
            "client-name": "eve",
            "msg": "Concurrent duplicate test",
            "message_id": concurrent_id,
        }

        # Launch 10 concurrent requests with the identical message ID
        async def send_req():
            return await self.client.post("/message", json=payload)

        responses = await asyncio.gather(*(send_req() for _ in range(10)))
        for r in responses:
            self.assertIn(r.status, (200, 201))

        # Exactly 1 request must have created=True, all others created=False
        created_count = 0
        for r in responses:
            d = await r.json()
            if d.get("created"):
                created_count += 1
        self.assertEqual(created_count, 1)

        # Database must have exactly ONE row
        con = get_db_connection(self.db_path)
        cur = con.cursor()
        cur.execute("SELECT COUNT(*) FROM messages WHERE message_id = ?", (concurrent_id,))
        count = cur.fetchone()[0]
        con.close()
        self.assertEqual(count, 1)

    @unittest_run_loop
    async def test_7_health_healthy_when_db_available(self):
        """Requirement 9: GET /health returns 200 healthy when DB is available."""
        resp = await self.client.get("/health")
        self.assertEqual(resp.status, 200)
        data = await resp.json()
        self.assertEqual(data["status"], "healthy")
        self.assertEqual(data["database"], "connected")
        self.assertEqual(data["backend_id"], "backend-test-instance")

    @unittest_run_loop
    async def test_8_health_unhealthy_when_db_unavailable(self):
        """Requirement 9: GET /health returns 503 unhealthy when DB is unavailable."""
        unhealthy_app = server.create_app(
            backend_id="backend-unhealthy",
            db_path="/non_existent_dir_12345/unreachable.db",
        )
        client = TestClient(TestServer(unhealthy_app))
        await client.start_server()
        try:
            resp = await client.get("/health")
            self.assertEqual(resp.status, 503)
            data = await resp.json()
            self.assertEqual(data["status"], "unhealthy")
            self.assertEqual(data["database"], "disconnected")
            self.assertEqual(data["backend_id"], "backend-unhealthy")
        finally:
            await client.close()



    @unittest_run_loop
    async def test_9_metrics_endpoint(self):
        """Requirement 10 & 31: GET /metrics returns valid CPU, memory, active requests, latency, uptime."""
        # Process a request to generate metrics
        await self.client.post("/message", json={"client-name": "frank", "msg": "Metrics test"})

        resp = await self.client.get("/metrics")
        self.assertEqual(resp.status, 200)
        data = await resp.json()

        self.assertEqual(data["backend_id"], "backend-test-instance")
        self.assertTrue(data["healthy"])
        self.assertIsInstance(data["cpu_percent"], (int, float))
        self.assertIsInstance(data["memory_percent"], (int, float))
        self.assertIsInstance(data["active_requests"], int)
        self.assertIsInstance(data["avg_latency_ms"], (int, float))
        self.assertGreaterEqual(data["request_count"], 1)
        self.assertGreaterEqual(data["uptime_seconds"], 0)

    @unittest_run_loop
    async def test_10_active_requests_counter_exception_safety(self):
        """Requirement 11 & 13: Active requests counter decrements even when requests fail/raise exceptions."""
        initial_active = metrics_tracker.active_requests

        # Send invalid JSON to trigger 400 Bad Request
        resp = await self.client.post("/message", data="NOT_A_JSON", headers={"Content-Type": "application/json"})
        self.assertEqual(resp.status, 400)

        # Active requests must return back to initial value
        self.assertEqual(metrics_tracker.active_requests, initial_active)

    @unittest_run_loop
    async def test_11_persistence_survives_restart(self):
        """Requirement 6 & 8: Data persists and is retrievable after simulated backend restart."""
        # Write message to backend instance
        await self.client.post("/message", json={"client-name": "grace", "msg": "Restart persistence message"})

        # Simulate restart: create a new aiohttp app pointing to the exact same SQLite database file
        new_app = server.create_app()
        client = TestClient(TestServer(new_app))
        await client.start_server()
        try:
            feed_resp = await client.get("/feed")
            self.assertEqual(feed_resp.status, 200)
            feed_data = await feed_resp.json()

            matching = [m for m in feed_data["messages"] if m["client-name"] == "grace"]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0]["msg"], "Restart persistence message")
            self.assertTrue(matching[0]["verified"])
        finally:
            await client.close()


class TestCrossBackendPersistence(AioHTTPTestCase):
    """
    Requirement 24: Cross-Backend Persistence Verification
    Simulate Systems 2, 3, and 4 running independent backend instances sharing the SAME database.
    Step 1: Send POST /message to Backend 2.
    Step 2: Confirm message is stored.
    Step 3: Send GET /feed directly to Backend 3. Expected: message appears.
    Step 4: Send GET /feed directly to Backend 4. Expected: same message appears.
    """

    async def get_application(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.shared_db = os.path.join(self.test_dir.name, "multi_backend_shared.db")
        self.keys_dir = Path(self.test_dir.name) / "keys"

        # Initialize shared database schema
        init_db(self.shared_db)

        # Primary app (Backend 2)
        server.DB_PATH = self.shared_db
        server.BACKEND_ID = "backend-2"
        server.key_manager = server.KeyManager(keys_dir=self.keys_dir, db_path=self.shared_db)
        return server.create_app(backend_id="backend-2", db_path=self.shared_db)

    async def tearDownAsync(self):
        await super().tearDownAsync()
        if hasattr(self, "test_dir"):
            self.test_dir.cleanup()

    @unittest_run_loop
    async def test_cross_backend_read_write(self):
        backend_2 = self.client

        # Create Backend 3 pointing to the shared DB
        app_3 = server.create_app(backend_id="backend-3", db_path=self.shared_db)
        backend_3 = TestClient(TestServer(app_3))
        await backend_3.start_server()

        # Create Backend 4 pointing to the shared DB
        app_4 = server.create_app(backend_id="backend-4", db_path=self.shared_db)
        backend_4 = TestClient(TestServer(app_4))
        await backend_4.start_server()

        try:
            # Step 1: Send POST /message to Backend 2
            post_resp = await backend_2.post("/message", json={
                "client-name": "heidi",
                "msg": "Cross-backend shared persistence verified!",
                "message_id": "CROSS-BACKEND-100",
            })
            self.assertEqual(post_resp.status, 201)
            post_data = await post_resp.json()
            self.assertEqual(post_data["backend_id"], "backend-2")
            self.assertEqual(post_data["message_id"], "CROSS-BACKEND-100")

            # Step 2: Query Backend 3 GET /feed directly
            feed_3_resp = await backend_3.get("/feed")
            self.assertEqual(feed_3_resp.status, 200)
            feed_3_data = await feed_3_resp.json()
            self.assertEqual(feed_3_data["backend_id"], "backend-3")

            msgs_3 = [m for m in feed_3_data["messages"] if m["message_id"] == "CROSS-BACKEND-100"]
            self.assertEqual(len(msgs_3), 1)
            self.assertEqual(msgs_3[0]["msg"], "Cross-backend shared persistence verified!")
            self.assertTrue(msgs_3[0]["verified"])

            # Step 3: Query Backend 4 GET /feed directly
            feed_4_resp = await backend_4.get("/feed")
            self.assertEqual(feed_4_resp.status, 200)
            feed_4_data = await feed_4_resp.json()
            self.assertEqual(feed_4_data["backend_id"], "backend-4")

            msgs_4 = [m for m in feed_4_data["messages"] if m["message_id"] == "CROSS-BACKEND-100"]
            self.assertEqual(len(msgs_4), 1)
            self.assertEqual(msgs_4[0]["msg"], "Cross-backend shared persistence verified!")
            self.assertTrue(msgs_4[0]["verified"])

            # Step 4: Cross-backend duplicate submission idempotency test (Requirement 25)
            # Submit duplicate message_id to Backend 3
            dup_3 = await backend_3.post("/message", json={
                "client-name": "heidi",
                "msg": "Cross-backend duplicate retry",
                "message_id": "CROSS-BACKEND-100",
            })
            self.assertEqual(dup_3.status, 200)
            dup_3_data = await dup_3.json()
            self.assertFalse(dup_3_data["created"])

            # Submit duplicate message_id to Backend 4
            dup_4 = await backend_4.post("/message", json={
                "client-name": "heidi",
                "msg": "Cross-backend duplicate retry",
                "message_id": "CROSS-BACKEND-100",
            })
            self.assertEqual(dup_4.status, 200)
            dup_4_data = await dup_4.json()
            self.assertFalse(dup_4_data["created"])

            # Shared DB must contain only 1 record
            con = get_db_connection(self.shared_db)
            cur = con.cursor()
            cur.execute("SELECT COUNT(*) FROM messages WHERE message_id = 'CROSS-BACKEND-100'")
            self.assertEqual(cur.fetchone()[0], 1)
            con.close()
        finally:
            await backend_3.close()
            await backend_4.close()


if __name__ == "__main__":
    unittest.main()
