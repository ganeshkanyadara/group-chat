import asyncio
import os
import sys
import tempfile
import unittest
from pathlib import Path
import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import init_db
import server


class TestLiveMultiBackend(unittest.IsolatedAsyncioTestCase):
    """
    Spins up 3 live HTTP servers on ephemeral/local ports to test real network requests,
    cross-backend persistence, duplicate protection, health, and metrics.
    """

    async def asyncSetUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.shared_db = os.path.join(self.test_dir.name, "live_shared.db")
        self.keys_dir = Path(self.test_dir.name) / "keys"

        # Initialize shared DB
        init_db(self.shared_db)
        server.key_manager = server.KeyManager(keys_dir=self.keys_dir, db_path=self.shared_db)

        # Start 3 backend instances on ports 8101, 8102, 8103
        self.runners = []
        self.sites = []
        self.ports = [8101, 8102, 8103]
        self.backend_ids = ["backend-2", "backend-3", "backend-4"]

        for b_id, port in zip(self.backend_ids, self.ports):
            app = server.create_app(backend_id=b_id, db_path=self.shared_db)
            runner = aiohttp.web.AppRunner(app, access_log=None)
            await runner.setup()
            site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            self.runners.append(runner)
            self.sites.append(site)

        self.session = aiohttp.ClientSession()

    async def asyncTearDown(self):
        await self.session.close()
        for runner in self.runners:
            await runner.cleanup()
        self.test_dir.cleanup()

    async def test_cross_backend_network_flow(self):
        """
        Full end-to-end assessment verification across 3 running network servers:
        1. Send POST /message to Backend 2 (port 8101).
        2. Query GET /feed on Backend 3 (port 8102) -> confirms message present.
        3. Query GET /feed on Backend 4 (port 8103) -> confirms same message present.
        4. Send duplicate POST /message with same ID to Backend 3 and Backend 4 -> idempotent.
        5. Query GET /health and GET /metrics on all 3 backends.
        """
        b2_url = f"http://127.0.0.1:{self.ports[0]}"
        b3_url = f"http://127.0.0.1:{self.ports[1]}"
        b4_url = f"http://127.0.0.1:{self.ports[2]}"

        # 1. POST /message to Backend 2
        post_payload = {
            "client-name": "alice_network",
            "msg": "Network cross-backend message",
            "message_id": "NET-MSG-12345",
        }
        async with self.session.post(f"{b2_url}/message", json=post_payload) as resp:
            self.assertEqual(resp.status, 201)
            data = await resp.json()
            self.assertEqual(data["status"], "success")
            self.assertEqual(data["message_id"], "NET-MSG-12345")
            self.assertEqual(data["backend_id"], "backend-2")
            self.assertTrue(data["created"])
            self.assertEqual(resp.headers.get("X-Backend-Id"), "backend-2")

        # 2. GET /feed on Backend 3
        async with self.session.get(f"{b3_url}/feed") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["backend_id"], "backend-3")
            self.assertEqual(resp.headers.get("X-Backend-Id"), "backend-3")
            matching = [m for m in data["messages"] if m["message_id"] == "NET-MSG-12345"]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0]["msg"], "Network cross-backend message")
            self.assertEqual(matching[0]["client-name"], "alice_network")
            self.assertTrue(matching[0]["verified"])

        # 3. GET /feed on Backend 4
        async with self.session.get(f"{b4_url}/feed") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["backend_id"], "backend-4")
            matching = [m for m in data["messages"] if m["message_id"] == "NET-MSG-12345"]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0]["msg"], "Network cross-backend message")
            self.assertTrue(matching[0]["verified"])

        # 4. Idempotency / Retry test across different backends
        # Retry to Backend 3
        async with self.session.post(f"{b3_url}/message", json=post_payload) as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["message_id"], "NET-MSG-12345")
            self.assertFalse(data["created"])

        # Retry to Backend 4
        async with self.session.post(f"{b4_url}/message", json=post_payload) as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["message_id"], "NET-MSG-12345")
            self.assertFalse(data["created"])

        # Feed must still only contain 1 message
        async with self.session.get(f"{b2_url}/feed") as resp:
            data = await resp.json()
            matching = [m for m in data["messages"] if m["message_id"] == "NET-MSG-12345"]
            self.assertEqual(len(matching), 1)

        # 5. Check /health on all backends
        for port, expected_id in zip(self.ports, self.backend_ids):
            async with self.session.get(f"http://127.0.0.1:{port}/health") as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertEqual(data["status"], "healthy")
                self.assertEqual(data["database"], "connected")
                self.assertEqual(data["backend_id"], expected_id)

        # 6. Check /metrics on all backends
        for port, expected_id in zip(self.ports, self.backend_ids):
            async with self.session.get(f"http://127.0.0.1:{port}/metrics") as resp:
                self.assertEqual(resp.status, 200)
                data = await resp.json()
                self.assertEqual(data["backend_id"], expected_id)
                self.assertTrue(data["healthy"])
                self.assertIn("cpu_percent", data)
                self.assertIn("memory_percent", data)
                self.assertIn("active_requests", data)
                self.assertIn("avg_latency_ms", data)
                self.assertIn("uptime_seconds", data)

    async def test_realtime_cross_backend_websocket_sync(self):
        """
        Verify real-time message sync across separate backend instances via WebSockets.
        Client A connects to Node 2 (8101).
        Client B connects to Node 3 (8102).
        Client A sends a message -> Client B must receive it in real time over WS.
        """
        import json

        ws2_url = f"http://127.0.0.1:{self.ports[0]}/"
        ws3_url = f"http://127.0.0.1:{self.ports[1]}/"

        async with self.session.ws_connect(ws2_url) as ws2, \
                   self.session.ws_connect(ws3_url) as ws3:

            async def read_until_type(ws, target_type, timeout=3.0):
                start = asyncio.get_event_loop().time()
                while asyncio.get_event_loop().time() - start < timeout:
                    msg = await asyncio.wait_for(ws.receive_json(), timeout=timeout)
                    if msg.get("type") == target_type:
                        return msg
                return None

            # Join Node 2
            await ws2.send_json({"type": "join", "username": "alice_node2"})
            auth2 = await read_until_type(ws2, "authenticated")
            self.assertIsNotNone(auth2)

            # Join Node 3
            await ws3.send_json({"type": "join", "username": "bob_node3"})
            auth3 = await read_until_type(ws3, "authenticated")
            self.assertIsNotNone(auth3)

            # Alice on Node 2 sends a message
            await ws2.send_json({
                "type": "message",
                "message": "Hello from Node 2 in real time!",
                "message_id": "WS-REALTIME-999",
            })

            # Alice receives her own broadcast
            local_echo = await read_until_type(ws2, "message")
            self.assertIsNotNone(local_echo)
            self.assertEqual(local_echo["message"], "Hello from Node 2 in real time!")

            # Bob on Node 3 MUST receive Alice's message in real time via the sync loop!
            bob_msg = await read_until_type(ws3, "message", timeout=3.0)
            self.assertIsNotNone(bob_msg, "Bob on Node 3 did not receive Alice's real-time message from Node 2")
            self.assertEqual(bob_msg["username"], "alice_node2")
            self.assertEqual(bob_msg["message"], "Hello from Node 2 in real time!")
            self.assertEqual(bob_msg["message_id"], local_echo["message_id"])
            self.assertTrue(bob_msg.get("verified"))

            # Now Alice on Node 2 leaves (closes WebSocket)
            await ws2.close()

            # Bob on Node 3 should receive system notification that alice_node2 left the chat!
            leave_event = None
            start = asyncio.get_event_loop().time()
            while asyncio.get_event_loop().time() - start < 3.0:
                try:
                    msg = await asyncio.wait_for(ws3.receive_json(), timeout=1.0)
                    if msg.get("type") == "system" and "alice_node2 left" in msg.get("message", ""):
                        leave_event = msg
                        break
                except asyncio.TimeoutError:
                    break

            self.assertIsNotNone(leave_event, "Bob on Node 3 did not receive real-time leave event for Alice")

    async def test_cross_backend_signature_verification_with_stale_cache(self):
        """
        Verify that even if a node has a stale in-memory keypair for a user,
        it uses the canonical public key from the database and correctly
        verifies messages sent from another node as verified=True (Green checkmark).
        """
        b2_url = f"http://127.0.0.1:{self.ports[0]}"
        b3_url = f"http://127.0.0.1:{self.ports[1]}"

        # Simulate stale in-memory key on Node 3
        from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
        server.key_manager._memory_private_keys["gopi"] = Ed25519PrivateKey.generate()

        # Now send a message as gopi on Node 2
        payload = {
            "client-name": "gopi",
            "msg": "Hello ganesh from node 2",
            "message_id": "GOPI-MSG-001",
        }
        async with self.session.post(f"{b2_url}/message", json=payload) as resp:
            self.assertEqual(resp.status, 201)

        # Now fetch feed from Node 3
        async with self.session.get(f"{b3_url}/feed") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            matching = [m for m in data["messages"] if m["message_id"] == "GOPI-MSG-001"]
            self.assertEqual(len(matching), 1)
            self.assertEqual(matching[0]["msg"], "Hello ganesh from node 2")
            # Must be verified=True despite Node 3's stale in-memory key!
            self.assertTrue(matching[0]["verified"])


if __name__ == "__main__":
    unittest.main()
