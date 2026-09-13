import asyncio
import json
import os
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path
import aiohttp

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database import init_db
import server


class TestGoLoadBalancer(unittest.IsolatedAsyncioTestCase):
    """
    Spins up 3 live backend server nodes and tests the compiled Go Performance Load Balancer:
    - Route verification: POST /message and GET /feed
    - Response headers: X-Backend-Server, X-Backend-Id, X-Load-Balancer
    - Idempotency & Persistence across nodes
    - Health checking and dynamic performance-based routing
    """

    @classmethod
    def setUpClass(cls):
        # Locate or build Go load balancer binary
        lb_dir = Path(__file__).resolve().parent.parent / "load-balancer"
        cls.lb_binary = lb_dir / "load-balancer"
        if not cls.lb_binary.exists():
            build_script = lb_dir / "build.sh"
            subprocess.run(["bash", str(build_script)], check=True, cwd=str(lb_dir))
        assert cls.lb_binary.exists(), f"Go load balancer binary not found at {cls.lb_binary}"

    async def asyncSetUp(self):
        self.test_dir = tempfile.TemporaryDirectory()
        self.shared_db = os.path.join(self.test_dir.name, "lb_test_shared.db")
        self.keys_dir = Path(self.test_dir.name) / "keys"

        # Initialize shared database
        init_db(self.shared_db)
        server.key_manager = server.KeyManager(keys_dir=self.keys_dir, db_path=self.shared_db)

        # Start 3 backend instances on ports 8201, 8202, 8203
        self.runners = []
        self.sites = []
        self.ports = [8201, 8202, 8203]
        self.backend_ids = ["backend-node-1", "backend-node-2", "backend-node-3"]

        for b_id, port in zip(self.backend_ids, self.ports):
            app = server.create_app(backend_id=b_id, db_path=self.shared_db)
            runner = aiohttp.web.AppRunner(app, access_log=None)
            await runner.setup()
            site = aiohttp.web.TCPSite(runner, "127.0.0.1", port)
            await site.start()
            self.runners.append(runner)
            self.sites.append(site)

        # Launch Go Load Balancer on port 8200
        self.lb_port = 8200
        backends_str = ",".join(f"127.0.0.1:{p}" for p in self.ports)
        self.lb_process = subprocess.Popen(
            [
                str(self.lb_binary),
                "-listen", f":{self.lb_port}",
                "-backends", backends_str,
                "-interval", "200ms",
                "-cpu-threshold", "70.0",
                "-conn-threshold", "10",
                "-latency-threshold", "100.0"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        self.session = aiohttp.ClientSession()

        # Wait for LB to become responsive
        lb_url = f"http://127.0.0.1:{self.lb_port}/health"
        for _ in range(50):
            try:
                async with self.session.get(lb_url, timeout=aiohttp.ClientTimeout(total=0.5)) as resp:
                    if resp.status == 200:
                        break
            except Exception:
                await asyncio.sleep(0.1)
        else:
            self.fail("Go load balancer failed to become healthy within 5 seconds")

    async def asyncTearDown(self):
        await self.session.close()

        # Terminate Go LB process
        if self.lb_process:
            if self.lb_process.poll() is None:
                self.lb_process.terminate()
                try:
                    self.lb_process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    self.lb_process.kill()
            if self.lb_process.stdout:
                self.lb_process.stdout.close()
            if self.lb_process.stderr:
                self.lb_process.stderr.close()

        # Stop backend runners
        for runner in self.runners:
            await runner.cleanup()

        self.test_dir.cleanup()

    async def test_load_balancer_health_and_status(self):
        """Verify GET /health, GET /metrics, and GET /status on the Go Load Balancer"""
        base_url = f"http://127.0.0.1:{self.lb_port}"

        # 1. /health
        async with self.session.get(f"{base_url}/health") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["status"], "healthy")
            self.assertEqual(data["load_balancer"], "active")
            self.assertEqual(data["healthy_backends"], 3)
            self.assertEqual(data["total_backends"], 3)

        # 2. /status
        async with self.session.get(f"{base_url}/status") as resp:
            self.assertEqual(resp.status, 200)
            status = await resp.json()
            self.assertEqual(status["healthy_backends"], 3)
            self.assertIn("backends", status)
            self.assertEqual(len(status["backends"]), 3)
            self.assertIn("thresholds", status)

    async def test_load_balancer_message_and_feed_flow(self):
        """
        Verify exact routes:
        1. POST /message accepts client-name and msg
        2. Returns 201 Created and response headers (X-Backend-Server, X-Backend-Id)
        3. GET /feed retrieves the posted message
        """
        base_url = f"http://127.0.0.1:{self.lb_port}"

        # 1. POST /message (JSON format)
        payload = {
            "client-name": "alice_test",
            "msg": "Hello world from Go Load Balancer test!"
        }
        async with self.session.post(f"{base_url}/message", json=payload) as resp:
            self.assertEqual(resp.status, 201)
            self.assertIn("X-Backend-Server", resp.headers)
            self.assertIn("X-Backend-Id", resp.headers)
            self.assertEqual(resp.headers.get("X-Load-Balancer"), "Go-Performance-v2")

            data = await resp.json()
            self.assertEqual(data["status"], "success")
            self.assertIn("message_id", data)
            msg_id = data["message_id"]

        # 2. GET /feed
        async with self.session.get(f"{base_url}/feed") as resp:
            self.assertEqual(resp.status, 200)
            feed_data = await resp.json()
            self.assertEqual(feed_data["status"], "success")
            self.assertIn("messages", feed_data)

            # Confirm message is in feed
            msg_ids = [m["message_id"] for m in feed_data["messages"]]
            self.assertIn(msg_id, msg_ids)

            saved_msg = next(m for m in feed_data["messages"] if m["message_id"] == msg_id)
            self.assertEqual(saved_msg["user"], "alice_test")
            self.assertEqual(saved_msg["text"], "Hello world from Go Load Balancer test!")

    async def test_form_encoded_message(self):
        """Verify POST /message accepts standard application/x-www-form-urlencoded data"""
        base_url = f"http://127.0.0.1:{self.lb_port}"

        form_data = {
            "client-name": "bob_form_user",
            "msg": "Form-encoded chat message"
        }
        async with self.session.post(f"{base_url}/message", data=form_data) as resp:
            self.assertEqual(resp.status, 201)
            data = await resp.json()
            self.assertEqual(data["status"], "success")
            self.assertIn("message_id", data)

        async with self.session.get(f"{base_url}/feed") as resp:
            feed = await resp.json()
            texts = [m["text"] for m in feed["messages"]]
            self.assertIn("Form-encoded chat message", texts)

    async def test_idempotency_and_no_duplicates(self):
        """Verify that retrying a message with the same message_id never creates duplicates"""
        base_url = f"http://127.0.0.1:{self.lb_port}"

        fixed_id = "test-idempotent-uuid-999"
        payload = {
            "client-name": "charlie_dedup",
            "msg": "Important transaction message",
            "message_id": fixed_id
        }

        # First delivery
        async with self.session.post(f"{base_url}/message", json=payload) as resp1:
            self.assertEqual(resp1.status, 201)
            data1 = await resp1.json()
            self.assertEqual(data1["status"], "success")

        # Second delivery with identical message_id (retry / retransmission)
        async with self.session.post(f"{base_url}/message", json=payload) as resp2:
            self.assertEqual(resp2.status, 200)
            data2 = await resp2.json()
            self.assertTrue(data2.get("duplicate") or not data2.get("created"))

        # Third delivery
        async with self.session.post(f"{base_url}/message", json=payload) as resp3:
            self.assertEqual(resp3.status, 200)

        # Check feed: message must appear EXACTLY ONCE
        async with self.session.get(f"{base_url}/feed") as resp:
            feed = await resp.json()
            matches = [m for m in feed["messages"] if m.get("message_id") == fixed_id]
            self.assertEqual(len(matches), 1, "Duplicate messages found in database feed!")

    async def test_failover_when_backend_stops(self):
        """Verify that when the current backend goes down, LB fails over to remaining healthy backends"""
        base_url = f"http://127.0.0.1:{self.lb_port}"

        # 1. Stop backend on port 8201
        await self.runners[0].cleanup()

        # Allow LB health monitor to detect backend-1 is down
        await asyncio.sleep(0.5)

        # 2. Check LB status
        async with self.session.get(f"{base_url}/health") as resp:
            self.assertEqual(resp.status, 200)
            data = await resp.json()
            self.assertEqual(data["healthy_backends"], 2)

        # 3. Request should still succeed via remaining backends
        payload = {
            "client-name": "failover_tester",
            "msg": "Should be routed to surviving backend"
        }
        async with self.session.post(f"{base_url}/message", json=payload) as resp:
            self.assertEqual(resp.status, 201)
            data = await resp.json()
            self.assertEqual(data["status"], "success")
            handled_by = resp.headers.get("X-Backend-Server", "")
            # Must NOT be 8201
            self.assertNotIn("8201", handled_by)

    async def test_dynamic_performance_switching_on_threshold_breach(self):
        """
        Verify performance-based switching:
        When the active backend's CPU exceeds the threshold, the LB dynamically
        switches incoming requests to another suitable healthy backend below threshold.
        """
        node_a_cpu = 10.0
        node_b_cpu = 10.0

        async def health_handler(request):
            return aiohttp.web.json_response({"status": "healthy", "database": "connected"})

        async def metrics_a(request):
            return aiohttp.web.json_response({
                "cpu_percent": node_a_cpu,
                "active_requests": 0,
                "avg_latency_ms": 5.0,
                "backend_id": "mock-node-A"
            })

        async def metrics_b(request):
            return aiohttp.web.json_response({
                "cpu_percent": node_b_cpu,
                "active_requests": 0,
                "avg_latency_ms": 5.0,
                "backend_id": "mock-node-B"
            })

        async def msg_handler(request):
            return aiohttp.web.json_response({"status": "success", "message_id": "mock-1"}, status=201)

        # Setup Node A (8301) and Node B (8302)
        app_a = aiohttp.web.Application()
        app_a.router.add_get("/health", health_handler)
        app_a.router.add_get("/metrics", metrics_a)
        app_a.router.add_post("/message", msg_handler)

        app_b = aiohttp.web.Application()
        app_b.router.add_get("/health", health_handler)
        app_b.router.add_get("/metrics", metrics_b)
        app_b.router.add_post("/message", msg_handler)

        runner_a = aiohttp.web.AppRunner(app_a)
        await runner_a.setup()
        await aiohttp.web.TCPSite(runner_a, "127.0.0.1", 8301).start()

        runner_b = aiohttp.web.AppRunner(app_b)
        await runner_b.setup()
        await aiohttp.web.TCPSite(runner_b, "127.0.0.1", 8302).start()

        # Start dedicated LB on 8300 with CPU threshold 50.0% and fast polling 100ms
        lb_proc = subprocess.Popen(
            [
                str(self.lb_binary),
                "-listen", ":8300",
                "-backends", "127.0.0.1:8301,127.0.0.1:8302",
                "-interval", "100ms",
                "-cpu-threshold", "50.0",
                "-conn-threshold", "50",
                "-latency-threshold", "100.0"
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )

        try:
            # Wait for LB
            for _ in range(30):
                try:
                    async with self.session.get("http://127.0.0.1:8300/health", timeout=aiohttp.ClientTimeout(total=0.5)) as r:
                        if r.status == 200:
                            break
                except Exception:
                    await asyncio.sleep(0.05)

            # 1. Send first message -> routed to one of the nodes (e.g. 8301)
            async with self.session.post("http://127.0.0.1:8300/message", json={"client-name": "u", "msg": "m1"}) as r:
                self.assertEqual(r.status, 201)
                initial_backend = r.headers.get("X-Backend-Server")

            # Determine which node was initial
            other_backend = "127.0.0.1:8302" if "8301" in initial_backend else "127.0.0.1:8301"

            # 2. Simulate CPU overload on initial backend (spike to 88%, well above threshold 50%)
            if "8301" in initial_backend:
                node_a_cpu = 88.0
            else:
                node_b_cpu = 88.0

            # Allow LB to poll metrics (interval is 100ms)
            await asyncio.sleep(0.3)

            # 3. Next message must trigger dynamic switch to the healthier other backend!
            async with self.session.post("http://127.0.0.1:8300/message", json={"client-name": "u", "msg": "m2"}) as r:
                self.assertEqual(r.status, 201)
                switched_backend = r.headers.get("X-Backend-Server")
                self.assertIn(other_backend, switched_backend,
                              f"Traffic did not switch to {other_backend}! Still on {switched_backend}")

            # 4. Confirm switch count in /status
            async with self.session.get("http://127.0.0.1:8300/status") as r:
                status_data = await r.json()
                self.assertGreaterEqual(status_data["switch_count"], 1)

        finally:
            if lb_proc.poll() is None:
                lb_proc.terminate()
                try:
                    lb_proc.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    lb_proc.kill()
            if lb_proc.stdout:
                lb_proc.stdout.close()
            if lb_proc.stderr:
                lb_proc.stderr.close()
            await runner_a.cleanup()
            await runner_b.cleanup()

    async def test_load_generator_execution(self):
        """
        Verify that load_generator.py runs successfully against the Go Load Balancer,
        records metrics across all 4 systems, and outputs summary JSON and plots.
        """
        out_dir = Path(self.test_dir.name) / "bench_out"
        lg_script = Path(__file__).resolve().parent.parent / "load_generator.py"

        cmd = [
            sys.executable,
            str(lg_script),
            "--url", f"http://127.0.0.1:{self.lb_port}",
            "--users", "5",
            "--duration", "2",
            "--min-interval", "0.05",
            "--max-interval", "0.15",
            "--output-dir", str(out_dir),
            "--plot"
        ]

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        stdout, stderr = await proc.communicate()

        self.assertEqual(proc.returncode, 0, f"load_generator.py failed: {stderr.decode()}")

        # Verify summary JSON
        summary_file = out_dir / "benchmark_summary.json"
        self.assertTrue(summary_file.exists(), "benchmark_summary.json was not created")

        with open(summary_file, "r") as f:
            summary = json.load(f)

        self.assertGreater(summary["results"]["total_sent"], 0)
        self.assertGreater(summary["results"]["successful"], 0)
        self.assertEqual(summary["results"]["failed"], 0)
        self.assertIn("p50", summary["results"]["percentiles"])
        self.assertIn("p99", summary["results"]["percentiles"])

        # Verify plots generated
        self.assertTrue((out_dir / "response_time.png").exists(), "response_time.png plot not generated")
        self.assertTrue((out_dir / "system_utilization.png").exists(), "system_utilization.png plot not generated")
        self.assertTrue((out_dir / "backend_distribution.png").exists(), "backend_distribution.png plot not generated")



if __name__ == "__main__":
    unittest.main()
