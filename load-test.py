import asyncio
import json
import time
from collections import Counter

import websockets


# ============================================================
# CONFIGURATION
# ============================================================

# Sys1 Go Load Balancer
LOAD_BALANCER = "ws://10.1.75.51:3213"

# Number of simultaneous WebSocket connections
NUM_CLIENTS = 500

# Messages sent by each client
MESSAGES_PER_CLIENT = 1

# Maximum time to wait for a response (seconds)
# Increased to 15s to allow backend broadcast queues & SQLite writes to process under load
TIMEOUT = 20

# Ramp-up delay between client connections (seconds) to prevent connection storms & join timeouts
RAMP_UP_DELAY = 0.05 # 10ms delay per client connection start

# WebSocket handshake open timeout (seconds)
OPEN_TIMEOUT = 20


# ============================================================
# METRICS
# ============================================================

successful_requests = 0
failed_requests = 0

latencies = []

backend_distribution = Counter()

lock = asyncio.Lock()


# ============================================================
# PERCENTILE
# ============================================================

def percentile(values, p):
    if not values:
        return 0.0

    values = sorted(values)

    index = int(
        p * (len(values) - 1)
    )

    return values[index]


# ============================================================
# ONE CLIENT
# ============================================================

async def run_client(client_id):
    global successful_requests
    global failed_requests

    username = f"user_{client_id}"

    # Stagger connection initiation to avoid thundering herd / join timeouts
    if RAMP_UP_DELAY > 0:
        await asyncio.sleep(client_id * RAMP_UP_DELAY)

    try:
        # ====================================================
        # CONNECT TO LOAD BALANCER
        # ====================================================
        async with websockets.connect(
            LOAD_BALANCER,
            ping_interval=None,
            max_size=None,
            open_timeout=OPEN_TIMEOUT,
        ) as websocket:

            # Extract backend server ID from HTTP 101 response header sent by load-balcer.go
            # Check both legacy and modern Python websockets attribute paths case-insensitively
            headers = {}
            if hasattr(websocket, "response_headers") and websocket.response_headers:
                headers = websocket.response_headers
            elif hasattr(websocket, "response") and hasattr(websocket.response, "headers") and websocket.response.headers:
                headers = websocket.response.headers

            backend_server = "unknown"
            if headers:
                for k, v in headers.items():
                    if k.lower() == "x-backend-server":
                        backend_server = v
                        break

            # =================================================
            # JOIN
            # =================================================
            await websocket.send(
                json.dumps({
                    "type": "join",
                    "username": username
                })
            )

            # =================================================
            # RECEIVE JOIN RESPONSE
            # =================================================
            try:
                raw = await asyncio.wait_for(
                    websocket.recv(),
                    timeout=TIMEOUT
                )
            except asyncio.TimeoutError:
                print(f"[CLIENT {client_id}] JOIN TIMEOUT")
                async with lock:
                    failed_requests += MESSAGES_PER_CLIENT
                return

            # Parse response
            try:
                response = json.loads(raw)
            except (json.JSONDecodeError, TypeError):
                response = {}

            # If backend_server wasn't found in header, attempt JSON response fallback
            if backend_server == "unknown":
                backend_server = (
                    response.get("server")
                    or response.get("backend")
                    or response.get("server_id")
                    or response.get("node")
                    or response.get("from")
                    or response.get("port")
                    or "unknown"
                )

            async with lock:
                backend_distribution[backend_server] += 1

            # =================================================
            # SEND MESSAGES
            # =================================================
            for message_number in range(MESSAGES_PER_CLIENT):
                message_text = f"client-{client_id}-message-{message_number}"
                message = {
                    "type": "message",
                    "message": message_text
                }

                # Start latency timer
                start = time.perf_counter()

                # Send message
                try:
                    await websocket.send(json.dumps(message))
                except Exception as e:
                    print(f"[CLIENT {client_id}] SEND ERROR: {e}")
                    remaining_msgs = MESSAGES_PER_CLIENT - message_number
                    async with lock:
                        failed_requests += remaining_msgs
                    break

                # Wait for our own message
                received_our_message = False
                deadline = time.perf_counter() + TIMEOUT
                connection_lost = False

                while time.perf_counter() < deadline:
                    remaining = deadline - time.perf_counter()
                    if remaining <= 0:
                        break

                    try:
                        raw_response = await asyncio.wait_for(
                            websocket.recv(),
                            timeout=remaining
                        )
                    except asyncio.TimeoutError:
                        break
                    except Exception as e:
                        print(f"[CLIENT {client_id}] RECEIVE ERROR: {e}")
                        connection_lost = True
                        break

                    try:
                        response = json.loads(raw_response)
                    except (json.JSONDecodeError, TypeError):
                        continue

                    response_type = response.get("type")
                    if response_type != "message":
                        continue

                    received_message = response.get("message")
                    if received_message == message_text:
                        latency = time.perf_counter() - start
                        async with lock:
                            successful_requests += 1
                            latencies.append(latency)
                        received_our_message = True
                        break

                if not received_our_message:
                    async with lock:
                        failed_requests += 1

                if connection_lost:
                    remaining_msgs = MESSAGES_PER_CLIENT - (message_number + 1)
                    if remaining_msgs > 0:
                        async with lock:
                            failed_requests += remaining_msgs
                    break

    except Exception as e:
        print(f"[CLIENT {client_id}] CONNECTION ERROR: {type(e).__name__}: {e}")
        async with lock:
            failed_requests += MESSAGES_PER_CLIENT


# ============================================================
# LOAD TEST
# ============================================================

async def main():
    global successful_requests
    global failed_requests

    total_requests = NUM_CLIENTS * MESSAGES_PER_CLIENT

    # ========================================================
    # HEADER
    # ========================================================
    print()
    print("=" * 60)
    print("             WEBSOCKET LOAD TEST")
    print("=" * 60)
    print(f"Load Balancer       : {LOAD_BALANCER}")
    print(f"Concurrent Clients  : {NUM_CLIENTS}")
    print(f"Messages / Client   : {MESSAGES_PER_CLIENT}")
    print(f"Total Messages      : {total_requests}")
    if RAMP_UP_DELAY > 0:
        print(f"Ramp-up Delay       : {RAMP_UP_DELAY * 1000:.1f} ms / client")
    print(f"Response Timeout    : {TIMEOUT}s")
    print()

    # ========================================================
    # START TEST
    # ========================================================
    start_time = time.perf_counter()

    tasks = []
    for client_id in range(NUM_CLIENTS):
        tasks.append(
            asyncio.create_task(
                run_client(client_id)
            )
        )

    # ========================================================
    # WAIT FOR ALL CLIENTS
    # ========================================================
    await asyncio.gather(*tasks)

    # ========================================================
    # END TEST
    # ========================================================
    total_time = time.perf_counter() - start_time

    # ========================================================
    # COPY METRICS
    # ========================================================
    async with lock:
        success = successful_requests
        failed = failed_requests
        latency_values = list(latencies)
        distribution = Counter(backend_distribution)

    # ========================================================
    # THROUGHPUT & DROPOUT
    # ========================================================
    throughput = (success / total_time) if total_time > 0 else 0.0
    dropout = (failed / total_requests * 100) if total_requests > 0 else 0.0

    # ========================================================
    # PERCENTILES
    # ========================================================
    p50 = percentile(latency_values, 0.50)
    p95 = percentile(latency_values, 0.95)
    p99 = percentile(latency_values, 0.99)

    # ========================================================
    # RESULTS
    # ========================================================
    print()
    print("=" * 60)
    print("                    RESULTS")
    print("=" * 60)
    print(f"Total Requests : {total_requests}")
    print(f"Successful     : {success}")
    print(f"Failed         : {failed}")
    print(f"Dropout        : {dropout:.2f}%")
    print(f"Throughput     : {throughput:.2f} req/sec")
    print(f"P50            : {p50 * 1000:.2f} ms")
    print(f"P95            : {p95 * 1000:.2f} ms")
    print(f"P99            : {p99 * 1000:.2f} ms")
    print(f"Test Duration  : {total_time:.2f} sec")

    # ========================================================
    # BACKEND DISTRIBUTION
    # ========================================================
    print()
    print("=" * 60)
    print("             BACKEND DISTRIBUTION")
    print("=" * 60)
    for backend, count in distribution.items():
        print(f"{backend:15s}: {count}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())