# Real-Time Group Chat Application (Persistent + Secure)

A persistent, secure, real-time **Group Chat Application** built using **Python WebSockets, SQLite3, AES-256-GCM encryption, Ed25519 digital signatures, HTML5, CSS3, and JavaScript**.

The application provides end-to-end real-time communication, encrypted persistence, sender digital signature verification, and tamper detection between the four authorized group members.

---

## Key Features

- **Real-time Communication**: Low-latency WebSocket broadcasting for live messaging.
- **SQLite Persistence**: Chat history is stored securely in an SQLite database (`chat.db`) and restored when a user connects.
- **AES-256-GCM Encryption**: Plaintext messages are encrypted before being written to disk. The database contains ONLY binary ciphertext and nonces.
- **Ed25519 Digital Signatures**: Messages are digitally signed using sender-specific private keys to ensure sender authenticity and non-repudiation.
- **Tamper-Aware Verification**: Digital signatures are verified against canonical message payloads upon reading history or receiving messages. Modified ciphertext or signatures trigger security alerts.
- **Authentication & Authorization**: Strict access control restricting connections to four pre-authorized group members (`ganesh`, `venu`, `venkat`, `dheemanth`).
- **Key Management**: Ed25519 private keys persist in a protected `keys/` directory (git-ignored) while public keys are stored in SQLite for signature verification.
- **Evaluation & Inspection Tools**: Dedicated scripts to inspect SQLite binary contents (`scripts/inspect_db.py`) and simulate tampering (`scripts/tamper_demo.py`).

---

## Security Architecture & Design

### Cryptographic Distinction

| Technology | Security Function | Description |
|---|---|---|
| **AES-256-GCM** | **Confidentiality** | Protects message contents from unauthorized inspection. Plaintext is encrypted with a unique 96-bit (12-byte) random nonce per message before SQLite storage. |
| **Ed25519 Signature** | **Sender Authenticity & Tamper Detection** | Proves sender identity using asymmetric cryptography. Binds message content, timestamp, room ID, and sender ID into a canonical payload to detect any modification. |

```text
                 ┌───────────────┐
                 │    Client     │
                 └───────┬───────┘
                         │
                     WebSocket
                         │
                         ▼
                 ┌───────────────┐
                 │ WebSocket     │
                 │ Server        │
                 └───────┬───────┘
                         │
             ┌───────────┴───────────┐
             │                       │
             ▼                       ▼
       Authentication          Crypto Layer
                                     │
                              ┌──────┴──────┐
                              │             │
                           AES-GCM      Signature
                              │             │
                              └──────┬──────┘
                                     │
                                     ▼
                              ┌─────────────┐
                              │   SQLite    │
                              │  Database   │
                              └─────────────┘
```

---

## Database Schema (`chat.db`)

The database table `messages` stores **ONLY** binary encrypted data:

```sql
CREATE TABLE IF NOT EXISTS messages (
    message_id  INTEGER PRIMARY KEY AUTOINCREMENT,
    room_id     TEXT    NOT NULL,
    sender_id   TEXT    NOT NULL,
    ciphertext  BLOB    NOT NULL,
    nonce       BLOB    NOT NULL,
    signature   BLOB    NOT NULL,
    timestamp   TEXT    NOT NULL
);
```

Public keys for signature verification are stored in `user_keys`:

```sql
CREATE TABLE IF NOT EXISTS user_keys (
    username    TEXT PRIMARY KEY,
    public_key  BLOB NOT NULL
);
```

> [!IMPORTANT]
> **Plaintext is NEVER written to the database.** Inspecting `chat.db` directly shows only encrypted ciphertext blobs, random nonces, and binary signature signatures.

---

## Data Flow Diagrams

### 1. Sending a New Message

```text
User types message
        ↓
WebSocket transmission
        ↓
Authenticate sender
        ↓
Validate message & create UTC timestamp
        ↓
Construct canonical message payload (room_id + sender_id + message + timestamp)
        ↓
Sign canonical payload with Sender's Ed25519 Private Key
        ↓
Encrypt plaintext with AES-256-GCM (fresh 12-byte random nonce)
        ↓
Store (ciphertext + nonce + signature) in SQLite `messages` table
        ↓
Broadcast decrypted representation + verification status to active WebSockets
```

### 2. Receiving Chat History (User Connection)

```text
User connects & Authenticates
        ↓
Query SQLite `messages` table for room history
        ↓
For every message record:
        ├─ Decrypt ciphertext using AES-256-GCM
        ├─ Reconstruct canonical payload (room_id + sender_id + plaintext + timestamp)
        ├─ Retrieve sender's public key from `user_keys`
        └─ Verify Ed25519 signature
        ↓
Send history frame to connected client (with verified status ✓ / ✗)
        ↓
WebSocket connection remains open for live messaging
```

---

## Project Structure

```text
group-chat/
│
├── server.py              # Main asynchronous WebSocket server
├── README.md              # Complete project documentation
├── requirements.txt        # Python dependency requirements
├── .env.example           # Environment template
├── .gitignore             # Excludes secret keys, .env, and chat.db
├── chat.db                # SQLite database (generated at runtime)
│
├── crypto/                # Cryptographic subpackage
│   ├── __init__.py
│   ├── encryption.py      # AES-256-GCM encryption and decryption
│   ├── signing.py         # Ed25519 digital signatures & canonical payload
│   └── key_manager.py     # Per-user Ed25519 keypair management
│
├── database/              # Persistence subpackage
│   ├── __init__.py
│   └── database.py        # SQLite connection, table schemas & CRUD
│
├── scripts/               # Management & evaluation scripts
│   ├── generate_keys.py   # Pre-generates keys and .env file
│   ├── inspect_db.py      # Dumps database records in raw binary/hex/b64 format
│   └── tamper_demo.py     # Simulates message tampering in SQLite
│
├── tests/                 # Automated test suite
│   └── test_chat.py       # 8 unit and integration security tests
│
└── client/                # Frontend Web Application
    ├── index.html         # User Interface layout
    ├── style.css          # Modern dark-themed styling
    └── app.js             # Client WebSocket controller & badge renderer
```

---

## Installation & Setup

### 1. Prerequisites

Ensure Python 3.10+ is installed:

```bash
python3 --version
```

### 2. Install Dependencies

Install required Python packages:

```bash
pip install -r requirements.txt
```

*(Or activate your virtualenv, e.g., `source ~/Desktop/torch-env/bin/activate`)*

### 3. Key Generation & Setup

Run the key generator script to create symmetric encryption keys and Ed25519 user signing keypairs:

```bash
python3 scripts/generate_keys.py
```

This creates `.env` with `CHAT_ENCRYPTION_KEY` and populates the `keys/` directory.

---

## Running the Application

### Step 1: Start the Backend WebSocket Server

In Terminal 1:

```bash
python3 server.py
```

The WebSocket server listens on `ws://0.0.0.0:4000`.

### Step 2: Start the Frontend HTTP Server

In Terminal 2:

```bash
cd client
python3 -m http.server 5000 --bind 0.0.0.0
```

### Step 3: Access the Application

Open a browser and navigate to:
`http://localhost:5000` (or `http://<SERVER_IP>:5000`)

---

## Authorized Credentials

Only the following four authorized users can log in:

| No. | Username | Access Code |
|---|---|---|
| 1 | `ganesh` | `12341080` |
| 2 | `venu` | `12341110` |
| 3 | `venkat` | `12341070` |
| 4 | `dheemanth` | `12341710` |

---

## Evaluation & Demonstration Guides

### 1. Database Plaintext Inspection

To prove that raw plaintext is NOT stored in SQLite, run:

```bash
python3 scripts/inspect_db.py
```

**Output Demonstration:**
Displays records showing message_id, room_id, sender_id, base64-encoded binary ciphertext, nonce, and signature. Plaintext is non-existent in the database.

### 2. Tampering & Signature Verification Demonstration

To demonstrate that digital signatures detect database tampering:

1. Send a message in the chat (e.g. *"Meeting at 5 PM"*).
2. Run the tamper demonstration script:
   ```bash
   python3 scripts/tamper_demo.py
   ```
3. Select option `2` to corrupt the signature (or option `1` to corrupt ciphertext).
4. Refresh or reconnect the client.
5. Notice that the corrupted message is flagged as unverified (**✗**) or fails decryption, proving that signature verification catches unauthorized modifications.

### 3. Automated Test Suite

Run all 8 security and persistence tests:

```bash
python3 -m unittest discover -s tests
```

**Test Coverage:**
- `test_1_normal_message_flow`: Message delivery, storage, decryption, signature validation.
- `test_2_persistence`: DB survival and chat history restoration upon reconnect.
- `test_3_encryption_no_plaintext_stored`: Verification that raw plaintext is absent from SQLite.
- `test_4_decryption`: AES-256-GCM ciphertext decryption accuracy.
- `test_5_signature_verification`: Ed25519 signature verification against canonical payloads.
- `test_6_tampering_detection`: Detection of corrupted ciphertext or signature bytes.
- `test_7_unauthorized_user`: Rejection of non-authorized usernames.
- `test_8_multiple_users`: Multi-user key isolation and signing for all 4 group members.

---

## Running Multiple Backend Instances

This backend is designed to run concurrently across multiple physical or virtual hosts (Systems 2, 3, and 4) behind a Dynamic Load Balancer (System 1), all sharing a single persistent database.

```text
               Load Generator / Clients
                          │
                          ▼
            System 1: Go Load Balancer
                          │
         ┌────────────────┼────────────────┐
         │                │                │
         ▼                ▼                ▼
     System 2         System 3         System 4
  Backend Instance 1  Backend Instance 2  Backend Instance 3
  (BACKEND_ID=b-1)  (BACKEND_ID=b-2)  (BACKEND_ID=b-3)
         │                │                │
         └────────────────┼────────────────┘
                          │
                          ▼
               Shared Persistent SQLite
                  (chat.db with WAL)
```

### 1. Configuration & Environment Variables

Configure each backend using environment variables or a `.env` file:

| Variable | Description | Example / Default |
|---|---|---|
| `BACKEND_ID` | Unique identifier for this physical/virtual backend | `backend-1` (Sys 2), `backend-2` (Sys 3), `backend-3` (Sys 4) |
| `BACKEND_HOST` | Network interface IP to bind to | `0.0.0.0` |
| `BACKEND_PORT` | Port for HTTP & WebSocket listeners | `4000` (or `8001`) |
| `DATABASE_URL` | Path to the shared SQLite database file | `/mnt/shared/chat.db` or `sqlite:///mnt/shared/chat.db` |
| `CHAT_DB_PATH` | Fallback database path if `DATABASE_URL` not set | `chat.db` |
| `CHAT_ENCRYPTION_KEY`| 32-byte AES key for at-rest message encryption | 64-char hex string |

> [!IMPORTANT]
> **Shared Database Setup:** Ensure Systems 2, 3, and 4 mount the same directory or share the SQLite database file (e.g., via NFS, SSHFS, or shared volume). SQLite automatically uses Write-Ahead Logging (`WAL` mode) and 30-second busy timeouts to safely coordinate concurrent transactions across multiple instances.

---

### 2. Starting the Backend on Each System

#### System 2 (Backend Instance 1)
```bash
export BACKEND_ID="backend-1"
export BACKEND_HOST="0.0.0.0"
export BACKEND_PORT=4000
export DATABASE_URL="/mnt/shared/chat.db"
export CHAT_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

python3 server.py
```

#### System 3 (Backend Instance 2)
```bash
export BACKEND_ID="backend-2"
export BACKEND_HOST="0.0.0.0"
export BACKEND_PORT=4000
export DATABASE_URL="/mnt/shared/chat.db"
export CHAT_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

python3 server.py
```

#### System 4 (Backend Instance 3)
```bash
export BACKEND_ID="backend-3"
export BACKEND_HOST="0.0.0.0"
export BACKEND_PORT=4000
export DATABASE_URL="/mnt/shared/chat.db"
export CHAT_ENCRYPTION_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

python3 server.py
```

---

### 3. API Verification with `curl`

Replace `SYSTEM2_IP`, `SYSTEM3_IP`, `SYSTEM4_IP` with the respective host addresses (port `4000` or configured port).

#### A. Health Check (`GET /health`)
Verify application status and shared database connectivity:
```bash
curl -s http://SYSTEM2_IP:4000/health
```
**Example Response (`200 OK`):**
```json
{
  "status": "healthy",
  "database": "connected",
  "backend_id": "backend-1"
}
```

#### B. Performance Metrics (`GET /metrics`)
Internal monitoring endpoint used by the dynamic Load Balancer to observe load:
```bash
curl -s http://SYSTEM2_IP:4000/metrics
```
**Example Response (`200 OK`):**
```json
{
  "backend_id": "backend-1",
  "healthy": true,
  "cpu_percent": 14.2,
  "memory_percent": 41.8,
  "active_requests": 0,
  "avg_latency_ms": 4.12,
  "recent_latency_ms": 3.85,
  "request_count": 142,
  "uptime_seconds": 3600
}
```

#### C. Post a Message (`POST /message`)
Submit a new chat message:
```bash
curl -s -X POST http://SYSTEM2_IP:4000/message \
  -H "Content-Type: application/json" \
  -d '{
    "client-name": "alice",
    "msg": "Hello from System 2!"
  }'
```
**Example Response (`201 Created`):**
```json
{
  "status": "success",
  "message_id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
  "created": true,
  "client-name": "alice",
  "backend_id": "backend-1"
}
```

#### D. Fetch Feed (`GET /feed`)
Retrieve persisted, decrypted, and signature-verified chat history:
```bash
curl -s http://SYSTEM3_IP:4000/feed
```
**Example Response (`200 OK`):**
```json
{
  "status": "success",
  "count": 1,
  "backend_id": "backend-2",
  "messages": [
    {
      "message_id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
      "id": "a1b2c3d4-e5f6-4a5b-8c9d-0e1f2a3b4c5d",
      "client-name": "alice",
      "client_name": "alice",
      "username": "alice",
      "msg": "Hello from System 2!",
      "message": "Hello from System 2!",
      "timestamp": "2026-09-12T07:45:00Z",
      "created_at": "2026-09-12T07:45:00Z",
      "verified": true
    }
  ]
}
```

---

### 4. Cross-Backend Persistence Verification Procedure

To verify that all backend instances share storage and remain stateless:

1. **Step 1 — Send a message to Backend 1 (System 2):**
   ```bash
   curl -s -X POST http://SYSTEM2_IP:4000/message \
     -H "Content-Type: application/json" \
     -d '{"client-name": "sys2_client", "msg": "Cross-backend sync test", "message_id": "SYNC-TEST-001"}'
   ```
2. **Step 2 — Read feed from Backend 2 (System 3):**
   ```bash
   curl -s http://SYSTEM3_IP:4000/feed | grep "SYNC-TEST-001"
   ```
   *Expected:* The message appears in System 3's feed with `"verified": true`.
3. **Step 3 — Read feed from Backend 3 (System 4):**
   ```bash
   curl -s http://SYSTEM4_IP:4000/feed | grep "SYNC-TEST-001"
   ```
   *Expected:* The exact same message appears in System 4's feed.

---

### 5. Idempotent Duplicate Prevention Verification

To verify that repeated submissions or network retries do not produce duplicate records:

1. **Submit message with a fixed ID to System 2:**
   ```bash
   curl -s -X POST http://SYSTEM2_IP:4000/message \
     -H "Content-Type: application/json" \
     -d '{"client-name": "alice", "msg": "Idempotent test", "message_id": "IDEMP-999"}'
   ```
   *Response:* Status `201 Created`, `"created": true`.

2. **Retry the same message with the same ID to System 3:**
   ```bash
   curl -s -X POST http://SYSTEM3_IP:4000/message \
     -H "Content-Type: application/json" \
     -d '{"client-name": "alice", "msg": "Idempotent test", "message_id": "IDEMP-999"}'
   ```
   *Response:* Status `200 OK`, `"created": false`.

3. **Check message count in feed:**
   ```bash
   curl -s http://SYSTEM4_IP:4000/feed | grep -c "IDEMP-999"
   ```
   *Expected Output:* `1` (The database enforces uniqueness; duplicate record was prevented).

