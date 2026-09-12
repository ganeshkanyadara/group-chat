import os
import secrets
import base64
from pathlib import Path
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

# Direct .env loader fallback if python-dotenv is missing
for _base in [Path(__file__).resolve().parent.parent, Path.cwd()]:
    _env_f = _base / ".env"
    if _env_f.exists():
        try:
            for _line in _env_f.read_text(encoding="utf-8").splitlines():
                _line = _line.strip()
                if not _line or _line.startswith("#") or "=" not in _line:
                    continue
                _k, _v = _line.split("=", 1)
                _k, _v = _k.strip(), _v.strip().strip("'\"")
                if _k and _k not in os.environ:
                    os.environ[_k] = _v
        except Exception:
            pass

KEYS_DIR = Path(os.getenv("KEYS_DIR", "keys"))
KEY_FILE = KEYS_DIR / "chat_encryption.key"


def get_encryption_key() -> bytes:
    """
    Retrieve or generate the 32-byte (256-bit) AES-GCM encryption key.
    
    Order of precedence:
    1. Environment variable `CHAT_ENCRYPTION_KEY` (hex or base64 or raw 32-byte ascii)
    2. Saved keyfile `keys/chat_encryption.key`
    3. Auto-generated and stored in `keys/chat_encryption.key`
    """
    env_key = os.getenv("CHAT_ENCRYPTION_KEY")
    if env_key:
        env_key = env_key.strip()
        # Check if hex encoded (64 chars)
        if len(env_key) == 64:
            try:
                return bytes.fromhex(env_key)
            except ValueError:
                pass
        # Check if base64 encoded
        try:
            decoded = base64.b64decode(env_key)
            if len(decoded) == 32:
                return decoded
        except Exception:
            pass
        # Raw bytes/string fallback if exactly 32 bytes
        raw_bytes = env_key.encode("utf-8")
        if len(raw_bytes) == 32:
            return raw_bytes

    # Check keyfile fallback
    if KEY_FILE.exists():
        try:
            key_data = KEY_FILE.read_bytes()
            if len(key_data) == 32:
                return key_data
            elif len(key_data) == 64:
                return bytes.fromhex(key_data.decode("utf-8").strip())
        except Exception:
            pass

    # Auto-generate new key and persist to keyfile
    KEYS_DIR.mkdir(parents=True, exist_ok=True)
    new_key = AESGCM.generate_key(bit_length=256)
    KEY_FILE.write_bytes(new_key)
    # Restrict key file permissions on Linux/macOS
    try:
        os.chmod(KEY_FILE, 0o600)
    except Exception:
        pass

    return new_key


def encrypt_message(plaintext: str, key: bytes | None = None) -> tuple[bytes, bytes]:
    """
    Encrypt plaintext string using AES-256-GCM.
    
    Generates a secure random 12-byte (96-bit) nonce for every operation.
    Returns (ciphertext_bytes, nonce_bytes).
    """
    if key is None:
        key = get_encryption_key()

    cipher = AESGCM(key)
    nonce = secrets.token_bytes(12)  # 96-bit nonce
    ciphertext = cipher.encrypt(nonce, plaintext.encode("utf-8"), None)

    return ciphertext, nonce


def decrypt_message(ciphertext: bytes, nonce: bytes, key: bytes | None = None) -> str:
    """
    Decrypt AES-256-GCM ciphertext using the given nonce and key.
    
    Raises cryptography.exceptions.InvalidTag if ciphertext or nonce is invalid/tampered.
    """
    if key is None:
        key = get_encryption_key()

    cipher = AESGCM(key)
    plaintext_bytes = cipher.decrypt(nonce, ciphertext, None)
    return plaintext_bytes.decode("utf-8")
