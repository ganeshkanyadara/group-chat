import os
import threading
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.hazmat.primitives.serialization import (
    load_pem_private_key,
    Encoding,
    PrivateFormat,
    PublicFormat,
    NoEncryption,
)
from database import save_public_key, load_public_key

DEFAULT_KEYS_DIR = Path(__file__).resolve().parent.parent / "keys"
KEYS_DIR = Path(os.getenv("KEYS_DIR", str(DEFAULT_KEYS_DIR)))


class KeyManager:
    """
    Manages persistent Ed25519 user keypairs.
    
    Private keys are saved securely on disk in `keys/<username>_private.pem`
    and NEVER transmitted over WebSockets or stored in SQLite.
    Public keys are stored in SQLite `user_keys` table for signature verification.
    """

    def __init__(self, keys_dir: Path = KEYS_DIR, db_path: str = None):
        self.keys_dir = Path(keys_dir)
        self.keys_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = db_path
        self._memory_private_keys: dict[str, Ed25519PrivateKey] = {}
        self._synced_usernames: set[str] = set()
        self._public_keys_cache: dict[str, bytes] = {}

    def get_private_key_path(self, username: str) -> Path:
        return self.keys_dir / f"{username}_private.pem"

    def get_or_create_user_keypair(self, username: str, db_path: str = None) -> tuple[Ed25519PrivateKey, bytes]:
        """
        Load or generate Ed25519 private key for username.
        Syncs the corresponding public key to database once per user.
        Returns (private_key, public_key_raw_bytes).
        """
        if username in self._memory_private_keys:
            private_key = self._memory_private_keys[username]
        else:
            key_path = self.get_private_key_path(username)
            if key_path.exists():
                try:
                    pem_data = key_path.read_bytes()
                    private_key = load_pem_private_key(pem_data, password=None)
                except Exception:
                    private_key = Ed25519PrivateKey.generate()
            else:
                private_key = Ed25519PrivateKey.generate()
            self._memory_private_keys[username] = private_key

        public_key = private_key.public_key()
        pub_bytes = public_key.public_bytes(Encoding.Raw, PublicFormat.Raw)
        self._public_keys_cache[username] = pub_bytes

        # Sync public key to database in background thread without blocking the request
        if username not in self._synced_usernames:
            self._synced_usernames.add(username)
            target_db = db_path or self.db_path
            if target_db:
                threading.Thread(target=save_public_key, args=(username, pub_bytes, target_db), daemon=True).start()

        return private_key, pub_bytes

    def get_public_key_bytes(self, username: str, db_path: str = None) -> bytes | None:
        """
        Fetch raw public key bytes for signature verification.
        Uses in-memory cache first, then shared DB, falling back to local memory and disk.
        """
        # 0. Fast in-memory cache hit
        if username in self._public_keys_cache:
            return self._public_keys_cache[username]

        target_db = db_path or self.db_path

        # 1. Fetch from shared database first (canonical source of truth for the cluster)
        if target_db:
            try:
                db_pub = load_public_key(username, db_path=target_db)
                if db_pub:
                    self._public_keys_cache[username] = db_pub
                    return db_pub
            except Exception:
                pass

        # 2. Fall back to local memory if DB has no record or is unreachable
        if username in self._memory_private_keys:
            pub = self._memory_private_keys[username].public_key()
            pub_bytes = pub.public_bytes(Encoding.Raw, PublicFormat.Raw)
            self._public_keys_cache[username] = pub_bytes
            return pub_bytes

        # 3. Fall back to local disk
        key_path = self.get_private_key_path(username)
        if key_path.exists():
            try:
                pem_data = key_path.read_bytes()
                priv = load_pem_private_key(pem_data, password=None)
                pub_bytes = priv.public_key().public_bytes(Encoding.Raw, PublicFormat.Raw)
                self._public_keys_cache[username] = pub_bytes
                return pub_bytes
            except Exception:
                pass

        return None

    def initialize_keys_for_users(self, usernames: list[str]) -> None:
        """Pre-initialize key pairs for a list of authorized usernames."""
        for username in usernames:
            self.get_or_create_user_keypair(username)
