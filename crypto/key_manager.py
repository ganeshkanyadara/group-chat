import os
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

KEYS_DIR = Path(os.getenv("KEYS_DIR", "keys"))


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

    def get_private_key_path(self, username: str) -> Path:
        return self.keys_dir / f"{username}_private.pem"

    def get_or_create_user_keypair(self, username: str, db_path: str = None) -> tuple[Ed25519PrivateKey, bytes]:
        """
        Load or generate Ed25519 private key for username.
        Syncs the corresponding public key to database.
        Returns (private_key, public_key_raw_bytes).
        """
        if username in self._memory_private_keys:
            private_key = self._memory_private_keys[username]
        else:
            key_path = self.get_private_key_path(username)
            if key_path.exists():
                pem_data = key_path.read_bytes()
                private_key = load_pem_private_key(pem_data, password=None)
            else:
                private_key = Ed25519PrivateKey.generate()
                pem_bytes = private_key.private_bytes(
                    Encoding.PEM,
                    PrivateFormat.PKCS8,
                    NoEncryption(),
                )
                key_path.write_bytes(pem_bytes)
                try:
                    os.chmod(key_path, 0o600)
                except Exception:
                    pass

            self._memory_private_keys[username] = private_key

        public_key = private_key.public_key()
        pub_bytes = public_key.public_bytes(Encoding.Raw, PublicFormat.Raw)

        # Sync public key to database
        target_db = db_path or self.db_path
        if target_db:
            save_public_key(username, pub_bytes, db_path=target_db)
        else:
            save_public_key(username, pub_bytes)

        return private_key, pub_bytes

    def get_public_key_bytes(self, username: str, db_path: str = None) -> bytes | None:
        """Fetch raw public key bytes from memory, disk, or SQLite."""
        target_db = db_path or self.db_path

        # If we have it locally, verify it against DB or return it
        if username in self._memory_private_keys:
            pub = self._memory_private_keys[username].public_key()
            return pub.public_bytes(Encoding.Raw, PublicFormat.Raw)

        if target_db:
            return load_public_key(username, db_path=target_db)
        return load_public_key(username)

    def initialize_keys_for_users(self, usernames: list[str]) -> None:
        """Pre-initialize key pairs for a list of authorized usernames."""
        for username in usernames:
            self.get_or_create_user_keypair(username)
