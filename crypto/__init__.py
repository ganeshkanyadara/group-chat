"""
Crypto package providing AES-256-GCM encryption, Ed25519 digital signatures, and key management.
"""
from .encryption import encrypt_message, decrypt_message, get_encryption_key
from .signing import sign_message, verify_signature, construct_canonical_payload
from .key_manager import KeyManager

__all__ = [
    "encrypt_message",
    "decrypt_message",
    "get_encryption_key",
    "sign_message",
    "verify_signature",
    "construct_canonical_payload",
    "KeyManager",
]
