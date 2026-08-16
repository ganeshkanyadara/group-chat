import json
from cryptography.hazmat.primitives.asymmetric.ed25519 import (
    Ed25519PrivateKey,
    Ed25519PublicKey,
)
from cryptography.exceptions import InvalidSignature


def construct_canonical_payload(
    room_id: str,
    sender_id: str,
    message: str,
    timestamp: str,
) -> bytes:
    """
    Construct a deterministic canonical JSON payload binding message metadata.
    
    Binds room_id, sender_id, message plaintext, and timestamp to prevent
    metadata modification or replaying across rooms/users.
    """
    payload_dict = {
        "room_id": str(room_id),
        "sender_id": str(sender_id),
        "message": str(message),
        "timestamp": str(timestamp),
    }
    return json.dumps(payload_dict, sort_keys=True, separators=(",", ":")).encode("utf-8")


def sign_message(private_key: Ed25519PrivateKey, canonical_payload: bytes) -> bytes:
    """
    Sign canonical byte payload using sender's Ed25519 private key.
    Returns raw 64-byte signature bytes.
    """
    return private_key.sign(canonical_payload)


def verify_signature(
    public_key_bytes: bytes,
    canonical_payload: bytes,
    signature_bytes: bytes,
) -> bool:
    """
    Verify signature against canonical payload using sender's raw Ed25519 public key bytes.
    
    Returns True if verification succeeds.
    Returns False if signature verification fails or input is invalid.
    """
    try:
        if not public_key_bytes or not signature_bytes or not canonical_payload:
            return False

        public_key = Ed25519PublicKey.from_public_bytes(public_key_bytes)
        public_key.verify(signature_bytes, canonical_payload)
        return True

    except (InvalidSignature, ValueError, TypeError, Exception):
        return False
