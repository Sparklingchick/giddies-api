import hashlib
import hmac
import secrets
import uuid
import json
import os
from datetime import datetime, timedelta

SECRET_KEY = os.environ.get("SECRET_KEY", "gx-secret-change-this-in-production-2024")

def hash_password(password: str) -> str:
    """Secure password hashing with PBKDF2"""
    salt = "gx_pbkdf2_salt_v1"
    return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()

def verify_password(password: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(password), hashed)

def generate_id() -> str:
    return str(uuid.uuid4())

def now_iso() -> str:
    return datetime.utcnow().isoformat()

def expires_iso(hours: int = 8) -> str:
    return (datetime.utcnow() + timedelta(hours=hours)).isoformat()

def is_expired(expires_at: str) -> bool:
    try:
        return datetime.fromisoformat(expires_at) < datetime.utcnow()
    except:
        return True

def create_token(user_id: str, role: str, email: str) -> str:
    """Create a signed token: base64(payload).signature"""
    import base64
    payload = {
        "uid": user_id,
        "role": role,
        "email": email,
        "jti": secrets.token_hex(16),
        "iat": datetime.utcnow().isoformat(),
        "exp": expires_iso(8)
    }
    payload_b64 = base64.urlsafe_b64encode(
        json.dumps(payload).encode()
    ).decode().rstrip("=")
    
    sig = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
    return f"{payload_b64}.{sig}"

def verify_token_signature(token: str) -> dict | None:
    """Verify token signature and return payload if valid"""
    import base64
    try:
        parts = token.rsplit(".", 1)
        if len(parts) != 2:
            return None
        payload_b64, sig = parts
        expected_sig = hmac.new(SECRET_KEY.encode(), payload_b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected_sig):
            return None
        # Decode payload
        padded = payload_b64 + "=" * (4 - len(payload_b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        # Check expiry
        if is_expired(payload.get("exp", "")):
            return None
        return payload
    except:
        return None

def strong_password_check(password: str) -> str | None:
    """Returns error message if password is weak, None if strong"""
    if len(password) < 8:
        return "Password must be at least 8 characters"
    if not any(c.isupper() for c in password):
        return "Password must contain at least one uppercase letter"
    if not any(c.islower() for c in password):
        return "Password must contain at least one lowercase letter"
    if not any(c.isdigit() for c in password):
        return "Password must contain at least one number"
    return None
