from fastapi import Header, HTTPException, Request
from database import get_db
from auth_utils import verify_token_signature, now_iso, generate_id
import json

MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_MINUTES = 15

def get_current_user(authorization: str = Header(None)):
    """Validate token on EVERY request - role comes from DB, never from token alone"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Authentication required")
    
    token = authorization.split(" ", 1)[1].strip()
    
    # 1. Verify token signature first (fast, no DB needed)
    payload = verify_token_signature(token)
    if not payload:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    
    db = get_db()
    try:
        # 2. Check session exists and is active in DB
        session = db.execute(
            "SELECT * FROM sessions WHERE token=? AND is_active=1 AND force_logout=0",
            (token,)
        ).fetchone()
        
        if not session:
            raise HTTPException(status_code=401, 
                detail="Session not found or was terminated")
        
        # 3. Get fresh user data from DB (role always from DB, not token)
        user = db.execute(
            "SELECT u.*, e.name, e.id as emp_id, e.department as emp_dept, "
            "e.salary, e.title, e.color "
            "FROM users u LEFT JOIN employees e ON e.user_id=u.id "
            "WHERE u.id=?", (payload["uid"],)
        ).fetchone()
        
        if not user:
            raise HTTPException(status_code=401, detail="User not found")
        
        if not user["is_active"]:
            raise HTTPException(status_code=403, detail="Account deactivated")
        
        if user["is_suspended"]:
            reason = user["suspension_reason"] or "Contact HR or IT Support"
            raise HTTPException(status_code=403, 
                detail=f"Account suspended: {reason}")
        
        # Role comes from DATABASE, not from the token
        return {
            "id": user["id"],
            "email": user["email"],
            "role": user["role"],  # ← always from DB
            "department": user["emp_dept"] or user["department"],
            "name": user["name"] or user["email"],
            "emp_id": user["emp_id"],
            "salary": user["salary"] or 0,
            "title": user["title"] or "",
            "color": user["color"] or "#FF6B00",
            "token": token,
            "session_id": session["id"]
        }
    finally:
        db.close()

def get_current_user_optional(authorization: str = Header(None)):
    """Same as get_current_user but returns None instead of raising"""
    try:
        return get_current_user(authorization)
    except:
        return None

def log_action(user_id, user_name, user_role, action, 
               resource=None, resource_id=None, details=None, 
               status="success", ip=None):
    db = get_db()
    try:
        db.execute("""INSERT INTO activity_log 
                      (id,user_id,user_name,user_role,action,resource,resource_id,details,ip_address,status)
                      VALUES (?,?,?,?,?,?,?,?,?,?)""",
                   (generate_id(), user_id, user_name, user_role, action,
                    resource, resource_id,
                    json.dumps(details) if details and not isinstance(details, str) else details,
                    ip, status))
        db.commit()
    except Exception as e:
        print(f"Log error: {e}")
    finally:
        db.close()
