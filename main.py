import sqlite3, hashlib, hmac, secrets, uuid, json, os, random
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

SECRET_KEY = os.environ.get("SECRET_KEY", "gx-change-this-secret-2024")
DB_PATH = os.environ.get("DB_PATH", "giddies.db")
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_MINUTES = 15
COLORS = ['#FF6B00','#F472B6','#60A5FA','#4ADE80','#A78BFA',
          '#FB923C','#34D399','#22D3EE','#F87171','#C084FC']

app = FastAPI(title="Giddies Express API")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.executescript("""
    CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT UNIQUE NOT NULL COLLATE NOCASE,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'employee',
        department TEXT,
        is_active INTEGER DEFAULT 1,
        is_suspended INTEGER DEFAULT 0,
        suspension_reason TEXT,
        suspended_by TEXT,
        suspended_at TEXT,
        login_attempts INTEGER DEFAULT 0,
        locked_until TEXT,
        password_reset_required INTEGER DEFAULT 0,
        created_by TEXT,
        last_login TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS employees (
        id TEXT PRIMARY KEY,
        user_id TEXT UNIQUE REFERENCES users(id) ON DELETE CASCADE,
        name TEXT NOT NULL,
        email TEXT UNIQUE NOT NULL COLLATE NOCASE,
        phone TEXT,
        department TEXT,
        role TEXT NOT NULL DEFAULT 'employee',
        title TEXT,
        salary REAL DEFAULT 0,
        contract_type TEXT DEFAULT 'Permanent',
        join_date TEXT,
        status TEXT DEFAULT 'active',
        color TEXT DEFAULT '#FF6B00',
        emergency_contact TEXT,
        emergency_phone TEXT,
        address TEXT,
        date_of_birth TEXT,
        bank_name TEXT,
        account_number TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        token TEXT UNIQUE NOT NULL,
        ip_address TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        expires_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        force_logout INTEGER DEFAULT 0,
        logout_reason TEXT
    );
    CREATE TABLE IF NOT EXISTS activity_log (
        id TEXT PRIMARY KEY,
        user_id TEXT,
        user_name TEXT,
        user_role TEXT,
        action TEXT NOT NULL,
        details TEXT,
        ip_address TEXT,
        status TEXT DEFAULT 'success',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS user_requests (
        id TEXT PRIMARY KEY,
        type TEXT NOT NULL DEFAULT 'new_user',
        status TEXT DEFAULT 'pending_manager',
        submitted_by TEXT,
        submitted_by_name TEXT,
        target_user_id TEXT,
        request_data TEXT,
        manager_approval TEXT,
        manager_name TEXT,
        manager_note TEXT,
        manager_approved_at TEXT,
        it_approval TEXT,
        it_name TEXT,
        it_note TEXT,
        it_approved_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS departments (
        id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        icon TEXT DEFAULT '🏢',
        head_name TEXT,
        budget REAL DEFAULT 0,
        spent REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS attendance (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        date TEXT NOT NULL,
        clock_in TEXT,
        clock_out TEXT,
        status TEXT DEFAULT 'absent',
        notes TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS leave_requests (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        type TEXT NOT NULL,
        from_date TEXT NOT NULL,
        to_date TEXT NOT NULL,
        days INTEGER,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejected_reason TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS budget_requests (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL,
        title TEXT,
        amount REAL NOT NULL,
        reason TEXT,
        submitted_by TEXT,
        submitter_name TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS fund_transfers (
        id TEXT PRIMARY KEY,
        to_dept TEXT NOT NULL,
        amount REAL NOT NULL,
        note TEXT,
        reference TEXT,
        sent_by TEXT,
        sent_by_name TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS spending_requests (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL,
        title TEXT NOT NULL,
        amount REAL NOT NULL,
        vendor TEXT,
        reason TEXT,
        submitted_by TEXT,
        submitter_name TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS announcements (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        type TEXT DEFAULT 'info',
        department TEXT DEFAULT 'all',
        posted_by TEXT,
        poster_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS notifications (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT,
        type TEXT DEFAULT 'in',
        read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS it_tickets (
        id TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        description TEXT,
        submitted_by TEXT,
        submitter_name TEXT,
        department TEXT,
        priority TEXT DEFAULT 'medium',
        status TEXT DEFAULT 'open',
        assigned_to TEXT,
        assigned_name TEXT,
        resolution TEXT,
        resolved_at TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        sender_name TEXT,
        content TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS tasks (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        assigned_to TEXT,
        assigned_to_name TEXT,
        assigned_by TEXT,
        assigned_by_name TEXT,
        department TEXT,
        due_date TEXT,
        priority TEXT DEFAULT 'medium',
        status TEXT DEFAULT 'pending',
        progress_note TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS payroll_records (
        id TEXT PRIMARY KEY,
        employee_id TEXT REFERENCES employees(id),
        period TEXT NOT NULL,
        gross REAL DEFAULT 0,
        tax REAL DEFAULT 0,
        ni REAL DEFAULT 0,
        pension REAL DEFAULT 0,
        net REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        paid_date TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS complaints (
        id TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        detail TEXT,
        filer_id TEXT,
        filer_name TEXT,
        against_name TEXT,
        department TEXT,
        severity TEXT DEFAULT 'medium',
        status TEXT DEFAULT 'open',
        hr_note TEXT,
        resolved_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS job_applications (
        id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        role_applied TEXT,
        department TEXT,
        cover_letter TEXT,
        status TEXT DEFAULT 'pending',
        reviewer_name TEXT,
        interview_date TEXT,
        interview_notes TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS supply_requests (
        id TEXT PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity INTEGER DEFAULT 1,
        reason TEXT,
        department TEXT,
        requester_name TEXT,
        requester_id TEXT,
        urgency TEXT DEFAULT 'Normal',
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS performance_reviews (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id),
        employee_name TEXT,
        department TEXT,
        reviewer_id TEXT,
        reviewer_name TEXT,
        period TEXT NOT NULL,
        score INTEGER DEFAULT 0,
        punctuality INTEGER DEFAULT 0,
        teamwork INTEGER DEFAULT 0,
        productivity INTEGER DEFAULT 0,
        communication INTEGER DEFAULT 0,
        comments TEXT,
        status TEXT DEFAULT 'draft',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS overtime_requests (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id),
        employee_name TEXT,
        department TEXT,
        date TEXT NOT NULL,
        hours REAL NOT NULL,
        reason TEXT,
        rate_multiplier REAL DEFAULT 1.5,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS expense_claims (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id),
        employee_name TEXT,
        department TEXT,
        title TEXT NOT NULL,
        amount REAL NOT NULL,
        category TEXT DEFAULT 'other',
        description TEXT,
        receipt_note TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS disciplinary_records (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id),
        employee_name TEXT,
        department TEXT,
        type TEXT NOT NULL,
        description TEXT,
        action_taken TEXT,
        issued_by TEXT,
        issuer_name TEXT,
        severity TEXT DEFAULT 'warning',
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    conn.commit()
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        uid = str(uuid.uuid4())
        eid = str(uuid.uuid4())
        conn.execute("INSERT INTO users (id,email,password_hash,role,department,is_active) VALUES (?,?,?,?,?,1)",
                     (uid, "admin@giddiesexpress.com", _hash_pwd("Admin@1234"), "admin", "Administration"))
        conn.execute("INSERT INTO employees (id,user_id,name,email,department,role,title,status,color) VALUES (?,?,?,?,?,?,?,'active','#F59E0B')",
                     (eid, uid, "System Administrator", "admin@giddiesexpress.com",
                      "Administration", "admin", "System Administrator"))
        conn.commit()
        print("✅ Admin created — email: admin@giddiesexpress.com | password: Admin@1234")
    if conn.execute("SELECT COUNT(*) FROM departments").fetchone()[0] == 0:
        depts = [("HR","👥"),("Finance","💰"),("IT Support","💻"),("Payroll","🏦"),
                 ("Logistics","🚚"),("Warehouse","📦"),("Customer Service","🤝"),
                 ("Marketing","📣"),("Legal","⚖️"),("Management","🏢"),
                 ("Administration","⚙️"),("Recruitment","📢")]
        for name, icon in depts:
            conn.execute("INSERT OR IGNORE INTO departments (id,name,icon) VALUES (?,?,?)",
                         (str(uuid.uuid4()), name, icon))
        conn.commit()
    conn.close()

def _hash_pwd(password: str) -> str:
    return hashlib.pbkdf2_hmac('sha256', password.encode(), b'gx_salt_v1', 100000).hex()

def _verify_pwd(password: str, hashed: str) -> bool:
    return hmac.compare_digest(_hash_pwd(password), hashed)

def _gen_id() -> str:
    return str(uuid.uuid4())

def _now() -> str:
    return datetime.utcnow().isoformat()

def _expires(hours=8) -> str:
    return (datetime.utcnow() + timedelta(hours=hours)).isoformat()

def _is_expired(dt_str: str) -> bool:
    try:
        return datetime.fromisoformat(dt_str) < datetime.utcnow()
    except:
        return True

def _create_token(user_id: str, role: str, email: str) -> str:
    import base64
    payload = json.dumps({"uid": user_id, "role": role, "email": email,
                          "jti": secrets.token_hex(16), "exp": _expires(8)})
    b64 = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    sig = hmac.new(SECRET_KEY.encode(), b64.encode(), hashlib.sha256).hexdigest()
    return f"{b64}.{sig}"

def _verify_token(token: str):
    import base64
    try:
        b64, sig = token.rsplit(".", 1)
        expected = hmac.new(SECRET_KEY.encode(), b64.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        padded = b64 + "=" * (4 - len(b64) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded).decode())
        if _is_expired(payload.get("exp", "")):
            return None
        return payload
    except:
        return None

def _check_pwd_strength(pwd: str):
    if len(pwd) < 8: return "Password must be at least 8 characters"
    if not any(c.isupper() for c in pwd): return "Must have at least one uppercase letter"
    if not any(c.islower() for c in pwd): return "Must have at least one lowercase letter"
    if not any(c.isdigit() for c in pwd): return "Must have at least one number"
    return None

def _get_user(authorization: str):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Authentication required")
    token = authorization.split(" ", 1)[1].strip()
    payload = _verify_token(token)
    if not payload:
        raise HTTPException(401, "Invalid or expired token — please login again")
    db = get_db()
    try:
        sess = db.execute("SELECT * FROM sessions WHERE token=? AND is_active=1 AND force_logout=0", (token,)).fetchone()
        if not sess:
            raise HTTPException(401, "Session not found or was terminated")
        user = db.execute(
            "SELECT u.*, e.name, e.id as emp_id, e.department as emp_dept, e.salary, e.title, e.color "
            "FROM users u LEFT JOIN employees e ON e.user_id=u.id WHERE u.id=?",
            (payload["uid"],)
        ).fetchone()
        if not user: raise HTTPException(401, "User not found")
        if not user["is_active"]: raise HTTPException(403, "Account deactivated")
        if user["is_suspended"]:
            raise HTTPException(403, f"Account suspended. Reason: {user['suspension_reason'] or 'Contact HR'}")
        d = dict(user)
        d["department"] = d.get("emp_dept") or d.get("department")
        return d
    finally:
        db.close()

def _log(user_id, user_name, user_role, action, details=None, ip=None, status="success"):
    db = get_db()
    try:
        db.execute("INSERT INTO activity_log (id,user_id,user_name,user_role,action,details,ip_address,status) VALUES (?,?,?,?,?,?,?,?)",
                   (_gen_id(), user_id, user_name, user_role, action,
                    json.dumps(details) if details else None, ip, status))
        db.commit()
    finally:
        db.close()

def _notify(employee_id, title, message, ntype="in"):
    db = get_db()
    try:
        db.execute("INSERT INTO notifications (id,employee_id,title,message,type) VALUES (?,?,?,?,?)",
                   (_gen_id(), employee_id, title, message, ntype))
        db.commit()
    finally:
        db.close()

init_db()

class LoginReq(BaseModel):
    email: str
    password: str

@app.post("/api/auth/login")
def login(req: LoginReq, request: Request):
    ip = request.client.host if request.client else "unknown"
    db = get_db()
    try:
        user = db.execute("SELECT u.*, e.name, e.id as emp_id, e.color, e.department as emp_dept, e.salary, e.title "
                          "FROM users u LEFT JOIN employees e ON e.user_id=u.id "
                          "WHERE u.email=?", (req.email.lower(),)).fetchone()
        if not user:
            _log(None, req.email, "unknown", "LOGIN_FAILED", {"reason": "user not found"}, ip, "fail")
            raise HTTPException(401, "Invalid email or password")
        if user["locked_until"] and not _is_expired(user["locked_until"]):
            raise HTTPException(429, f"Account locked. Try again after {user['locked_until'][:16]}")
        if user["is_suspended"]:
            raise HTTPException(403, f"Account suspended: {user['suspension_reason'] or 'Contact HR'}")
        if not user["is_active"]:
            raise HTTPException(403, "Account is deactivated")
        if not _verify_pwd(req.password, user["password_hash"]):
            attempts = (user["login_attempts"] or 0) + 1
            locked_until = None
            if attempts >= MAX_LOGIN_ATTEMPTS:
                locked_until = (datetime.utcnow() + timedelta(minutes=LOCKOUT_MINUTES)).isoformat()
            db.execute("UPDATE users SET login_attempts=?, locked_until=? WHERE id=?",
                       (attempts, locked_until, user["id"]))
            db.commit()
            _log(user["id"], user["name"], user["role"], "LOGIN_FAILED", {"attempts": attempts}, ip, "fail")
            raise HTTPException(401, f"Invalid password. {MAX_LOGIN_ATTEMPTS - attempts} attempts remaining")
        token = _create_token(user["id"], user["role"], user["email"])
        db.execute("INSERT INTO sessions (id,user_id,token,ip_address,expires_at) VALUES (?,?,?,?,?)",
                   (_gen_id(), user["id"], token, ip, _expires(8)))
        db.execute("UPDATE users SET login_attempts=0, locked_until=NULL, last_login=? WHERE id=?",
                   (_now(), user["id"]))
        db.commit()
        _log(user["id"], user["name"], user["role"], "LOGIN_SUCCESS", None, ip)
        return {
            "token": token,
            "user": {
                "id": user["id"], "email": user["email"], "role": user["role"],
                "department": user.get("emp_dept") or user.get("department"),
                "name": user["name"], "color": user["color"], "emp_id": user["emp_id"],
                "salary": user["salary"], "title": user["title"],
                "password_reset_required": bool(user["password_reset_required"])
            }
        }
    finally:
        db.close()

@app.post("/api/auth/logout")
def logout(authorization: str = Header(None)):
    if authorization:
        token = authorization.replace("Bearer ", "").strip()
        db = get_db()
        db.execute("UPDATE sessions SET is_active=0 WHERE token=?", (token,))
        db.commit()
        db.close()
    return {"message": "Logged out"}

@app.get("/api/auth/verify")
def verify(authorization: str = Header(None)):
    user = _get_user(authorization)
    return {"valid": True, "user": user}

@app.get("/api/auth/verify-role")
def verify_role(required_role: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    allowed = [r.strip() for r in required_role.split(",")]
    if user["role"] not in allowed:
        raise HTTPException(403, f"Access denied. Required: {', '.join(allowed)}")
    return {"valid": True, "user": user}

class ChangePwdReq(BaseModel):
    current_password: str
    new_password: str

@app.post("/api/auth/change-password")
def change_password(req: ChangePwdReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    err = _check_pwd_strength(req.new_password)
    if err: raise HTTPException(400, err)
    db = get_db()
    try:
        db_user = db.execute("SELECT * FROM users WHERE id=?", (user["id"],)).fetchone()
        if not _verify_pwd(req.current_password, db_user["password_hash"]):
            raise HTTPException(400, "Current password is incorrect")
        db.execute("UPDATE users SET password_hash=?, password_reset_required=0, updated_at=? WHERE id=?",
                   (_hash_pwd(req.new_password), _now(), user["id"]))
        db.commit()
        _log(user["id"], user["name"], user["role"], "PASSWORD_CHANGED")
        return {"message": "Password changed successfully"}
    finally:
        db.close()

class CreateUserReq(BaseModel):
    name: str
    email: str
    password: str
    role: str
    department: str
    title: str = ""
    salary: float = 0
    phone: str = ""

@app.post("/api/users/create")
def create_user(req: CreateUserReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Only Admin or IT Support can create users")
    err = _check_pwd_strength(req.password)
    if err: raise HTTPException(400, err)
    db = get_db()
    try:
        if db.execute("SELECT id FROM users WHERE email=?", (req.email.lower(),)).fetchone():
            raise HTTPException(400, "Email already registered")
        uid = _gen_id(); eid = _gen_id()
        db.execute("INSERT INTO users (id,email,password_hash,role,department,is_active,created_by) VALUES (?,?,?,?,?,1,?)",
                   (uid, req.email.lower(), _hash_pwd(req.password), req.role, req.department, user["id"]))
        db.execute("INSERT INTO employees (id,user_id,name,email,department,role,title,salary,phone,join_date,status,color) VALUES (?,?,?,?,?,?,?,?,?,date('now'),'active',?)",
                   (eid, uid, req.name, req.email.lower(), req.department, req.role, req.title, req.salary, req.phone, random.choice(COLORS)))
        db.commit()
        _log(user["id"], user["name"], user["role"], "USER_CREATED", {"name": req.name, "email": req.email, "role": req.role})
        return {"message": f"{req.name} created successfully", "user_id": uid, "employee_id": eid}
    finally:
        db.close()

@app.get("/api/users/")
def list_users(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        rows = db.execute("""SELECT u.id, u.email, u.role, u.department, u.is_active,
                             u.is_suspended, u.suspension_reason, u.last_login, u.created_at,
                             u.password_reset_required, u.login_attempts,
                             e.name, e.title, e.salary, e.status as emp_status, e.department as emp_dept
                             FROM users u LEFT JOIN employees e ON e.user_id=u.id
                             ORDER BY u.created_at DESC""").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class SuspendReq(BaseModel):
    reason: str
    notify_user: bool = True

@app.post("/api/users/{user_id}/suspend")
def suspend_user(user_id: str, body: SuspendReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if not body.reason.strip(): raise HTTPException(400, "A reason is required to suspend")
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized to suspend users")
    if user["role"] == "dept_manager" and user["department"] not in ["HR", "IT Support"]:
        raise HTTPException(403, "Only HR or IT Support managers can suspend accounts")
    db = get_db()
    try:
        target = db.execute("SELECT u.*, e.name as emp_name, e.id as emp_id FROM users u LEFT JOIN employees e ON e.user_id=u.id WHERE u.id=?", (user_id,)).fetchone()
        if not target: raise HTTPException(404, "User not found")
        if user_id == user["id"]: raise HTTPException(400, "Cannot suspend your own account")
        if target["role"] == "admin": raise HTTPException(403, "Cannot suspend an Admin account")
        if target["role"] == "manager" and user["role"] == "dept_manager":
            raise HTTPException(403, "Department managers cannot suspend company managers")
        db.execute("UPDATE users SET is_suspended=1, suspension_reason=?, suspended_by=?, suspended_at=?, updated_at=? WHERE id=?",
                   (body.reason.strip(), user["id"], _now(), _now(), user_id))
        db.execute("UPDATE sessions SET is_active=0, force_logout=1, logout_reason='Account suspended' WHERE user_id=?", (user_id,))
        if body.notify_user and target["emp_id"]:
            _notify(target["emp_id"], "Account Suspended",
                    f"Your account was suspended by {user['name']}. Reason: {body.reason}. Contact HR or IT Support.", "er")
        db.commit()
        _log(user["id"], user["name"], user["role"], "USER_SUSPENDED", {"target": target["emp_name"], "reason": body.reason})
        return {"message": f"{target['emp_name']} suspended"}
    finally:
        db.close()

@app.post("/api/users/{user_id}/unsuspend")
def unsuspend_user(user_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    if user["role"] == "dept_manager" and user["department"] not in ["HR", "IT Support"]:
        raise HTTPException(403, "Only HR or IT Support can unsuspend")
    db = get_db()
    try:
        target = db.execute("SELECT u.*, e.name as emp_name, e.id as emp_id FROM users u LEFT JOIN employees e ON e.user_id=u.id WHERE u.id=?", (user_id,)).fetchone()
        if not target: raise HTTPException(404, "User not found")
        db.execute("UPDATE users SET is_suspended=0, suspension_reason=NULL, suspended_by=NULL, suspended_at=NULL, updated_at=? WHERE id=?", (_now(), user_id))
        if target["emp_id"]:
            _notify(target["emp_id"], "Account Reinstated",
                    f"Your account has been reinstated by {user['name']}. You may now log in.", "ok")
        db.commit()
        _log(user["id"], user["name"], user["role"], "USER_UNSUSPENDED", {"target": target["emp_name"]})
        return {"message": f"{target['emp_name']} reinstated"}
    finally:
        db.close()

@app.delete("/api/users/{user_id}")
def delete_user(user_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin": raise HTTPException(403, "Only Admin can delete users")
    if user_id == user["id"]: raise HTTPException(400, "Cannot delete your own account")
    db = get_db()
    try:
        target = db.execute("SELECT u.*, e.name as emp_name FROM users u LEFT JOIN employees e ON e.user_id=u.id WHERE u.id=?", (user_id,)).fetchone()
        if not target: raise HTTPException(404, "User not found")
        if target["role"] == "admin": raise HTTPException(400, "Cannot delete another Admin")
        name = target["emp_name"] or target["email"]
        db.execute("DELETE FROM users WHERE id=?", (user_id,))
        db.commit()
        _log(user["id"], user["name"], user["role"], "USER_DELETED", {"deleted": name})
        return {"message": f"{name} permanently deleted"}
    finally:
        db.close()

@app.post("/api/users/{user_id}/reset-password")
def reset_password(user_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Only Admin or IT Support can reset passwords")
    new_pwd = "Welcome@" + secrets.token_hex(3).upper()
    db = get_db()
    try:
        target = db.execute("SELECT u.*, e.name as emp_name, e.id as emp_id FROM users u LEFT JOIN employees e ON e.user_id=u.id WHERE u.id=?", (user_id,)).fetchone()
        if not target: raise HTTPException(404, "User not found")
        db.execute("UPDATE users SET password_hash=?, password_reset_required=1, updated_at=? WHERE id=?",
                   (_hash_pwd(new_pwd), _now(), user_id))
        db.execute("UPDATE sessions SET is_active=0, force_logout=1, logout_reason='Password reset' WHERE user_id=?", (user_id,))
        if target["emp_id"]:
            _notify(target["emp_id"], "Password Reset by IT Support",
                    f"Your password was reset by {user['name']}. New temporary password: {new_pwd} — Change it immediately after login.", "wa")
        db.commit()
        _log(user["id"], user["name"], user["role"], "PASSWORD_RESET", {"target": target["emp_name"]})
        return {"message": f"Password reset for {target['emp_name']}", "new_password": new_pwd}
    finally:
        db.close()

@app.get("/api/users/active-sessions")
def active_sessions(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        rows = db.execute("""SELECT s.id, s.created_at, s.ip_address, s.expires_at,
                             u.email, u.role, u.department, e.name
                             FROM sessions s JOIN users u ON u.id=s.user_id
                             LEFT JOIN employees e ON e.user_id=s.user_id
                             WHERE s.is_active=1 AND s.force_logout=0
                             ORDER BY s.created_at DESC""").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.post("/api/users/sessions/{session_id}/force-logout")
def force_logout(session_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        db.execute("UPDATE sessions SET is_active=0, force_logout=1, logout_reason='Force logout by IT/Admin' WHERE id=?", (session_id,))
        db.commit()
        _log(user["id"], user["name"], user["role"], "FORCE_LOGOUT", {"session": session_id})
        return {"message": "Session terminated"}
    finally:
        db.close()

class NewUserWorkflowReq(BaseModel):
    name: str
    email: str
    role: str
    department: str
    title: str = ""
    salary: float = 0
    phone: str = ""
    reason: str = ""

@app.post("/api/users/request-new-user")
def request_new_user(req: NewUserWorkflowReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["dept_manager", "admin", "manager"]:
        raise HTTPException(403, "Only HR/Managers can request new users")
    db = get_db()
    try:
        if db.execute("SELECT id FROM users WHERE email=?", (req.email.lower(),)).fetchone():
            raise HTTPException(400, "Email already registered")
        req_id = _gen_id()
        db.execute("INSERT INTO user_requests (id,type,status,submitted_by,submitted_by_name,request_data) VALUES (?,?,?,?,?,?)",
                   (req_id, "new_user", "pending_manager", user["id"], user["name"], json.dumps(req.dict())))
        managers = db.execute("SELECT e.id FROM employees e WHERE e.role='manager'").fetchall()
        for m in managers:
            _notify(m["id"], "New Staff Request — Needs Approval",
                    f"{user['name']} wants to add {req.name} as {req.role} in {req.department}. Please review.", "wa")
        db.commit()
        _log(user["id"], user["name"], user["role"], "NEW_USER_REQUESTED", {"name": req.name, "email": req.email})
        return {"message": "Request submitted. Waiting for Manager approval.", "request_id": req_id}
    finally:
        db.close()

class ApprovalReq(BaseModel):
    approved: bool
    note: str = ""

@app.post("/api/users/requests/{req_id}/manager-approve")
def manager_approve(req_id: str, body: ApprovalReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["manager", "admin"]:
        raise HTTPException(403, "Only Managers can approve")
    db = get_db()
    try:
        req = db.execute("SELECT * FROM user_requests WHERE id=? AND status='pending_manager'", (req_id,)).fetchone()
        if not req: raise HTTPException(404, "Request not found or already processed")
        new_status = "pending_it" if body.approved else "rejected"
        db.execute("UPDATE user_requests SET status=?,manager_approval=?,manager_name=?,manager_note=?,manager_approved_at=?,updated_at=? WHERE id=?",
                   (new_status, "approved" if body.approved else "rejected", user["name"], body.note, _now(), _now(), req_id))
        db.commit()
        req_data = json.loads(req["request_data"])
        if body.approved:
            it_team = db.execute("SELECT e.id FROM employees e WHERE e.department='IT Support'").fetchall()
            for it in it_team:
                _notify(it["id"], "New Account to Create — Manager Approved",
                        f"Please create account for {req_data['name']} ({req_data['role']} / {req_data['department']})", "wa")
        else:
            hr_emp = db.execute("SELECT e.id FROM employees e WHERE e.user_id=?", (req["submitted_by"],)).fetchone()
            if hr_emp:
                _notify(hr_emp["id"], "User Request Rejected by Manager",
                        f"Request for {req_data['name']} rejected by {user['name']}. Reason: {body.note or 'No reason given'}", "er")
        return {"message": f"Request {'approved → sent to IT' if body.approved else 'rejected'}"}
    finally:
        db.close()

class ITFinalizeReq(BaseModel):
    approved: bool
    note: str = ""
    initial_password: str = "Welcome@1234"

@app.post("/api/users/requests/{req_id}/it-finalize")
def it_finalize(req_id: str, body: ITFinalizeReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Only IT Support or Admin can finalize accounts")
    err = _check_pwd_strength(body.initial_password)
    if err: raise HTTPException(400, f"Password too weak: {err}")
    db = get_db()
    try:
        req = db.execute("SELECT * FROM user_requests WHERE id=? AND status='pending_it'", (req_id,)).fetchone()
        if not req: raise HTTPException(404, "Request not found or not ready")
        req_data = json.loads(req["request_data"])
        if not body.approved:
            db.execute("UPDATE user_requests SET status='rejected',it_approval='rejected',it_name=?,it_note=?,it_approved_at=?,updated_at=? WHERE id=?",
                       (user["name"], body.note, _now(), _now(), req_id))
            db.commit()
            return {"message": "Request rejected by IT"}
        if db.execute("SELECT id FROM users WHERE email=?", (req_data["email"].lower(),)).fetchone():
            raise HTTPException(400, "Email already registered since request was made")
        uid = _gen_id(); eid = _gen_id()
        db.execute("INSERT INTO users (id,email,password_hash,role,department,is_active,password_reset_required,created_by) VALUES (?,?,?,?,?,1,1,?)",
                   (uid, req_data["email"].lower(), _hash_pwd(body.initial_password), req_data["role"], req_data["department"], user["id"]))
        db.execute("INSERT INTO employees (id,user_id,name,email,department,role,title,salary,phone,join_date,status,color) VALUES (?,?,?,?,?,?,?,?,?,date('now'),'active',?)",
                   (eid, uid, req_data["name"], req_data["email"].lower(), req_data["department"], req_data["role"],
                    req_data.get("title",""), req_data.get("salary",0), req_data.get("phone",""), random.choice(COLORS)))
        db.execute("UPDATE user_requests SET status='completed',it_approval='approved',it_name=?,it_note=?,it_approved_at=?,target_user_id=?,updated_at=? WHERE id=?",
                   (user["name"], body.note, _now(), uid, _now(), req_id))
        hr_emp = db.execute("SELECT e.id FROM employees e WHERE e.user_id=?", (req["submitted_by"],)).fetchone()
        if hr_emp:
            _notify(hr_emp["id"], "Account Created ✅",
                    f"{req_data['name']} account is live. Email: {req_data['email']} | Temp password: {body.initial_password} | They must change password on first login.", "ok")
        db.commit()
        _log(user["id"], user["name"], user["role"], "USER_ACCOUNT_CREATED",
             {"name": req_data["name"], "email": req_data["email"], "role": req_data["role"]})
        return {"message": f"Account created for {req_data['name']}", "user_id": uid,
                "initial_password": body.initial_password}
    finally:
        db.close()

@app.get("/api/users/requests")
def get_requests(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] == "admin":
            rows = db.execute("SELECT * FROM user_requests ORDER BY created_at DESC LIMIT 100").fetchall()
        elif user["role"] == "manager":
            rows = db.execute("SELECT * FROM user_requests WHERE status='pending_manager' ORDER BY created_at DESC").fetchall()
        elif user["role"] == "dept_manager" and user["department"] == "IT Support":
            rows = db.execute("SELECT * FROM user_requests WHERE status='pending_it' ORDER BY created_at DESC").fetchall()
        else:
            rows = db.execute("SELECT * FROM user_requests WHERE submitted_by=? ORDER BY created_at DESC", (user["id"],)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("request_data"):
                try: d["request_data"] = json.loads(d["request_data"])
                except: pass
            result.append(d)
        return result
    finally:
        db.close()

@app.get("/api/employees/")
def get_employees(status: str = None, department: str = None, authorization: str = Header(None)):
    _get_user(authorization)
    db = get_db()
    try:
        q = "SELECT * FROM employees WHERE 1=1"
        params = []
        if status: q += " AND status=?"; params.append(status)
        if department: q += " AND department=?"; params.append(department)
        q += " ORDER BY name"
        return [dict(r) for r in db.execute(q, params).fetchall()]
    finally:
        db.close()

@app.get("/api/employees/me")
def get_me(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Employee not found")
        return dict(emp)
    finally:
        db.close()

class UpdateEmpReq(BaseModel):
    name: Optional[str] = None
    title: Optional[str] = None
    salary: Optional[float] = None
    phone: Optional[str] = None
    department: Optional[str] = None
    status: Optional[str] = None
    emergency_contact: Optional[str] = None
    emergency_phone: Optional[str] = None
    address: Optional[str] = None
    bank_name: Optional[str] = None
    account_number: Optional[str] = None

@app.patch("/api/employees/{emp_id}")
def update_employee(emp_id: str, req: UpdateEmpReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        updates = {k: v for k, v in req.dict().items() if v is not None}
        if not updates: return {"message": "Nothing to update"}
        sets = ", ".join(f"{k}=?" for k in updates)
        vals = list(updates.values()) + [_now(), emp_id]
        db.execute(f"UPDATE employees SET {sets}, updated_at=? WHERE id=?", vals)
        db.commit()
        return {"message": "Updated"}
    finally:
        db.close()

@app.patch("/api/employees/me/profile")
def update_my_profile(req: UpdateEmpReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        allowed = ["phone", "emergency_contact", "emergency_phone", "address"]
        updates = {k: v for k, v in req.dict().items() if v is not None and k in allowed}
        if not updates: return {"message": "Nothing to update"}
        sets = ", ".join(f"{k}=?" for k in updates)
        vals = list(updates.values()) + [_now(), emp["id"]]
        db.execute(f"UPDATE employees SET {sets}, updated_at=? WHERE id=?", vals)
        db.commit()
        return {"message": "Profile updated"}
    finally:
        db.close()

@app.post("/api/attendance/clock-in")
def clock_in(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Employee not found")
        from datetime import timezone, timedelta as td
        wat = timezone(td(hours=1))
        now = datetime.now(wat)
        today = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M")
        existing = db.execute("SELECT * FROM attendance WHERE employee_id=? AND date=?", (emp["id"], today)).fetchone()
        if existing and existing["clock_in"]:
            raise HTTPException(400, "Already clocked in today")
        status = "late" if now.hour >= 9 else "present"
        if existing:
            db.execute("UPDATE attendance SET clock_in=?, status=? WHERE employee_id=? AND date=?", (time_str, status, emp["id"], today))
        else:
            db.execute("INSERT INTO attendance (id,employee_id,employee_name,department,date,clock_in,status) VALUES (?,?,?,?,?,?,?)",
                       (_gen_id(), emp["id"], emp["name"], emp["department"], today, time_str, status))
        db.commit()
        return {"message": f"Clocked in at {time_str}", "status": status}
    finally:
        db.close()

@app.post("/api/attendance/clock-out")
def clock_out(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        from datetime import timezone, timedelta as td
        now = datetime.now(timezone(td(hours=1)))
        today = now.strftime("%Y-%m-%d")
        time_str = now.strftime("%H:%M")
        record = db.execute("SELECT * FROM attendance WHERE employee_id=? AND date=?", (emp["id"], today)).fetchone()
        if not record or not record["clock_in"]: raise HTTPException(400, "Not clocked in today")
        if record["clock_out"]: raise HTTPException(400, "Already clocked out")
        db.execute("UPDATE attendance SET clock_out=? WHERE employee_id=? AND date=?", (time_str, emp["id"], today))
        db.commit()
        return {"message": f"Clocked out at {time_str}"}
    finally:
        db.close()

@app.get("/api/attendance/today")
def today_attendance(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        from datetime import date
        today = date.today().isoformat()
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM attendance WHERE date=? ORDER BY clock_in", (today,)).fetchall()
        else:
            rows = db.execute("SELECT * FROM attendance WHERE date=? AND department=? ORDER BY clock_in", (today, user["department"])).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/attendance/my-history")
def my_attendance(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: return []
        rows = db.execute("SELECT * FROM attendance WHERE employee_id=? ORDER BY date DESC LIMIT 30", (emp["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/attendance/")
def get_all_attendance(department: str = None, date: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        q = "SELECT * FROM attendance WHERE 1=1"
        params = []
        if department: q += " AND department=?"; params.append(department)
        if date: q += " AND date=?"; params.append(date)
        q += " ORDER BY date DESC, clock_in"
        return [dict(r) for r in db.execute(q, params).fetchall()]
    finally:
        db.close()

class LeaveReq(BaseModel):
    type: str
    from_date: str
    to_date: str
    days: int
    reason: str

@app.post("/api/leave/")
def submit_leave(req: LeaveReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Employee not found")
        lid = _gen_id()
        db.execute("INSERT INTO leave_requests (id,employee_id,employee_name,department,type,from_date,to_date,days,reason) VALUES (?,?,?,?,?,?,?,?,?)",
                   (lid, emp["id"], emp["name"], emp["department"], req.type, req.from_date, req.to_date, req.days, req.reason))
        managers = db.execute("SELECT e.id FROM employees e WHERE e.role IN ('manager','admin')").fetchall()
        for m in managers:
            _notify(m["id"], f"Leave Request: {emp['name']}", f"{req.type} leave — {req.from_date} to {req.to_date} ({req.days} days)", "wa")
        db.commit()
        return {"message": "Leave request submitted", "id": lid}
    finally:
        db.close()

@app.get("/api/leave/")
def get_leaves(status: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            q = "SELECT * FROM leave_requests"; params = []
            if status: q += " WHERE status=?"; params.append(status)
        elif user["role"] == "dept_manager":
            q = "SELECT * FROM leave_requests WHERE department=?"; params = [user["department"]]
            if status: q += " AND status=?"; params.append(status)
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            q = "SELECT * FROM leave_requests WHERE employee_id=?"; params = [emp["id"] if emp else ""]
        return [dict(r) for r in db.execute(q + " ORDER BY created_at DESC", params).fetchall()]
    finally:
        db.close()

class LeaveAction(BaseModel):
    approved: bool
    note: str = ""

@app.post("/api/leave/{leave_id}/action")
def action_leave(leave_id: str, body: LeaveAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        leave = db.execute("SELECT * FROM leave_requests WHERE id=?", (leave_id,)).fetchone()
        if not leave: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE leave_requests SET status=?,approved_by=?,approver_name=?,rejected_reason=? WHERE id=?",
                   (status, user["id"], user["name"], body.note if not body.approved else None, leave_id))
        _notify(leave["employee_id"], f"Leave {'Approved ✅' if body.approved else 'Rejected ❌'}",
                f"Your {leave['type']} leave {'was approved' if body.approved else f'was rejected. Reason: {body.note}'}", "ok" if body.approved else "er")
        db.commit()
        return {"message": f"Leave {status}"}
    finally:
        db.close()

class BudgetReq(BaseModel):
    title: str
    amount: float
    reason: str
    category: str = "Operational"
    priority: str = "medium"
    department: str = ""

@app.post("/api/budgets/")
def submit_budget(req: BudgetReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["dept_manager", "manager", "admin"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        bid = _gen_id()
        dept = req.department or user["department"]
        db.execute("INSERT INTO budget_requests (id,department,title,amount,reason,submitted_by,submitter_name) VALUES (?,?,?,?,?,?,?)",
                   (bid, dept, req.title, req.amount, req.reason, user["id"], user["name"]))
        admins = db.execute("SELECT e.id FROM employees e WHERE e.role='admin'").fetchall()
        for a in admins:
            _notify(a["id"], f"Budget Request: {user['department']}", f"{user['name']} requests ₦{req.amount:,.0f} for {req.title}", "wa")
        db.commit()
        return {"message": "Budget request submitted", "id": bid}
    finally:
        db.close()

@app.get("/api/budgets/")
def get_budgets(status: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            q = "SELECT * FROM budget_requests"; params = []
            if status: q += " WHERE status=?"; params.append(status)
        else:
            q = "SELECT * FROM budget_requests WHERE submitted_by=?"; params = [user["id"]]
        return [dict(r) for r in db.execute(q + " ORDER BY created_at DESC", params).fetchall()]
    finally:
        db.close()

class BudgetAction(BaseModel):
    approved: bool
    note: str = ""

@app.post("/api/budgets/{bid}/action")
def action_budget(bid: str, body: BudgetAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"]:
        raise HTTPException(403, "Only Admin or Manager can approve budgets")
    db = get_db()
    try:
        budget = db.execute("SELECT * FROM budget_requests WHERE id=?", (bid,)).fetchone()
        if not budget: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE budget_requests SET status=?,approved_by=?,rejection_note=? WHERE id=?",
                   (status, user["id"], body.note if not body.approved else None, bid))
        sub_emp = db.execute("SELECT e.id FROM employees e WHERE e.user_id=?", (budget["submitted_by"],)).fetchone()
        if sub_emp:
            _notify(sub_emp["id"], f"Budget {'Approved ✅' if body.approved else 'Rejected ❌'}",
                    f"Your ₦{budget['amount']:,.0f} budget for {budget['department']} was {status}",
                    "ok" if body.approved else "er")
        if body.approved:
            fin = db.execute("SELECT e.id FROM employees e WHERE e.department='Finance' AND e.role='dept_manager'").fetchone()
            if fin:
                _notify(fin["id"], "Budget Approved — Allocate Funds",
                        f"Admin approved ₦{budget['amount']:,.0f} for {budget['department']}. Please process transfer.", "in")
        db.commit()
        return {"message": f"Budget {status}"}
    finally:
        db.close()

class AnnReq(BaseModel):
    title: str
    body: str
    type: str = "info"
    department: str = "all"

@app.post("/api/announcements/")
def create_ann(req: AnnReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        aid = _gen_id()
        db.execute("INSERT INTO announcements (id,title,body,type,department,posted_by,poster_name) VALUES (?,?,?,?,?,?,?)",
                   (aid, req.title, req.body, req.type, req.department, user["id"], user["name"]))
        db.commit()
        return {"message": "Announcement posted", "id": aid}
    finally:
        db.close()

@app.delete("/api/announcements/{ann_id}")
def delete_ann(ann_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        db.execute("DELETE FROM announcements WHERE id=?", (ann_id,))
        db.commit()
        return {"message": "Announcement deleted"}
    finally:
        db.close()

@app.get("/api/announcements/")
def get_anns(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM announcements WHERE department='all' OR department=? ORDER BY created_at DESC LIMIT 50",
                          (user["department"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/notifications/")
def get_notifs(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: return []
        rows = db.execute("SELECT * FROM notifications WHERE employee_id=? ORDER BY created_at DESC LIMIT 50", (emp["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/notifications/unread-count")
def unread_count(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: return {"count": 0}
        count = db.execute("SELECT COUNT(*) FROM notifications WHERE employee_id=? AND read=0", (emp["id"],)).fetchone()[0]
        return {"count": count}
    finally:
        db.close()

@app.post("/api/notifications/{notif_id}/read")
def mark_read(notif_id: str, authorization: str = Header(None)):
    _get_user(authorization)
    db = get_db()
    try:
        db.execute("UPDATE notifications SET read=1 WHERE id=?", (notif_id,))
        db.commit()
        return {"message": "Marked read"}
    finally:
        db.close()

@app.post("/api/notifications/mark-all-read")
def mark_all_read(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if emp:
            db.execute("UPDATE notifications SET read=1 WHERE employee_id=?", (emp["id"],))
            db.commit()
        return {"message": "All marked read"}
    finally:
        db.close()

class TicketReq(BaseModel):
    subject: str
    description: str
    priority: str = "medium"

@app.post("/api/it/tickets")
def create_ticket(req: TicketReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        tid = _gen_id()
        db.execute("INSERT INTO it_tickets (id,subject,description,submitted_by,submitter_name,department,priority) VALUES (?,?,?,?,?,?,?)",
                   (tid, req.subject, req.description, user["id"], user["name"], user["department"], req.priority))
        it_staff = db.execute("SELECT e.id FROM employees e WHERE e.department='IT Support'").fetchall()
        for it in it_staff:
            _notify(it["id"], f"New IT Ticket [{req.priority.upper()}]: {req.subject}",
                    f"From: {user['name']} ({user['department']})", "wa")
        db.commit()
        return {"message": "Ticket submitted", "ticket_id": tid}
    finally:
        db.close()

@app.get("/api/it/tickets")
def get_tickets(status: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin"] or (user["role"] == "dept_manager" and user["department"] == "IT Support"):
            q = "SELECT * FROM it_tickets"; params = []
            if status: q += " WHERE status=?"; params.append(status)
        else:
            q = "SELECT * FROM it_tickets WHERE submitted_by=?"; params = [user["id"]]
        return [dict(r) for r in db.execute(q + " ORDER BY created_at DESC", params).fetchall()]
    finally:
        db.close()

class TicketUpdate(BaseModel):
    status: str
    resolution: str = ""
    assigned_name: str = ""

@app.patch("/api/it/tickets/{ticket_id}")
def update_ticket(ticket_id: str, req: TicketUpdate, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Only IT Support can update tickets")
    db = get_db()
    try:
        ticket = db.execute("SELECT * FROM it_tickets WHERE id=?", (ticket_id,)).fetchone()
        if not ticket: raise HTTPException(404, "Not found")
        resolved_at = _now() if req.status == "resolved" else None
        db.execute("UPDATE it_tickets SET status=?,resolution=?,assigned_name=?,resolved_at=?,updated_at=? WHERE id=?",
                   (req.status, req.resolution, req.assigned_name, resolved_at, _now(), ticket_id))
        if req.status == "resolved":
            sub = db.execute("SELECT e.id FROM employees e WHERE e.user_id=?", (ticket["submitted_by"],)).fetchone()
            if sub:
                _notify(sub["id"], f"IT Ticket Resolved: {ticket['subject']}", f"Resolution: {req.resolution}", "ok")
        db.commit()
        return {"message": "Ticket updated"}
    finally:
        db.close()

@app.get("/api/it/logs")
def get_logs(limit: int = 100, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM activity_log ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/it/system-health")
def system_health(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] != "admin" and not (user["role"] == "dept_manager" and user["department"] == "IT Support"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        from datetime import date
        today = date.today().isoformat()
        return {
            "total_users": db.execute("SELECT COUNT(*) FROM users").fetchone()[0],
            "active_users": db.execute("SELECT COUNT(*) FROM users WHERE is_active=1 AND is_suspended=0").fetchone()[0],
            "suspended_users": db.execute("SELECT COUNT(*) FROM users WHERE is_suspended=1").fetchone()[0],
            "active_sessions": db.execute("SELECT COUNT(*) FROM sessions WHERE is_active=1").fetchone()[0],
            "open_tickets": db.execute("SELECT COUNT(*) FROM it_tickets WHERE status='open'").fetchone()[0],
            "total_employees": db.execute("SELECT COUNT(*) FROM employees WHERE status='active'").fetchone()[0],
            "pending_user_requests": db.execute("SELECT COUNT(*) FROM user_requests WHERE status IN ('pending_manager','pending_it')").fetchone()[0],
            "today_logins": db.execute("SELECT COUNT(*) FROM activity_log WHERE action='LOGIN_SUCCESS' AND created_at >= ?", (today,)).fetchone()[0],
            "today_failed_logins": db.execute("SELECT COUNT(*) FROM activity_log WHERE action='LOGIN_FAILED' AND created_at >= ?", (today,)).fetchone()[0],
            "db_size_kb": round(os.path.getsize(DB_PATH) / 1024, 2) if os.path.exists(DB_PATH) else 0,
            "status": "healthy"
        }
    finally:
        db.close()

@app.get("/api/departments/")
def get_depts(authorization: str = Header(None)):
    _get_user(authorization)
    db = get_db()
    try:
        return [dict(r) for r in db.execute("SELECT * FROM departments ORDER BY name").fetchall()]
    finally:
        db.close()

@app.get("/api/reports/dashboard-stats")
def dashboard_stats(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        from datetime import date
        today = date.today().isoformat()
        return {
            "total_employees": db.execute("SELECT COUNT(*) FROM employees WHERE status='active'").fetchone()[0],
            "pending_leave": db.execute("SELECT COUNT(*) FROM leave_requests WHERE status='pending'").fetchone()[0],
            "pending_budgets": db.execute("SELECT COUNT(*) FROM budget_requests WHERE status='pending'").fetchone()[0],
            "open_tickets": db.execute("SELECT COUNT(*) FROM it_tickets WHERE status='open'").fetchone()[0],
            "today_present": db.execute("SELECT COUNT(*) FROM attendance WHERE date=? AND status='present'", (today,)).fetchone()[0],
            "today_late": db.execute("SELECT COUNT(*) FROM attendance WHERE date=? AND status='late'", (today,)).fetchone()[0],
            "suspended_users": db.execute("SELECT COUNT(*) FROM users WHERE is_suspended=1").fetchone()[0],
            "pending_user_requests": db.execute("SELECT COUNT(*) FROM user_requests WHERE status IN ('pending_manager','pending_it')").fetchone()[0],
            "open_complaints": db.execute("SELECT COUNT(*) FROM complaints WHERE status='open'").fetchone()[0],
            "pending_expense_claims": db.execute("SELECT COUNT(*) FROM expense_claims WHERE status='pending'").fetchone()[0],
            "pending_overtime": db.execute("SELECT COUNT(*) FROM overtime_requests WHERE status='pending'").fetchone()[0],
        }
    finally:
        db.close()

@app.get("/api/reports/payroll-summary")
def payroll_summary(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        emps = db.execute("SELECT salary, department FROM employees WHERE status='active'").fetchall()
        total = sum(e["salary"] or 0 for e in emps)
        return {
            "total_employees": len(emps),
            "gross_payroll": total,
            "tax": round(total * 0.20),
            "ni": round(total * 0.08),
            "pension": round(total * 0.05),
            "net_payroll": round(total * 0.67)
        }
    finally:
        db.close()

class MsgReq(BaseModel):
    room_id: str
    content: str

@app.post("/api/messages/")
def send_msg(req: MsgReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        mid = _gen_id()
        db.execute("INSERT INTO messages (id,room_id,sender_id,sender_name,content) VALUES (?,?,?,?,?)",
                   (mid, req.room_id, user["id"], user["name"], req.content))
        db.commit()
        return {"message": "Sent", "id": mid}
    finally:
        db.close()

@app.get("/api/messages/{room_id}")
def get_msgs(room_id: str, authorization: str = Header(None)):
    _get_user(authorization)
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM messages WHERE room_id=? ORDER BY created_at ASC LIMIT 50", (room_id,)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class TaskReq(BaseModel):
    title: str
    description: str = ""
    assigned_to: str
    assigned_to_name: str
    department: str
    due_date: str
    priority: str = "medium"

@app.post("/api/tasks/")
def create_task(req: TaskReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        tid = _gen_id()
        db.execute("INSERT INTO tasks (id,title,description,assigned_to,assigned_to_name,assigned_by,assigned_by_name,department,due_date,priority) VALUES (?,?,?,?,?,?,?,?,?,?)",
                   (tid, req.title, req.description, req.assigned_to, req.assigned_to_name,
                    user["id"], user["name"], req.department, req.due_date, req.priority))
        _notify(req.assigned_to, f"New Task: {req.title}",
                f"Assigned by {user['name']} — Due: {req.due_date} — Priority: {req.priority}", "in")
        db.commit()
        return {"message": "Task created", "id": tid}
    finally:
        db.close()

@app.get("/api/tasks/")
def get_tasks(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: return []
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM tasks ORDER BY due_date ASC").fetchall()
        elif user["role"] == "dept_manager":
            rows = db.execute("SELECT * FROM tasks WHERE department=? OR assigned_by=? ORDER BY due_date", (user["department"], user["id"])).fetchall()
        else:
            rows = db.execute("SELECT * FROM tasks WHERE assigned_to=? ORDER BY due_date", (emp["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class TaskUpdate(BaseModel):
    status: str
    progress_note: str = ""

@app.patch("/api/tasks/{task_id}")
def update_task(task_id: str, req: TaskUpdate, authorization: str = Header(None)):
    _get_user(authorization)
    db = get_db()
    try:
        db.execute("UPDATE tasks SET status=?,progress_note=?,updated_at=? WHERE id=?",
                   (req.status, req.progress_note, _now(), task_id))
        db.commit()
        return {"message": "Task updated"}
    finally:
        db.close()

class PayrollRunReq(BaseModel):
    period: str
    paid_date: str
    notes: str = ""

@app.post("/api/payroll/run")
def run_payroll(req: PayrollRunReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "Payroll"):
        raise HTTPException(403, "Not authorized to run payroll")
    db = get_db()
    try:
        existing = db.execute("SELECT id FROM payroll_records WHERE period=?", (req.period,)).fetchone()
        if existing: raise HTTPException(400, f"Payroll for {req.period} already exists")
        emps = db.execute("SELECT * FROM employees WHERE status='active'").fetchall()
        for e in emps:
            g = e["salary"] or 0
            tax = round(g * 0.20)
            ni = round(g * 0.08)
            pen = round(g * 0.05)
            net = g - tax - ni - pen
            db.execute("INSERT INTO payroll_records (id,employee_id,period,gross,tax,ni,pension,net,status,paid_date) VALUES (?,?,?,?,?,?,?,?,?,?)",
                       (_gen_id(), e["id"], req.period, g, tax, ni, pen, net, "paid", req.paid_date))
            _notify(e["id"], f"Payslip Ready: {req.period}",
                    f"Your net pay of ₦{net:,.0f} for {req.period} has been processed.", "ok")
        db.commit()
        _log(user["id"], user["name"], user["role"], "PAYROLL_RUN", {"period": req.period, "employees": len(emps)})
        return {"message": f"Payroll run complete for {req.period}", "employees_paid": len(emps)}
    finally:
        db.close()

@app.get("/api/payroll/records")
def get_payroll_records(period: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"] or (user["role"] == "dept_manager" and user["department"] in ["Payroll", "Finance"]):
            q = "SELECT pr.*, e.name as emp_name, e.department FROM payroll_records pr LEFT JOIN employees e ON e.id=pr.employee_id"
            params = []
            if period: q += " WHERE pr.period=?"; params.append(period)
            q += " ORDER BY pr.created_at DESC"
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            if not emp: return []
            q = "SELECT pr.*, e.name as emp_name, e.department FROM payroll_records pr LEFT JOIN employees e ON e.id=pr.employee_id WHERE pr.employee_id=?"
            params = [emp["id"]]
            if period: q += " AND pr.period=?"; params.append(period)
            q += " ORDER BY pr.created_at DESC"
        return [dict(r) for r in db.execute(q, params).fetchall()]
    finally:
        db.close()

@app.get("/api/payroll/my-slip")
def my_payslip(period: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        q = "SELECT * FROM payroll_records WHERE employee_id=?"
        params = [emp["id"]]
        if period: q += " AND period=?"; params.append(period)
        q += " ORDER BY created_at DESC LIMIT 1"
        rec = db.execute(q, params).fetchone()
        if not rec: raise HTTPException(404, "No payslip found for this period")
        return {**dict(rec), "employee": dict(emp)}
    finally:
        db.close()

class ComplaintReq(BaseModel):
    subject: str
    detail: str
    against_name: str = ""
    severity: str = "medium"

@app.post("/api/complaints/")
def file_complaint(req: ComplaintReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        cid = _gen_id()
        db.execute("INSERT INTO complaints (id,subject,detail,filer_id,filer_name,against_name,department,severity) VALUES (?,?,?,?,?,?,?,?)",
                   (cid, req.subject, req.detail, emp["id"], emp["name"], req.against_name, emp["department"], req.severity))
        hr = db.execute("SELECT e.id FROM employees e WHERE e.department='HR'").fetchall()
        for h in hr:
            _notify(h["id"], f"New Complaint [{req.severity.upper()}]: {req.subject}",
                    f"Filed by {emp['name']} in {emp['department']}", "er" if req.severity == "high" else "wa")
        db.commit()
        return {"message": "Complaint filed successfully", "id": cid}
    finally:
        db.close()

@app.get("/api/complaints/")
def get_complaints(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"] or (user["role"] == "dept_manager" and user["department"] == "HR"):
            rows = db.execute("SELECT * FROM complaints ORDER BY created_at DESC").fetchall()
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            rows = db.execute("SELECT * FROM complaints WHERE filer_id=? ORDER BY created_at DESC", (emp["id"] if emp else "",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class ComplaintUpdate(BaseModel):
    status: str
    hr_note: str = ""

@app.patch("/api/complaints/{cid}")
def update_complaint(cid: str, req: ComplaintUpdate, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "HR"):
        raise HTTPException(403, "Only HR can update complaints")
    db = get_db()
    try:
        resolved_at = _now() if req.status == "resolved" else None
        db.execute("UPDATE complaints SET status=?,hr_note=?,resolved_at=? WHERE id=?",
                   (req.status, req.hr_note, resolved_at, cid))
        db.commit()
        return {"message": "Complaint updated"}
    finally:
        db.close()

class ApplicationReq(BaseModel):
    full_name: str
    email: str
    phone: str = ""
    role_applied: str
    department: str
    cover_letter: str = ""

@app.post("/api/applications/")
def submit_application(req: ApplicationReq):
    db = get_db()
    try:
        aid = _gen_id()
        db.execute("INSERT INTO job_applications (id,full_name,email,phone,role_applied,department,cover_letter) VALUES (?,?,?,?,?,?,?)",
                   (aid, req.full_name, req.email, req.phone, req.role_applied, req.department, req.cover_letter))
        hr = db.execute("SELECT e.id FROM employees e WHERE e.department='HR'").fetchall()
        for h in hr:
            _notify(h["id"], f"New Job Application: {req.role_applied}",
                    f"{req.full_name} applied for {req.role_applied} in {req.department}", "in")
        db.commit()
        return {"message": "Application received", "id": aid}
    finally:
        db.close()

@app.get("/api/applications/")
def get_applications(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] in ["HR", "Recruitment"]):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM job_applications ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class ApplicationUpdate(BaseModel):
    status: str
    reviewer_name: str = ""
    interview_date: str = ""
    interview_notes: str = ""

@app.patch("/api/applications/{aid}")
def update_application(aid: str, req: ApplicationUpdate, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] in ["HR", "Recruitment"]):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        db.execute("UPDATE job_applications SET status=?,reviewer_name=?,interview_date=?,interview_notes=? WHERE id=?",
                   (req.status, req.reviewer_name or user["name"], req.interview_date, req.interview_notes, aid))
        db.commit()
        return {"message": "Application updated"}
    finally:
        db.close()

class SupplyReq(BaseModel):
    item_name: str
    quantity: int = 1
    reason: str = ""
    urgency: str = "Normal"

@app.post("/api/supply-requests/")
def submit_supply(req: SupplyReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        sid = _gen_id()
        db.execute("INSERT INTO supply_requests (id,item_name,quantity,reason,department,requester_name,requester_id,urgency) VALUES (?,?,?,?,?,?,?,?)",
                   (sid, req.item_name, req.quantity, req.reason, user["department"], user["name"], user["id"], req.urgency))
        managers = db.execute("SELECT e.id FROM employees e WHERE e.role IN ('admin','manager','dept_manager') AND e.department=?", (user["department"],)).fetchall()
        for m in managers:
            _notify(m["id"], f"Supply Request: {req.item_name}",
                    f"{user['name']} requests {req.quantity}x {req.item_name} [{req.urgency}]", "in")
        db.commit()
        return {"message": "Supply request submitted", "id": sid}
    finally:
        db.close()

@app.get("/api/supply-requests/")
def get_supply_requests(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM supply_requests ORDER BY created_at DESC").fetchall()
        elif user["role"] == "dept_manager":
            rows = db.execute("SELECT * FROM supply_requests WHERE department=? ORDER BY created_at DESC", (user["department"],)).fetchall()
        else:
            rows = db.execute("SELECT * FROM supply_requests WHERE requester_id=? ORDER BY created_at DESC", (user["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class SupplyAction(BaseModel):
    approved: bool
    rejection_note: str = ""

@app.post("/api/supply-requests/{sid}/action")
def action_supply(sid: str, body: SupplyAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        sr = db.execute("SELECT * FROM supply_requests WHERE id=?", (sid,)).fetchone()
        if not sr: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE supply_requests SET status=?,approved_by=?,rejection_note=? WHERE id=?",
                   (status, user["name"], body.rejection_note if not body.approved else None, sid))
        requester_emp = db.execute("SELECT e.id FROM employees e WHERE e.user_id=?", (sr["requester_id"],)).fetchone()
        if requester_emp:
            _notify(requester_emp["id"], f"Supply Request {'Approved ✅' if body.approved else 'Rejected ❌'}",
                    f"Your request for {sr['item_name']} was {status}", "ok" if body.approved else "er")
        db.commit()
        return {"message": f"Supply request {status}"}
    finally:
        db.close()

class PerformanceReviewReq(BaseModel):
    employee_id: str
    period: str
    punctuality: int = 0
    teamwork: int = 0
    productivity: int = 0
    communication: int = 0
    comments: str = ""

@app.post("/api/performance/")
def create_review(req: PerformanceReviewReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Only managers can create performance reviews")
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE id=?", (req.employee_id,)).fetchone()
        if not emp: raise HTTPException(404, "Employee not found")
        score = round((req.punctuality + req.teamwork + req.productivity + req.communication) / 4)
        rid = _gen_id()
        db.execute("INSERT INTO performance_reviews (id,employee_id,employee_name,department,reviewer_id,reviewer_name,period,score,punctuality,teamwork,productivity,communication,comments,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                   (rid, req.employee_id, emp["name"], emp["department"], user["id"], user["name"],
                    req.period, score, req.punctuality, req.teamwork, req.productivity, req.communication, req.comments, "published"))
        _notify(emp["id"], f"Performance Review: {req.period}",
                f"Your performance review for {req.period} is ready. Score: {score}/10", "in")
        db.commit()
        return {"message": "Performance review saved", "id": rid, "score": score}
    finally:
        db.close()

@app.get("/api/performance/")
def get_reviews(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM performance_reviews ORDER BY created_at DESC").fetchall()
        elif user["role"] == "dept_manager":
            rows = db.execute("SELECT * FROM performance_reviews WHERE department=? ORDER BY created_at DESC", (user["department"],)).fetchall()
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            rows = db.execute("SELECT * FROM performance_reviews WHERE employee_id=? ORDER BY created_at DESC", (emp["id"] if emp else "",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class OvertimeReq(BaseModel):
    date: str
    hours: float
    reason: str

@app.post("/api/overtime/")
def submit_overtime(req: OvertimeReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        oid = _gen_id()
        db.execute("INSERT INTO overtime_requests (id,employee_id,employee_name,department,date,hours,reason) VALUES (?,?,?,?,?,?,?)",
                   (oid, emp["id"], emp["name"], emp["department"], req.date, req.hours, req.reason))
        managers = db.execute("SELECT e.id FROM employees e WHERE e.role IN ('manager','admin')").fetchall()
        for m in managers:
            _notify(m["id"], f"Overtime Request: {emp['name']}",
                    f"{req.hours}hrs on {req.date}. Reason: {req.reason}", "wa")
        db.commit()
        return {"message": "Overtime request submitted", "id": oid}
    finally:
        db.close()

@app.get("/api/overtime/")
def get_overtime(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM overtime_requests ORDER BY created_at DESC").fetchall()
        elif user["role"] == "dept_manager":
            rows = db.execute("SELECT * FROM overtime_requests WHERE department=? ORDER BY created_at DESC", (user["department"],)).fetchall()
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            rows = db.execute("SELECT * FROM overtime_requests WHERE employee_id=? ORDER BY created_at DESC", (emp["id"] if emp else "",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class OvertimeAction(BaseModel):
    approved: bool
    rejection_note: str = ""

@app.post("/api/overtime/{oid}/action")
def action_overtime(oid: str, body: OvertimeAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        ot = db.execute("SELECT * FROM overtime_requests WHERE id=?", (oid,)).fetchone()
        if not ot: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE overtime_requests SET status=?,approved_by=?,approver_name=?,rejection_note=? WHERE id=?",
                   (status, user["id"], user["name"], body.rejection_note if not body.approved else None, oid))
        _notify(ot["employee_id"], f"Overtime {'Approved ✅' if body.approved else 'Rejected ❌'}",
                f"Your overtime of {ot['hours']}hrs on {ot['date']} was {status}", "ok" if body.approved else "er")
        db.commit()
        return {"message": f"Overtime {status}"}
    finally:
        db.close()

class ExpenseClaimReq(BaseModel):
    title: str
    amount: float
    category: str = "other"
    description: str = ""
    receipt_note: str = ""

@app.post("/api/expenses/")
def submit_expense(req: ExpenseClaimReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: raise HTTPException(404, "Not found")
        eid = _gen_id()
        db.execute("INSERT INTO expense_claims (id,employee_id,employee_name,department,title,amount,category,description,receipt_note) VALUES (?,?,?,?,?,?,?,?,?)",
                   (eid, emp["id"], emp["name"], emp["department"], req.title, req.amount, req.category, req.description, req.receipt_note))
        managers = db.execute("SELECT e.id FROM employees e WHERE e.role IN ('manager','admin')").fetchall()
        for m in managers:
            _notify(m["id"], f"Expense Claim: {emp['name']}",
                    f"₦{req.amount:,.0f} for {req.title} ({req.category})", "wa")
        db.commit()
        return {"message": "Expense claim submitted", "id": eid}
    finally:
        db.close()

@app.get("/api/expenses/")
def get_expenses(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"]:
            rows = db.execute("SELECT * FROM expense_claims ORDER BY created_at DESC").fetchall()
        elif user["role"] == "dept_manager":
            rows = db.execute("SELECT * FROM expense_claims WHERE department=? ORDER BY created_at DESC", (user["department"],)).fetchall()
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            rows = db.execute("SELECT * FROM expense_claims WHERE employee_id=? ORDER BY created_at DESC", (emp["id"] if emp else "",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class ExpenseAction(BaseModel):
    approved: bool
    rejection_note: str = ""

@app.post("/api/expenses/{eid}/action")
def action_expense(eid: str, body: ExpenseAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager", "dept_manager"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        exp = db.execute("SELECT * FROM expense_claims WHERE id=?", (eid,)).fetchone()
        if not exp: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE expense_claims SET status=?,approved_by=?,approver_name=?,rejection_note=? WHERE id=?",
                   (status, user["id"], user["name"], body.rejection_note if not body.approved else None, eid))
        _notify(exp["employee_id"], f"Expense Claim {'Approved ✅' if body.approved else 'Rejected ❌'}",
                f"Your ₦{exp['amount']:,.0f} claim for {exp['title']} was {status}", "ok" if body.approved else "er")
        db.commit()
        return {"message": f"Expense claim {status}"}
    finally:
        db.close()

class DisciplinaryReq(BaseModel):
    employee_id: str
    type: str
    description: str
    action_taken: str = ""
    severity: str = "warning"

@app.post("/api/disciplinary/")
def create_disciplinary(req: DisciplinaryReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "HR"):
        raise HTTPException(403, "Only HR or Managers can issue disciplinary records")
    db = get_db()
    try:
        emp = db.execute("SELECT * FROM employees WHERE id=?", (req.employee_id,)).fetchone()
        if not emp: raise HTTPException(404, "Employee not found")
        did = _gen_id()
        db.execute("INSERT INTO disciplinary_records (id,employee_id,employee_name,department,type,description,action_taken,issued_by,issuer_name,severity) VALUES (?,?,?,?,?,?,?,?,?,?)",
                   (did, req.employee_id, emp["name"], emp["department"], req.type, req.description, req.action_taken, user["id"], user["name"], req.severity))
        _notify(emp["id"], f"Disciplinary Notice: {req.type}",
                f"A {req.severity} has been issued. Please check HR for details.", "er")
        db.commit()
        return {"message": "Disciplinary record created", "id": did}
    finally:
        db.close()

@app.get("/api/disciplinary/")
def get_disciplinary(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"] or (user["role"] == "dept_manager" and user["department"] == "HR"):
            rows = db.execute("SELECT * FROM disciplinary_records ORDER BY created_at DESC").fetchall()
        else:
            emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
            rows = db.execute("SELECT * FROM disciplinary_records WHERE employee_id=? ORDER BY created_at DESC", (emp["id"] if emp else "",)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/fund-transfers/")
def get_transfers(authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "Finance"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM fund_transfers ORDER BY created_at DESC").fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class TransferReq(BaseModel):
    to_dept: str
    amount: float
    note: str = ""
    type: str = "transfer"

@app.post("/api/fund-transfers/")
def create_transfer(req: TransferReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "Finance"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        ref = "TXN-" + secrets.token_hex(4).upper()
        tid = _gen_id()
        db.execute("INSERT INTO fund_transfers (id,to_dept,amount,note,reference,sent_by,sent_by_name,status) VALUES (?,?,?,?,?,?,?,?)",
                   (tid, req.to_dept, req.amount, req.note, ref, user["id"], user["name"], "completed"))
        db.execute("UPDATE departments SET spent=spent+? WHERE name=?", (req.amount, req.to_dept))
        db.commit()
        _log(user["id"], user["name"], user["role"], "FUND_TRANSFER", {"to": req.to_dept, "amount": req.amount, "ref": ref})
        return {"message": "Transfer completed", "reference": ref, "id": tid}
    finally:
        db.close()

@app.get("/api/spending-requests/")
def get_spending(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        if user["role"] in ["admin", "manager"] or (user["role"] == "dept_manager" and user["department"] == "Finance"):
            rows = db.execute("SELECT * FROM spending_requests ORDER BY created_at DESC").fetchall()
        else:
            rows = db.execute("SELECT * FROM spending_requests WHERE submitted_by=? ORDER BY created_at DESC", (user["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class SpendingReq(BaseModel):
    title: str
    amount: float
    vendor: str = ""
    reason: str = ""

@app.post("/api/spending-requests/")
def submit_spending(req: SpendingReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["dept_manager", "manager", "admin"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        sid = _gen_id()
        db.execute("INSERT INTO spending_requests (id,department,title,amount,vendor,reason,submitted_by,submitter_name) VALUES (?,?,?,?,?,?,?,?)",
                   (sid, user["department"], req.title, req.amount, req.vendor, req.reason, user["id"], user["name"]))
        db.commit()
        return {"message": "Spending request submitted", "id": sid}
    finally:
        db.close()

class SpendingAction(BaseModel):
    approved: bool
    note: str = ""

@app.post("/api/spending-requests/{sid}/action")
def action_spending(sid: str, body: SpendingAction, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["admin", "manager"] and not (user["role"] == "dept_manager" and user["department"] == "Finance"):
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        sr = db.execute("SELECT * FROM spending_requests WHERE id=?", (sid,)).fetchone()
        if not sr: raise HTTPException(404, "Not found")
        status = "approved" if body.approved else "rejected"
        db.execute("UPDATE spending_requests SET status=?,approved_by=?,rejection_note=? WHERE id=?",
                   (status, user["id"], body.note if not body.approved else None, sid))
        db.commit()
        return {"message": f"Spending request {status}"}
    finally:
        db.close()

@app.get("/unlock")
def unlock_admin():
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("UPDATE users SET login_attempts=0, locked_until=NULL, is_suspended=0, password_hash=?, is_active=1 WHERE email=?",
                   (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        user = db.execute("SELECT email, role, login_attempts, locked_until, is_active FROM users WHERE email=?",
                         ("admin@giddiesexpress.com",)).fetchone()
        return {"message": "Admin unlocked! Password reset to Admin@1234", "email": user["email"],
                "role": user["role"], "attempts": user["login_attempts"], "locked": user["locked_until"], "active": user["is_active"]}
    finally:
        db.close()

@app.get("/api/fix-admin")
def fix_admin():
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("UPDATE users SET password_hash=?, login_attempts=0, locked_until=NULL, is_suspended=0 WHERE email=?",
                   (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        user = db.execute("SELECT email, role, is_active FROM users WHERE email=?", ("admin@giddiesexpress.com",)).fetchone()
        if not user:
            init_db()
            db.execute("UPDATE users SET password_hash=? WHERE email=?", (_hash_pwd("Admin@1234"), "admin@giddiesexpress.com"))
            db.commit()
            return {"message": "Admin created and fixed!", "status": "created"}
        return {"message": "Fixed!", "email": user["email"], "role": user["role"], "active": user["is_active"]}
    finally:
        db.close()

@app.get("/fix")
def fix_simple():
    return fix_admin()

@app.get("/api/reset-db")
def reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("UPDATE users SET password_hash=? WHERE email=?", (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        return {"message": "Database reset! Admin password is Admin@1234", "users": count}
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)


import base64 as _b64

CHAT_TABLES = """
CREATE TABLE IF NOT EXISTS chat_conversations (
    id TEXT PRIMARY KEY,
    type TEXT DEFAULT 'direct',
    name TEXT,
    created_by TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS chat_members (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    employee_id TEXT NOT NULL,
    employee_name TEXT,
    joined_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS chat_messages (
    id TEXT PRIMARY KEY,
    conversation_id TEXT NOT NULL REFERENCES chat_conversations(id) ON DELETE CASCADE,
    sender_id TEXT NOT NULL,
    sender_name TEXT,
    sender_color TEXT DEFAULT '#FF6B00',
    content TEXT,
    msg_type TEXT DEFAULT 'text',
    file_name TEXT,
    file_size INTEGER,
    file_data TEXT,
    file_mime TEXT,
    is_deleted INTEGER DEFAULT 0,
    created_at TEXT DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS mail_messages (
    id TEXT PRIMARY KEY,
    from_id TEXT NOT NULL,
    from_name TEXT,
    to_id TEXT NOT NULL,
    to_name TEXT,
    subject TEXT NOT NULL,
    body TEXT,
    msg_type TEXT DEFAULT 'mail',
    file_name TEXT,
    file_size INTEGER,
    file_data TEXT,
    file_mime TEXT,
    is_read INTEGER DEFAULT 0,
    is_starred INTEGER DEFAULT 0,
    is_deleted_sender INTEGER DEFAULT 0,
    is_deleted_recipient INTEGER DEFAULT 0,
    reply_to TEXT,
    created_at TEXT DEFAULT (datetime('now'))
);
"""

def _ensure_chat_tables():
    db = get_db()
    try:
        db.executescript(CHAT_TABLES)
        db.commit()
    finally:
        db.close()

_ensure_chat_tables()

class ConvoReq(BaseModel):
    type: str = "direct"
    name: str = ""
    member_ids: list

@app.post("/api/chat/conversations")
def create_convo(req: ConvoReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            raise HTTPException(404, "Employee not found")
        if req.type == "direct" and len(req.member_ids) == 1:
            other_id = req.member_ids[0]
            existing = db.execute("""
                SELECT cm1.conversation_id FROM chat_members cm1
                JOIN chat_members cm2 ON cm1.conversation_id = cm2.conversation_id
                JOIN chat_conversations cc ON cc.id = cm1.conversation_id
                WHERE cm1.employee_id=? AND cm2.employee_id=? AND cc.type='direct'
            """, (me["id"], other_id)).fetchone()
            if existing:
                return {"id": existing["conversation_id"], "existing": True}
        cid = _gen_id()
        name = req.name or ""
        db.execute("INSERT INTO chat_conversations (id,type,name,created_by) VALUES (?,?,?,?)",
                   (cid, req.type, name, me["id"]))
        all_members = list(set(req.member_ids + [me["id"]]))
        for eid in all_members:
            emp = db.execute("SELECT * FROM employees WHERE id=?", (eid,)).fetchone()
            if emp:
                db.execute("INSERT INTO chat_members (id,conversation_id,employee_id,employee_name) VALUES (?,?,?,?)",
                           (_gen_id(), cid, eid, emp["name"]))
        db.commit()
        return {"id": cid, "existing": False}
    finally:
        db.close()

@app.get("/api/chat/conversations")
def get_convos(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            return []
        rows = db.execute("""
            SELECT cc.*, 
                   (SELECT COUNT(*) FROM chat_messages cm 
                    WHERE cm.conversation_id=cc.id AND cm.is_deleted=0) as msg_count,
                   (SELECT cm2.content FROM chat_messages cm2 
                    WHERE cm2.conversation_id=cc.id AND cm2.is_deleted=0 
                    ORDER BY cm2.created_at DESC LIMIT 1) as last_message,
                   (SELECT cm2.created_at FROM chat_messages cm2 
                    WHERE cm2.conversation_id=cc.id AND cm2.is_deleted=0 
                    ORDER BY cm2.created_at DESC LIMIT 1) as last_at
            FROM chat_conversations cc
            JOIN chat_members mb ON mb.conversation_id=cc.id
            WHERE mb.employee_id=?
            ORDER BY last_at DESC NULLS LAST, cc.created_at DESC
        """, (me["id"],)).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            members = db.execute(
                "SELECT employee_id, employee_name FROM chat_members WHERE conversation_id=?",
                (r["id"],)
            ).fetchall()
            d["members"] = [dict(m) for m in members]
            result.append(d)
        return result
    finally:
        db.close()

@app.get("/api/chat/conversations/{cid}/messages")
def get_convo_msgs(cid: str, before: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            raise HTTPException(403, "Not a member")
        member = db.execute("SELECT id FROM chat_members WHERE conversation_id=? AND employee_id=?",
                            (cid, me["id"])).fetchone()
        if not member:
            raise HTTPException(403, "Not a member of this conversation")
        q = "SELECT * FROM chat_messages WHERE conversation_id=? AND is_deleted=0"
        params = [cid]
        if before:
            q += " AND created_at < ?"
            params.append(before)
        q += " ORDER BY created_at ASC LIMIT 60"
        rows = db.execute(q, params).fetchall()
        result = []
        for r in rows:
            d = dict(r)
            if d.get("file_data"):
                d["has_file"] = True
            result.append(d)
        return result
    finally:
        db.close()

class ChatMsgReq(BaseModel):
    content: str = ""
    msg_type: str = "text"
    file_name: str = ""
    file_size: int = 0
    file_data: str = ""
    file_mime: str = ""

@app.post("/api/chat/conversations/{cid}/messages")
def send_convo_msg(cid: str, req: ChatMsgReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            raise HTTPException(404, "Not found")
        member = db.execute("SELECT id FROM chat_members WHERE conversation_id=? AND employee_id=?",
                            (cid, me["id"])).fetchone()
        if not member:
            raise HTTPException(403, "Not a member of this conversation")
        mid = _gen_id()
        db.execute("""INSERT INTO chat_messages 
                      (id,conversation_id,sender_id,sender_name,sender_color,content,msg_type,
                       file_name,file_size,file_data,file_mime)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
                   (mid, cid, me["id"], me["name"], me.get("color","#FF6B00"),
                    req.content, req.msg_type, req.file_name, req.file_size,
                    req.file_data if req.file_data else None, req.file_mime))
        members = db.execute("SELECT employee_id FROM chat_members WHERE conversation_id=? AND employee_id!=?",
                             (cid, me["id"])).fetchall()
        preview = req.content[:80] if req.content else f"📎 {req.file_name}"
        for m in members:
            _notify(m["employee_id"], f"💬 {me['name']}",
                    preview or "Sent a file", "in")
        db.commit()
        return {"id": mid, "message": "Sent"}
    finally:
        db.close()

@app.delete("/api/chat/messages/{mid}")
def delete_chat_msg(mid: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        msg = db.execute("SELECT * FROM chat_messages WHERE id=?", (mid,)).fetchone()
        if not msg:
            raise HTTPException(404, "Not found")
        if msg["sender_id"] != me["id"] and user["role"] not in ["admin", "manager"]:
            raise HTTPException(403, "Cannot delete others' messages")
        db.execute("UPDATE chat_messages SET is_deleted=1, content='[deleted]', file_data=NULL WHERE id=?", (mid,))
        db.commit()
        return {"message": "Deleted"}
    finally:
        db.close()

@app.get("/api/chat/employees")
def get_chat_employees(q: str = "", authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        query = f"%{q}%"
        rows = db.execute("""SELECT id, name, department, role, color, status 
                             FROM employees WHERE status='active' AND id!=? 
                             AND (name LIKE ? OR department LIKE ?)
                             ORDER BY name LIMIT 30""",
                          (me["id"] if me else "", query, query)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

class MailReq(BaseModel):
    to_id: str
    subject: str
    body: str = ""
    file_name: str = ""
    file_size: int = 0
    file_data: str = ""
    file_mime: str = ""
    reply_to: str = ""

@app.post("/api/mail/send")
def send_mail(req: MailReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            raise HTTPException(404, "Not found")
        recipient = db.execute("SELECT * FROM employees WHERE id=?", (req.to_id,)).fetchone()
        if not recipient:
            raise HTTPException(404, "Recipient not found")
        mid = _gen_id()
        db.execute("""INSERT INTO mail_messages
                      (id,from_id,from_name,to_id,to_name,subject,body,
                       file_name,file_size,file_data,file_mime,reply_to)
                      VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                   (mid, me["id"], me["name"], recipient["id"], recipient["name"],
                    req.subject, req.body, req.file_name, req.file_size,
                    req.file_data if req.file_data else None, req.file_mime, req.reply_to or None))
        preview = req.body[:100] if req.body else f"📎 {req.file_name}"
        _notify(recipient["id"], f"📧 Mail from {me['name']}: {req.subject}",
                preview, "in")
        db.commit()
        _log(user["id"], user["name"], user["role"], "MAIL_SENT",
             {"to": recipient["name"], "subject": req.subject})
        return {"id": mid, "message": "Mail sent"}
    finally:
        db.close()

@app.get("/api/mail/inbox")
def get_inbox(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            return []
        rows = db.execute("""SELECT id,from_id,from_name,subject,body,is_read,is_starred,
                             file_name,file_size,file_mime,reply_to,created_at
                             FROM mail_messages 
                             WHERE to_id=? AND is_deleted_recipient=0
                             ORDER BY created_at DESC LIMIT 100""", (me["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/mail/sent")
def get_sent(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            return []
        rows = db.execute("""SELECT id,to_id,to_name,subject,body,is_read,is_starred,
                             file_name,file_size,file_mime,reply_to,created_at
                             FROM mail_messages 
                             WHERE from_id=? AND is_deleted_sender=0
                             ORDER BY created_at DESC LIMIT 100""", (me["id"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

@app.get("/api/mail/{mail_id}")
def get_mail(mail_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        row = db.execute("SELECT * FROM mail_messages WHERE id=?", (mail_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        if row["to_id"] != me["id"] and row["from_id"] != me["id"] and user["role"] != "admin":
            raise HTTPException(403, "Not authorized")
        if row["to_id"] == me["id"] and not row["is_read"]:
            db.execute("UPDATE mail_messages SET is_read=1 WHERE id=?", (mail_id,))
            db.commit()
        return dict(row)
    finally:
        db.close()

@app.patch("/api/mail/{mail_id}/star")
def star_mail(mail_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        row = db.execute("SELECT * FROM mail_messages WHERE id=?", (mail_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        db.execute("UPDATE mail_messages SET is_starred=1-is_starred WHERE id=?", (mail_id,))
        db.commit()
        return {"message": "Toggled"}
    finally:
        db.close()

@app.delete("/api/mail/{mail_id}")
def delete_mail(mail_id: str, authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        row = db.execute("SELECT * FROM mail_messages WHERE id=?", (mail_id,)).fetchone()
        if not row:
            raise HTTPException(404, "Not found")
        if row["to_id"] == me["id"]:
            db.execute("UPDATE mail_messages SET is_deleted_recipient=1 WHERE id=?", (mail_id,))
        if row["from_id"] == me["id"]:
            db.execute("UPDATE mail_messages SET is_deleted_sender=1 WHERE id=?", (mail_id,))
        db.commit()
        return {"message": "Deleted"}
    finally:
        db.close()

@app.get("/api/mail/unread-count")
def mail_unread(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        me = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not me:
            return {"count": 0}
        count = db.execute("SELECT COUNT(*) FROM mail_messages WHERE to_id=? AND is_read=0 AND is_deleted_recipient=0",
                           (me["id"],)).fetchone()[0]
        return {"count": count}
    finally:
        db.close()
