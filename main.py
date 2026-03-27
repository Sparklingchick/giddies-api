"""
GIDDIES EXPRESS - SINGLE FILE BACKEND
Everything in one file for easy deployment
"""
import sqlite3, hashlib, hmac, secrets, uuid, json, os, random
from datetime import datetime, timedelta
from fastapi import FastAPI, HTTPException, Header, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import Optional

# ════════════════════════════════════════════════════════
# CONFIG
# ════════════════════════════════════════════════════════
SECRET_KEY = os.environ.get("SECRET_KEY", "gx-change-this-secret-2024")
DB_PATH = os.environ.get("DB_PATH", "giddies.db")
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_MINUTES = 15
COLORS = ['#FF6B00','#F472B6','#60A5FA','#4ADE80','#A78BFA',
          '#FB923C','#34D399','#22D3EE','#F87171','#C084FC']

# ════════════════════════════════════════════════════════
# DATABASE
# ════════════════════════════════════════════════════════
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
        photo_url TEXT DEFAULT NULL,
        color TEXT DEFAULT '#FF6B00',
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
    CREATE TABLE IF NOT EXISTS mail (
        id TEXT PRIMARY KEY,
        sender_id TEXT NOT NULL,
        sender_name TEXT,
        sender_email TEXT,
        to_user_ids TEXT NOT NULL,
        to_names TEXT,
        subject TEXT NOT NULL,
        body TEXT NOT NULL,
        attachments TEXT DEFAULT '[]',
        is_read TEXT DEFAULT '{}',
        starred TEXT DEFAULT '[]',
        deleted_by TEXT DEFAULT '[]',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS channels (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL UNIQUE,
        description TEXT,
        created_by TEXT,
        is_private INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS channel_messages (
        id TEXT PRIMARY KEY,
        channel_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        sender_name TEXT,
        sender_color TEXT DEFAULT '#FF6B00',
        content TEXT NOT NULL,
        attachments TEXT DEFAULT '[]',
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
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS supply_requests (
        id TEXT PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity INTEGER DEFAULT 1,
        reason TEXT,
        department TEXT,
        requester_name TEXT,
        urgency TEXT DEFAULT 'Normal',
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    );
    """)
    # Seed default channels
    default_channels = [("general","General company channel"),
                        ("announcements","Official announcements"),
                        ("logistics","Logistics team"),
                        ("random","Casual conversations")]
    for ch_name, ch_desc in default_channels:
        if not conn.execute("SELECT id FROM channels WHERE name=?", (ch_name,)).fetchone():
            import uuid as _uuid3
            conn.execute("INSERT INTO channels (id,name,description,created_by) VALUES (?,?,?,?)",
                        (str(_uuid3.uuid4()), ch_name, ch_desc, "system"))
    conn.commit()

    conn.commit()
    # Seed only if empty
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        uid = str(uuid.uuid4())
        eid = str(uuid.uuid4())
        conn.execute("""INSERT INTO users (id,email,password_hash,role,department,is_active)
                        VALUES (?,?,?,?,?,1)""",
                     (uid, "admin@giddiesexpress.com", _hash_pwd("Admin@1234"), "admin", "Administration"))
        conn.execute("""INSERT INTO employees (id,user_id,name,email,department,role,title,status,color)
                        VALUES (?,?,?,?,?,?,?,'active','#F59E0B')""",
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

# ════════════════════════════════════════════════════════
# AUTH UTILITIES
# ════════════════════════════════════════════════════════
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
        sess = db.execute("SELECT * FROM sessions WHERE token=? AND is_active=1 AND force_logout=0",
                          (token,)).fetchone()
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
            raise HTTPException(403, f"Account suspended: {user['suspension_reason'] or 'Contact HR/IT'}")
        return {
            "id": user["id"], "email": user["email"],
            "role": user["role"],  # ALWAYS from DB
            "department": user["emp_dept"] or user["department"] or "",
            "name": user["name"] or user["email"],
            "emp_id": user["emp_id"], "salary": user["salary"] or 0,
            "token": token, "session_id": sess["id"]
        }
    finally:
        db.close()

def _log(user_id, user_name, user_role, action, details=None, status="success", ip=None):
    try:
        db = get_db()
        db.execute("INSERT INTO activity_log (id,user_id,user_name,user_role,action,details,ip_address,status) VALUES (?,?,?,?,?,?,?,?)",
                   (_gen_id(), user_id, user_name, user_role, action,
                    json.dumps(details) if details else None, ip, status))
        db.commit()
        db.close()
    except: pass

def _notify(emp_id, title, message, type="in"):
    try:
        db = get_db()
        db.execute("INSERT INTO notifications (id,employee_id,title,message,type) VALUES (?,?,?,?,?)",
                   (_gen_id(), emp_id, title, message, type))
        db.commit()
        db.close()
    except: pass

ROLE_PAGES = {
    "admin": "giddyexpress-admin.html",
    "manager": "giddyexpress-manager.html",
    "approval_manager": "giddyexpress-approval.html",
    "employee": "giddyexpress-employee.html",
}
DEPT_PAGES = {
    "HR": "giddyexpress-hr.html",
    "Finance": "giddyexpress-finance.html",
    "IT Support": "giddyexpress-it.html",
    "Payroll": "giddyexpress-payroll.html",
}

def _get_page(role, dept):
    if role == "dept_manager":
        return DEPT_PAGES.get(dept, "giddyexpress-dept.html")
    return ROLE_PAGES.get(role, "giddyexpress-employee.html")

# ════════════════════════════════════════════════════════
# APP
# ════════════════════════════════════════════════════════
app = FastAPI(title="Giddies Express API", version="2.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

@app.on_event("startup")
async def startup():
    init_db()

@app.get("/")
def root():
    return {"status": "Giddies Express API", "version": "2.0.0"}

@app.get("/health")
def health():
    return {"status": "ok", "time": _now()}

# ════════════════════════════════════════════════════════
# AUTH ROUTES
# ════════════════════════════════════════════════════════
class LoginReq(BaseModel):
    email: str
    password: str

@app.post("/api/auth/login")
async def login(req: LoginReq, request: Request):
    ip = request.client.host if request.client else "unknown"
    email = req.email.lower().strip()
    db = get_db()
    try:
        user = db.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        if not user:
            _log(None, email, None, "LOGIN_FAILED", f"Not found", "failed", ip)
            raise HTTPException(401, "Invalid email or password")
        if user["locked_until"] and not _is_expired(user["locked_until"]):
            raise HTTPException(429, f"Account locked. Too many failed attempts. Try again later.")
        if user["is_suspended"]:
            raise HTTPException(403, f"Account suspended: {user['suspension_reason'] or 'Contact HR/IT'}")
        if not user["is_active"]:
            raise HTTPException(403, "Account is deactivated")
        if not _verify_pwd(req.password, user["password_hash"]):
            attempts = (user["login_attempts"] or 0) + 1
            locked = (datetime.utcnow() + timedelta(minutes=LOCKOUT_MINUTES)).isoformat() if attempts >= MAX_LOGIN_ATTEMPTS else None
            db.execute("UPDATE users SET login_attempts=?, locked_until=? WHERE id=?", (attempts, locked, user["id"]))
            db.commit()
            _log(user["id"], email, user["role"], "LOGIN_FAILED", f"Wrong password attempt {attempts}", "failed", ip)
            remaining = MAX_LOGIN_ATTEMPTS - attempts
            if locked:
                raise HTTPException(429, f"Too many failed attempts. Account locked for {LOCKOUT_MINUTES} minutes.")
            raise HTTPException(401, f"Invalid email or password. {max(0,remaining)} attempts remaining.")
        # Success
        db.execute("UPDATE users SET login_attempts=0, locked_until=NULL, last_login=? WHERE id=?", (_now(), user["id"]))
        emp = db.execute("SELECT * FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        token = _create_token(user["id"], user["role"], user["email"])
        db.execute("INSERT INTO sessions (id,user_id,token,ip_address,expires_at) VALUES (?,?,?,?,?)",
                   (_gen_id(), user["id"], token, ip, _expires(8)))
        db.commit()
        dept = (emp["department"] if emp else user["department"]) or ""
        _log(user["id"], emp["name"] if emp else email, user["role"], "LOGIN_SUCCESS", ip=ip)
        return {
            "token": token,
            "redirect_to": _get_page(user["role"], dept),
            "password_reset_required": bool(user["password_reset_required"]),
            "user": {
                "id": user["id"], "email": user["email"],
                "role": user["role"], "department": dept,
                "name": emp["name"] if emp else email,
                "title": emp["title"] if emp else "",
                "salary": emp["salary"] if emp else 0,
                "color": emp["color"] if emp else "#FF6B00",
                "emp_id": emp["id"] if emp else None
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

# ════════════════════════════════════════════════════════
# USER MANAGEMENT
# ════════════════════════════════════════════════════════
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
        _log(user["id"], user["name"], user["role"], "USER_CREATED",
             {"name": req.name, "email": req.email, "role": req.role})
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
        _log(user["id"], user["name"], user["role"], "USER_SUSPENDED",
             {"target": target["emp_name"], "reason": body.reason})
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

# ════════════════════════════════════════════════════════
# NEW USER WORKFLOW (HR → Manager → IT)
# ════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════
# EMPLOYEES
# ════════════════════════════════════════════════════════
@app.get("/api/employees/")
def get_employees(status: str = None, department: str = None, authorization: str = Header(None)):
    user = _get_user(authorization)
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

# ════════════════════════════════════════════════════════
# ATTENDANCE
# ════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════
# LEAVE
# ════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════
# BUDGETS
# ════════════════════════════════════════════════════════
class BudgetReq(BaseModel):
    title: str
    amount: float
    reason: str

@app.post("/api/budgets/")
def submit_budget(req: BudgetReq, authorization: str = Header(None)):
    user = _get_user(authorization)
    if user["role"] not in ["dept_manager", "manager", "admin"]:
        raise HTTPException(403, "Not authorized")
    db = get_db()
    try:
        bid = _gen_id()
        db.execute("INSERT INTO budget_requests (id,department,title,amount,reason,submitted_by,submitter_name) VALUES (?,?,?,?,?,?,?)",
                   (bid, user["department"], req.title, req.amount, req.reason, user["id"], user["name"]))
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

# ════════════════════════════════════════════════════════
# ANNOUNCEMENTS
# ════════════════════════════════════════════════════════
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

@app.get("/api/announcements/")
def get_anns(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        rows = db.execute("SELECT * FROM announcements WHERE department='all' OR department=? ORDER BY created_at DESC LIMIT 30",
                          (user["department"],)).fetchall()
        return [dict(r) for r in rows]
    finally:
        db.close()

# ════════════════════════════════════════════════════════
# NOTIFICATIONS
# ════════════════════════════════════════════════════════
@app.get("/api/notifications/")
def get_notifs(authorization: str = Header(None)):
    user = _get_user(authorization)
    db = get_db()
    try:
        emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
        if not emp: return []
        rows = db.execute("SELECT * FROM notifications WHERE employee_id=? ORDER BY created_at DESC LIMIT 30", (emp["id"],)).fetchall()
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

# ════════════════════════════════════════════════════════
# IT SUPPORT
# ════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════
# DEPARTMENTS & REPORTS
# ════════════════════════════════════════════════════════
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

# ════════════════════════════════════════════════════════
# MESSAGES & TASKS
# ════════════════════════════════════════════════════════
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

@app.get("/unlock")
def unlock_admin():
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("""UPDATE users SET 
                      login_attempts=0, 
                      locked_until=NULL,
                      is_suspended=0,
                      password_hash=?,
                      is_active=1
                      WHERE email=?""",
                   (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        user = db.execute("SELECT email, role, login_attempts, locked_until, is_active FROM users WHERE email=?",
                         ("admin@giddiesexpress.com",)).fetchone()
        return {
            "message": "Admin unlocked! Password reset to Admin@1234",
            "email": user["email"],
            "role": user["role"],
            "attempts": user["login_attempts"],
            "locked": user["locked_until"],
            "active": user["is_active"]
        }
    finally:
        db.close()


# Run
@app.get("/api/fix-admin")
def fix_admin():
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("UPDATE users SET password_hash=?, login_attempts=0, locked_until=NULL, is_suspended=0 WHERE email=?",
                   (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        user = db.execute("SELECT email, role, is_active FROM users WHERE email=?",
                         ("admin@giddiesexpress.com",)).fetchone()
        if not user:
            # User doesn't exist, create fresh
            init_db()
            db.execute("UPDATE users SET password_hash=? WHERE email=?",
                      (_hash_pwd("Admin@1234"), "admin@giddiesexpress.com"))
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
    import os
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    init_db()
    db = get_db()
    try:
        new_hash = _hash_pwd("Admin@1234")
        db.execute("UPDATE users SET password_hash=? WHERE email=?",
                   (new_hash, "admin@giddiesexpress.com"))
        db.commit()
        count = db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        return {"message": "Database reset! Admin password is Admin@1234", "users": count}
    finally:
        db.close()



# ══════════════════════════════════════════════════════
# APPROVAL MANAGER ROLE + PROFILE PHOTO
# ══════════════════════════════════════════════════════
@app.post("/api/employees/{employee_id}/photo")
def upload_photo(employee_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    photo_url = data.get("photo_url", "")
    if not photo_url:
        raise HTTPException(400, "No photo provided")
    db.execute("UPDATE employees SET photo_url=? WHERE id=?", (photo_url, employee_id))
    db.commit()
    return {"message": "Photo updated"}

@app.patch("/api/employees/{employee_id}/info")
def update_employee_info(employee_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    allowed = ["name","phone","department","role","title","salary","contract_type","join_date","color"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        sets = ", ".join(k+"=?" for k in updates)
        db.execute(f"UPDATE employees SET {sets}, updated_at=datetime('now') WHERE id=?",
                   list(updates.values()) + [employee_id])
        if "role" in updates:
            db.execute("UPDATE users SET role=? WHERE id=(SELECT user_id FROM employees WHERE id=?)",
                       (updates["role"], employee_id))
        if "department" in updates:
            db.execute("UPDATE users SET department=? WHERE id=(SELECT user_id FROM employees WHERE id=?)",
                       (updates["department"], employee_id))
        db.commit()
    return {"message": "Employee updated"}

# ══════════════════════════════════════════════════════
# INTERNAL MAIL
# ══════════════════════════════════════════════════════
@app.get("/api/mail/inbox")
def get_inbox(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    import json as _j
    rows = db.execute("SELECT * FROM mail ORDER BY created_at DESC").fetchall()
    result = []
    for r in rows:
        d = dict(r)
        to_ids = d.get("to_user_ids","").split(",")
        if user["role"] == "admin" or user["id"] in to_ids:
            deleted = _j.loads(d.get("deleted_by") or "[]")
            if user["id"] not in deleted:
                reads = _j.loads(d.get("is_read") or "{}")
                d["is_read_by_me"] = user["id"] in reads
                result.append(d)
    return result

@app.get("/api/mail/sent")
def get_sent_mail(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    rows = db.execute("SELECT * FROM mail WHERE sender_id=? ORDER BY created_at DESC", (user["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/mail/all")
def get_all_mail(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    rows = db.execute("SELECT * FROM mail ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/mail/send")
def send_mail(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    if not data.get("subject"): raise HTTPException(400, "Subject required")
    if not data.get("to_user_ids"): raise HTTPException(400, "At least one recipient required")
    import json as _j
    mid = _id()
    to_ids = data["to_user_ids"] if isinstance(data["to_user_ids"], list) else [data["to_user_ids"]]
    emp = db.execute("SELECT name FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    sender_name = emp["name"] if emp else user["email"]
    db.execute(
        "INSERT INTO mail (id,sender_id,sender_name,sender_email,to_user_ids,to_names,subject,body,attachments,is_read,starred,deleted_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (mid, user["id"], sender_name, user["email"],
         ",".join(to_ids), data.get("to_names",""),
         data["subject"], data.get("body",""),
         _j.dumps(data.get("attachments",[])),
         _j.dumps({}), _j.dumps([]), _j.dumps([]))
    )
    db.commit()
    for rid in to_ids:
        _notify(rid, f"New mail: {data['subject'][:50]}", f"From {sender_name}", "in")
    return {"message": "Mail sent", "id": mid}

@app.post("/api/mail/{mail_id}/read")
def mark_mail_read(mail_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    import json as _j
    m = db.execute("SELECT is_read FROM mail WHERE id=?", (mail_id,)).fetchone()
    if not m: raise HTTPException(404)
    reads = _j.loads(m["is_read"] or "{}")
    reads[user["id"]] = _now()
    db.execute("UPDATE mail SET is_read=? WHERE id=?", (_j.dumps(reads), mail_id))
    db.commit()
    return {"message": "Read"}

@app.delete("/api/mail/{mail_id}")
def delete_mail(mail_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    import json as _j
    m = db.execute("SELECT deleted_by FROM mail WHERE id=?", (mail_id,)).fetchone()
    if not m: raise HTTPException(404)
    deleted = _j.loads(m["deleted_by"] or "[]")
    if user["id"] not in deleted:
        deleted.append(user["id"])
    db.execute("UPDATE mail SET deleted_by=? WHERE id=?", (_j.dumps(deleted), mail_id))
    db.commit()
    return {"message": "Deleted"}

# ══════════════════════════════════════════════════════
# CHANNELS
# ══════════════════════════════════════════════════════
@app.get("/api/channels/")
def get_channels(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    rows = db.execute("SELECT * FROM channels ORDER BY name").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/channels/")
def create_channel(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    if not data.get("name"): raise HTTPException(400, "Name required")
    name = data["name"].lower().replace(" ","-").replace("#","")
    cid = _id()
    db.execute("INSERT INTO channels (id,name,description,created_by,is_private) VALUES (?,?,?,?,?)",
               (cid, name, data.get("description",""), user["email"], 1 if data.get("is_private") else 0))
    db.commit()
    return {"message": f"Channel #{name} created", "id": cid}

@app.delete("/api/channels/{channel_id}")
def delete_channel(channel_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    db.execute("DELETE FROM channels WHERE id=?", (channel_id,))
    db.execute("DELETE FROM channel_messages WHERE channel_id=?", (channel_id,))
    db.commit()
    return {"message": "Channel deleted"}

@app.get("/api/channels/{channel_id}/messages")
def get_channel_msgs(channel_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    rows = db.execute(
        "SELECT * FROM channel_messages WHERE channel_id=? ORDER BY created_at ASC LIMIT 200",
        (channel_id,)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/channels/{channel_id}/messages")
def post_channel_msg(channel_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    import json as _j
    if not data.get("content") and not data.get("attachments"):
        raise HTTPException(400, "Content required")
    emp = db.execute("SELECT name, color FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    sender_name = emp["name"] if emp else user["email"]
    sender_color = emp["color"] if emp else "#FF6B00"
    mid = _id()
    db.execute(
        "INSERT INTO channel_messages (id,channel_id,sender_id,sender_name,sender_color,content,attachments) VALUES (?,?,?,?,?,?,?)",
        (mid, channel_id, user["id"], sender_name, sender_color,
         data.get("content",""), _j.dumps(data.get("attachments",[])))
    )
    db.commit()
    return {"message": "Sent", "id": mid}

# ══════════════════════════════════════════════════════
# TASKS
# ══════════════════════════════════════════════════════
@app.get("/api/tasks/")
def get_tasks(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    if user["role"] in ["admin","manager"]:
        rows = db.execute("SELECT * FROM tasks ORDER BY created_at DESC").fetchall()
    elif user["role"] == "approval_manager":
        rows = db.execute("SELECT * FROM tasks WHERE assigned_by=? ORDER BY created_at DESC", (user["id"],)).fetchall()
    else:
        rows = db.execute("SELECT * FROM tasks WHERE assigned_to=? ORDER BY created_at DESC", (user["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/tasks/")
def create_task(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Title required")
    emp = db.execute("SELECT name FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    assigner_name = emp["name"] if emp else user["email"]
    tid = _id()
    db.execute(
        "INSERT INTO tasks (id,title,description,assigned_to,assigned_to_name,assigned_by,assigned_by_name,department,due_date,priority,status) VALUES (?,?,?,?,?,?,?,?,?,?,'pending')",
        (tid, data["title"], data.get("description",""),
         data.get("assigned_to",""), data.get("assigned_to_name",""),
         user["id"], assigner_name, data.get("department",""),
         data.get("due_date",""), data.get("priority","medium"))
    )
    db.commit()
    if data.get("assigned_to"):
        _notify(data["assigned_to"], "New Task: "+data["title"], "Due: "+data.get("due_date","TBD"), "in")
    return {"message": "Task assigned", "id": tid}

@app.patch("/api/tasks/{task_id}")
def update_task_status(task_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","approval_manager","dept_manager","employee"], db)
    allowed = ["status","progress_note","priority","due_date","title","description"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        updates["updated_at"] = _now()
        sets = ", ".join(k+"=?" for k in updates)
        db.execute(f"UPDATE tasks SET {sets} WHERE id=?", list(updates.values()) + [task_id])
        db.commit()
    return {"message": "Updated"}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main_single:app", host="0.0.0.0", port=port)
