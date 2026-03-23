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
        is_locked INTEGER DEFAULT 0,
        lock_reason TEXT,
        locked_by TEXT,
        locked_at TEXT,
        password_expires_at TEXT,
        last_login_ip TEXT,
        trusted_devices TEXT,
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
        date_of_birth TEXT,
        address TEXT,
        emergency_contact_name TEXT,
        emergency_contact_phone TEXT,
        emergency_contact_relation TEXT,
        contract_end_date TEXT,
        probation_end_date TEXT,
        national_id TEXT,
        status TEXT DEFAULT 'active',
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
        is_half_day INTEGER DEFAULT 0,
        half_day_period TEXT,
        balance_deducted INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS leave_balances (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        year INTEGER NOT NULL,
        annual_total INTEGER DEFAULT 21,
        annual_used INTEGER DEFAULT 0,
        sick_total INTEGER DEFAULT 14,
        sick_used INTEGER DEFAULT 0,
        emergency_total INTEGER DEFAULT 5,
        emergency_used INTEGER DEFAULT 0,
        maternity_total INTEGER DEFAULT 90,
        maternity_used INTEGER DEFAULT 0,
        paternity_total INTEGER DEFAULT 14,
        paternity_used INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now')),
        UNIQUE(employee_id, year)
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
    CREATE TABLE IF NOT EXISTS expense_claims (
        id TEXT PRIMARY KEY,
        employee_id TEXT REFERENCES employees(id),
        employee_name TEXT,
        department TEXT,
        title TEXT NOT NULL,
        amount REAL NOT NULL,
        category TEXT DEFAULT 'General',
        description TEXT,
        receipt_note TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejection_note TEXT,
        payment_date TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS petty_cash (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL,
        description TEXT NOT NULL,
        amount REAL NOT NULL,
        category TEXT DEFAULT 'General',
        spent_by TEXT,
        spent_by_name TEXT,
        receipt_note TEXT,
        balance_before REAL DEFAULT 0,
        balance_after REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS petty_cash_funds (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL UNIQUE,
        total_fund REAL DEFAULT 0,
        current_balance REAL DEFAULT 0,
        last_topped_up TEXT,
        topped_up_by TEXT,
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS invoices (
        id TEXT PRIMARY KEY,
        invoice_number TEXT UNIQUE,
        department TEXT,
        vendor TEXT NOT NULL,
        title TEXT NOT NULL,
        amount REAL NOT NULL,
        tax_amount REAL DEFAULT 0,
        total_amount REAL NOT NULL,
        due_date TEXT,
        status TEXT DEFAULT 'unpaid',
        notes TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS vendors (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        category TEXT,
        contact_person TEXT,
        phone TEXT,
        email TEXT,
        address TEXT,
        bank_name TEXT,
        account_number TEXT,
        account_name TEXT,
        status TEXT DEFAULT 'active',
        notes TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS onboarding_checklists (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        id_submitted INTEGER DEFAULT 0,
        contract_signed INTEGER DEFAULT 0,
        orientation_done INTEGER DEFAULT 0,
        email_setup INTEGER DEFAULT 0,
        equipment_assigned INTEGER DEFAULT 0,
        system_access INTEGER DEFAULT 0,
        bank_details INTEGER DEFAULT 0,
        tax_form INTEGER DEFAULT 0,
        completed INTEGER DEFAULT 0,
        notes TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS disciplinary_records (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        type TEXT NOT NULL,
        description TEXT NOT NULL,
        action_taken TEXT,
        issued_by TEXT,
        issuer_name TEXT,
        status TEXT DEFAULT 'active',
        acknowledged INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS exit_management (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        exit_type TEXT NOT NULL,
        exit_date TEXT,
        reason TEXT,
        handover_done INTEGER DEFAULT 0,
        equipment_returned INTEGER DEFAULT 0,
        clearance_done INTEGER DEFAULT 0,
        final_payment_done INTEGER DEFAULT 0,
        exit_interview_done INTEGER DEFAULT 0,
        notes TEXT,
        processed_by TEXT,
        status TEXT DEFAULT 'in_progress',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS performance_reviews (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        review_period TEXT NOT NULL,
        reviewer_id TEXT,
        reviewer_name TEXT,
        work_quality INTEGER DEFAULT 3,
        punctuality INTEGER DEFAULT 3,
        teamwork INTEGER DEFAULT 3,
        communication INTEGER DEFAULT 3,
        initiative INTEGER DEFAULT 3,
        overall_score REAL DEFAULT 3,
        strengths TEXT,
        improvements TEXT,
        goals TEXT,
        comments TEXT,
        status TEXT DEFAULT 'draft',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS job_postings (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        department TEXT,
        type TEXT DEFAULT 'Full-time',
        location TEXT DEFAULT 'Lagos',
        description TEXT,
        requirements TEXT,
        salary_range TEXT,
        deadline TEXT,
        status TEXT DEFAULT 'open',
        applications_count INTEGER DEFAULT 0,
        posted_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS it_assets (
        id TEXT PRIMARY KEY,
        asset_tag TEXT UNIQUE,
        name TEXT NOT NULL,
        category TEXT NOT NULL,
        brand TEXT,
        model TEXT,
        serial_number TEXT,
        condition TEXT DEFAULT 'good',
        status TEXT DEFAULT 'available',
        assigned_to TEXT,
        assigned_to_name TEXT,
        assigned_date TEXT,
        purchase_date TEXT,
        purchase_price REAL DEFAULT 0,
        warranty_expiry TEXT,
        location TEXT,
        notes TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS software_licenses (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        vendor TEXT,
        license_key TEXT,
        seats INTEGER DEFAULT 1,
        seats_used INTEGER DEFAULT 0,
        expiry_date TEXT,
        cost REAL DEFAULT 0,
        category TEXT DEFAULT 'Productivity',
        status TEXT DEFAULT 'active',
        notes TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS it_remote_notes (
        id TEXT PRIMARY KEY,
        ticket_id TEXT REFERENCES it_tickets(id),
        employee_id TEXT,
        employee_name TEXT,
        session_type TEXT DEFAULT 'Remote',
        work_done TEXT NOT NULL,
        duration_minutes INTEGER DEFAULT 0,
        technician_id TEXT,
        technician_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS meetings (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        description TEXT,
        organizer_id TEXT,
        organizer_name TEXT,
        department TEXT,
        meeting_type TEXT DEFAULT 'Team Meeting',
        scheduled_date TEXT NOT NULL,
        scheduled_time TEXT,
        duration_minutes INTEGER DEFAULT 60,
        location TEXT DEFAULT 'Conference Room',
        attendees TEXT,
        agenda TEXT,
        minutes TEXT,
        status TEXT DEFAULT 'scheduled',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS dept_targets (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL,
        title TEXT NOT NULL,
        description TEXT,
        target_value REAL DEFAULT 0,
        current_value REAL DEFAULT 0,
        unit TEXT DEFAULT 'units',
        period TEXT NOT NULL,
        status TEXT DEFAULT 'active',
        set_by TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS overtime_requests (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        date TEXT NOT NULL,
        hours REAL NOT NULL,
        reason TEXT,
        hourly_rate REAL DEFAULT 0,
        total_amount REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS approval_delegations (
        id TEXT PRIMARY KEY,
        delegator_id TEXT NOT NULL,
        delegator_name TEXT,
        delegate_id TEXT NOT NULL,
        delegate_name TEXT,
        approval_types TEXT,
        from_date TEXT,
        to_date TEXT,
        reason TEXT,
        status TEXT DEFAULT 'active',
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS company_events (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        event_type TEXT DEFAULT 'holiday',
        date TEXT NOT NULL,
        end_date TEXT,
        description TEXT,
        is_public_holiday INTEGER DEFAULT 0,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS employee_documents (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        title TEXT NOT NULL,
        doc_type TEXT DEFAULT 'Other',
        file_note TEXT,
        uploaded_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS training_requests (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        course_title TEXT NOT NULL,
        provider TEXT,
        cost REAL DEFAULT 0,
        duration TEXT,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS announcement_reads (
        id TEXT PRIMARY KEY,
        announcement_id TEXT NOT NULL REFERENCES announcements(id) ON DELETE CASCADE,
        employee_id TEXT NOT NULL,
        employee_name TEXT,
        read_at TEXT DEFAULT (datetime('now')),
        UNIQUE(announcement_id, employee_id)
    );
    CREATE TABLE IF NOT EXISTS message_rooms (
        id TEXT PRIMARY KEY,
        name TEXT,
        room_type TEXT DEFAULT 'direct',
        participants TEXT,
        created_by TEXT,
        last_message TEXT,
        last_message_at TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS notice_board (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        content TEXT NOT NULL,
        priority INTEGER DEFAULT 0,
        pinned_by TEXT,
        expires_at TEXT,
        active INTEGER DEFAULT 1,
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
        is_pinned INTEGER DEFAULT 0,
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
        category TEXT DEFAULT 'General',
        sla_deadline TEXT,
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
        employee_name TEXT,
        department TEXT,
        period TEXT NOT NULL,
        run_id TEXT,
        gross REAL DEFAULT 0,
        allowances REAL DEFAULT 0,
        bonuses REAL DEFAULT 0,
        tax REAL DEFAULT 0,
        ni REAL DEFAULT 0,
        pension REAL DEFAULT 0,
        advance_deduction REAL DEFAULT 0,
        net REAL DEFAULT 0,
        status TEXT DEFAULT 'pending',
        paid_date TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS allowances (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        type TEXT NOT NULL,
        amount REAL DEFAULT 0,
        is_recurring INTEGER DEFAULT 1,
        description TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS bonuses (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        type TEXT NOT NULL,
        amount REAL DEFAULT 0,
        description TEXT,
        period TEXT,
        is_recurring INTEGER DEFAULT 0,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        created_by TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS salary_advances (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL REFERENCES employees(id) ON DELETE CASCADE,
        employee_name TEXT,
        department TEXT,
        amount REAL NOT NULL,
        reason TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        repayment_months INTEGER DEFAULT 1,
        monthly_deduction REAL DEFAULT 0,
        amount_repaid REAL DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS salary_grades (
        id TEXT PRIMARY KEY,
        level INTEGER NOT NULL,
        title TEXT NOT NULL,
        min_salary REAL NOT NULL,
        max_salary REAL NOT NULL,
        description TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS payroll_runs (
        id TEXT PRIMARY KEY,
        period TEXT NOT NULL,
        payment_date TEXT,
        total_employees INTEGER DEFAULT 0,
        total_gross REAL DEFAULT 0,
        total_deductions REAL DEFAULT 0,
        total_net REAL DEFAULT 0,
        total_allowances REAL DEFAULT 0,
        total_bonuses REAL DEFAULT 0,
        status TEXT DEFAULT 'draft',
        locked INTEGER DEFAULT 0,
        locked_by TEXT,
        locked_at TEXT,
        notes TEXT,
        run_by TEXT,
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
    CREATE TABLE IF NOT EXISTS portal_settings (
        key TEXT PRIMARY KEY,
        value TEXT,
        updated_by TEXT,
        updated_at TEXT
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
    conn.commit()
    # Seed only if empty
    if conn.execute("SELECT COUNT(*) FROM users").fetchone()[0] == 0:
        uid = str(uuid.uuid4())
        eid = str(uuid.uuid4())
        # Portal settings defaults
        conn.execute("INSERT OR IGNORE INTO portal_settings (key,value,updated_by,updated_at) VALUES ('portal_open','1','system','now')")
        # Default salary grades
        grades = [(1,'Junior',50000,150000,'Entry level'),(2,'Associate',150001,300000,'2-3 years experience'),
                  (3,'Mid-level',300001,500000,'3-5 years experience'),(4,'Senior',500001,800000,'5-8 years experience'),
                  (5,'Lead',800001,1200000,'Team lead/Senior specialist'),(6,'Manager',1200001,2000000,'Department manager'),
                  (7,'Director',2000001,5000000,'Director level'),(8,'Executive',5000001,99999999,'C-Suite/Executive')]
        for g in grades:
            conn.execute("INSERT OR IGNORE INTO salary_grades (id,level,title,min_salary,max_salary,description) VALUES (?,?,?,?,?,?)",
                        (_id(),g[0],g[1],g[2],g[3],g[4]))
        conn.execute("INSERT OR IGNORE INTO portal_settings (key,value,updated_by,updated_at) VALUES ('portal_closed_message','The portal is currently closed for maintenance. Please contact Admin.','system','now')")
        # Integration keys (empty by default)
        for _ikey in ['paystack_secret_key','paystack_public_key','twilio_account_sid','twilio_auth_token',
                      'twilio_whatsapp_from','smtp_host','smtp_user','smtp_password',
                      'company_tin','company_nhis_code','company_nsitf_code']:
            conn.execute("INSERT OR IGNORE INTO portal_settings (key,value,updated_by,updated_at) VALUES (?,'','system','now')", (_ikey,))
        # Default Nigerian public holidays for current year
        import datetime as _dt2
        year = _dt2.datetime.now().year
        holidays = [
            (f"{year}-01-01","New Year's Day","holiday",1),
            (f"{year}-04-18","Good Friday","holiday",1),
            (f"{year}-04-21","Easter Monday","holiday",1),
            (f"{year}-05-01","Workers' Day","holiday",1),
            (f"{year}-06-12","Democracy Day","holiday",1),
            (f"{year}-10-01","Independence Day","holiday",1),
            (f"{year}-12-25","Christmas Day","holiday",1),
            (f"{year}-12-26","Boxing Day","holiday",1),
        ]
        for h in holidays:
            conn.execute("INSERT OR IGNORE INTO company_events (id,date,title,event_type,is_public_holiday,created_by) VALUES (?,?,?,?,?,'system')",
                        (_id(),h[0],h[1],h[2],h[3]))
        conn.commit()
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
        # Check portal open/closed
        portal = db.execute("SELECT value FROM portal_settings WHERE key='portal_open'").fetchone()
        if portal and portal["value"] == "0" and user["role"] not in ["admin"]:
            msg = db.execute("SELECT value FROM portal_settings WHERE key='portal_closed_message'").fetchone()
            raise HTTPException(503, msg["value"] if msg else "Portal is currently closed.")
        
        # Check account locked
        if user["is_locked"] and user["role"] not in ["admin"]:
            reason = user["lock_reason"] or "Your account has been locked."
            raise HTTPException(423, f"{reason}|||LOCKED")

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
        db.execute("UPDATE users SET login_attempts=0, locked_until=NULL, last_login=?, last_login_ip=? WHERE id=?", 
                   (_now(), request.client.host if request else "unknown", user["id"]))
        
        # Force logout ALL other active sessions for this user (duplicate login protection)
        existing_sessions = db.execute(
            "SELECT id FROM sessions WHERE user_id=? AND is_active=1", (user["id"],)
        ).fetchall()
        if existing_sessions:
            db.execute(
                "UPDATE sessions SET is_active=0, force_logout=1, logout_reason='Another device logged in' WHERE user_id=? AND is_active=1",
                (user["id"],)
            )
            # Log the duplicate login event
            _log(db, user["id"], user["email"], user["role"], "DUPLICATE_LOGIN_DETECTED", 
                 {"message": "Previous session forced out - new login detected", "ip": request.client.host if request else "unknown"})
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
                             u.is_suspended, u.suspension_reason, u.is_locked, u.lock_reason,
                             u.locked_by, u.locked_at, u.last_login, u.created_at,
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
        # Auto-deduct leave balance when approved
        if body.approved and not leave["balance_deducted"]:
            emp = db.execute("SELECT id FROM employees WHERE id=?", (leave["employee_id"],)).fetchone()
            if emp:
                import datetime as _dt
                year = int(leave["from_date"][:4]) if leave["from_date"] else _dt.datetime.now().year
                days = leave["days"] or 1
                ltype = (leave["type"] or "").lower()
                if "annual" in ltype:
                    db.execute("UPDATE leave_balances SET annual_used=annual_used+? WHERE employee_id=? AND year=?", (days, emp["id"], year))
                elif "sick" in ltype:
                    db.execute("UPDATE leave_balances SET sick_used=sick_used+? WHERE employee_id=? AND year=?", (days, emp["id"], year))
                elif "emergency" in ltype:
                    db.execute("UPDATE leave_balances SET emergency_used=emergency_used+? WHERE employee_id=? AND year=?", (days, emp["id"], year))
                elif "maternity" in ltype:
                    db.execute("UPDATE leave_balances SET maternity_used=maternity_used+? WHERE employee_id=? AND year=?", (days, emp["id"], year))
                elif "paternity" in ltype:
                    db.execute("UPDATE leave_balances SET paternity_used=paternity_used+? WHERE employee_id=? AND year=?", (days, emp["id"], year))
                db.execute("UPDATE leave_requests SET balance_deducted=1 WHERE id=?", (leave_id,))
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

# ── LEAVE BALANCE ──
def _get_or_create_balance(db, employee_id: str, year: int = None):
    if not year:
        year = int(__import__('datetime').datetime.now().year)
    existing = db.execute(
        "SELECT * FROM leave_balances WHERE employee_id=? AND year=?",
        (employee_id, year)
    ).fetchone()
    if existing:
        return dict(existing)
    # Create new balance record for this year
    bid = _id()
    db.execute(
        """INSERT INTO leave_balances 
           (id, employee_id, year, annual_total, annual_used, sick_total, sick_used,
            emergency_total, emergency_used, maternity_total, maternity_used, 
            paternity_total, paternity_used)
           VALUES (?,?,?,21,0,14,0,5,0,90,0,14,0)""",
        (bid, employee_id, year)
    )
    db.commit()
    return dict(db.execute("SELECT * FROM leave_balances WHERE id=?", (bid,)).fetchone())

@app.get("/api/leave/balance/me")
def my_leave_balance(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp:
        raise HTTPException(404, "Employee record not found")
    year = int(__import__('datetime').datetime.now().year)
    balance = _get_or_create_balance(db, emp["id"], year)
    return {
        "year": year,
        "annual": {"total": balance["annual_total"], "used": balance["annual_used"], "remaining": balance["annual_total"] - balance["annual_used"]},
        "sick": {"total": balance["sick_total"], "used": balance["sick_used"], "remaining": balance["sick_total"] - balance["sick_used"]},
        "emergency": {"total": balance["emergency_total"], "used": balance["emergency_used"], "remaining": balance["emergency_total"] - balance["emergency_used"]},
        "maternity": {"total": balance["maternity_total"], "used": balance["maternity_used"], "remaining": balance["maternity_total"] - balance["maternity_used"]},
        "paternity": {"total": balance["paternity_total"], "used": balance["paternity_used"], "remaining": balance["paternity_total"] - balance["paternity_used"]},
    }

@app.get("/api/leave/balance/{employee_id}")
def get_employee_balance(employee_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["dept_manager","manager","admin"], db)
    year = int(__import__('datetime').datetime.now().year)
    balance = _get_or_create_balance(db, employee_id, year)
    return {
        "year": year,
        "annual": {"total": balance["annual_total"], "used": balance["annual_used"], "remaining": balance["annual_total"] - balance["annual_used"]},
        "sick": {"total": balance["sick_total"], "used": balance["sick_used"], "remaining": balance["sick_total"] - balance["sick_used"]},
        "emergency": {"total": balance["emergency_total"], "used": balance["emergency_used"], "remaining": balance["emergency_total"] - balance["emergency_used"]},
        "maternity": {"total": balance["maternity_total"], "used": balance["maternity_used"], "remaining": balance["maternity_total"] - balance["maternity_used"]},
        "paternity": {"total": balance["paternity_total"], "used": balance["paternity_used"], "remaining": balance["paternity_total"] - balance["paternity_used"]},
    }

@app.get("/api/leave/all-balances")
def all_leave_balances(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["dept_manager","manager","admin"], db)
    year = int(__import__('datetime').datetime.now().year)
    emps = db.execute("SELECT id, name, department FROM employees WHERE status='active'").fetchall()
    result = []
    for emp in emps:
        bal = _get_or_create_balance(db, emp["id"], year)
        result.append({
            "employee_id": emp["id"],
            "name": emp["name"],
            "department": emp["department"],
            "annual_remaining": bal["annual_total"] - bal["annual_used"],
            "sick_remaining": bal["sick_total"] - bal["sick_used"],
            "emergency_remaining": bal["emergency_total"] - bal["emergency_used"],
            "annual_used": bal["annual_used"],
            "sick_used": bal["sick_used"],
        })
    return result

@app.get("/api/leave/calendar")
def leave_calendar(month: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    if not month:
        from datetime import datetime
        month = datetime.now().strftime("%Y-%m")
    rows = db.execute(
        """SELECT lr.employee_name, lr.department, lr.from_date, lr.to_date, 
               lr.type, lr.days, lr.status
           FROM leave_requests lr
           WHERE (lr.from_date LIKE ? OR lr.to_date LIKE ?) 
           AND lr.status='approved'
           ORDER BY lr.from_date""",
        (month + "%", month + "%")
    ).fetchall()
    return [dict(r) for r in rows]

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

# ══════════════════════════════════════════════════════
# ALLOWANCES
# ══════════════════════════════════════════════════════
@app.get("/api/allowances/{employee_id}")
def get_allowances(employee_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager","employee"], db)
    rows = db.execute("SELECT * FROM allowances WHERE employee_id=? ORDER BY type", (employee_id,)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/allowances/")
def add_allowance(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    aid = _id()
    db.execute(
        "INSERT INTO allowances (id,employee_id,type,amount,is_recurring,description,created_by) VALUES (?,?,?,?,?,?,?)",
        (aid, data["employee_id"], data["type"], data.get("amount",0),
         1 if data.get("is_recurring",True) else 0, data.get("description",""), user["email"])
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "ALLOWANCE_ADDED",
         {"employee_id": data["employee_id"], "type": data["type"], "amount": data.get("amount",0)})
    return {"message": "Allowance added successfully", "id": aid}

@app.delete("/api/allowances/{allowance_id}")
def delete_allowance(allowance_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    db.execute("DELETE FROM allowances WHERE id=?", (allowance_id,))
    db.commit()
    return {"message": "Allowance removed"}

# ══════════════════════════════════════════════════════
# BONUSES
# ══════════════════════════════════════════════════════
@app.get("/api/bonuses/")
def get_bonuses(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM bonuses ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/bonuses/me")
def my_bonuses(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute("SELECT * FROM bonuses WHERE employee_id=? ORDER BY created_at DESC", (emp["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/bonuses/")
def add_bonus(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    bid = _id()
    emp = db.execute("SELECT name, department FROM employees WHERE id=?", (data["employee_id"],)).fetchone()
    db.execute(
        "INSERT INTO bonuses (id,employee_id,employee_name,department,type,amount,description,period,is_recurring,status,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (bid, data["employee_id"], emp["name"] if emp else "", emp["department"] if emp else "",
         data.get("type","Bonus"), data.get("amount",0), data.get("description",""),
         data.get("period",""), 1 if data.get("is_recurring") else 0, "approved", user["email"])
    )
    db.commit()
    if emp:
        emp_record = db.execute("SELECT id FROM employees WHERE id=?", (data["employee_id"],)).fetchone()
        _notify(data["employee_id"], "Bonus Added! 🎉",
                f"A {data.get('type','bonus')} of {data.get('amount',0)} has been added to your payroll", "ok")
    _log(db, user["id"], user["email"], user["role"], "BONUS_ADDED",
         {"employee_id": data["employee_id"], "amount": data.get("amount",0)})
    return {"message": "Bonus added", "id": bid}

# ══════════════════════════════════════════════════════
# SALARY ADVANCES
# ══════════════════════════════════════════════════════
@app.get("/api/salary-advances/")
def get_advances(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM salary_advances ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/salary-advances/me")
def my_advances(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute("SELECT * FROM salary_advances WHERE employee_id=? ORDER BY created_at DESC", (emp["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/salary-advances/")
def request_advance(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id, name, department, salary FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    amount = data.get("amount", 0)
    if amount <= 0: raise HTTPException(400, "Invalid amount")
    if emp["salary"] > 0 and amount > emp["salary"]:
        raise HTTPException(400, "Advance cannot exceed monthly salary")
    months = data.get("repayment_months", 1)
    monthly = round(amount / months, 2)
    aid = _id()
    db.execute(
        "INSERT INTO salary_advances (id,employee_id,employee_name,department,amount,reason,status,repayment_months,monthly_deduction) VALUES (?,?,?,?,?,?,?,?,?)",
        (aid, emp["id"], emp["name"], emp["department"], amount, data.get("reason",""), "pending", months, monthly)
    )
    db.commit()
    return {"message": "Salary advance request submitted", "id": aid, "monthly_deduction": monthly}

@app.post("/api/salary-advances/{advance_id}/action")
def action_advance(advance_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    status = "approved" if data.get("approved") else "rejected"
    db.execute("UPDATE salary_advances SET status=?, approved_by=? WHERE id=?",
               (status, user["email"], advance_id))
    db.commit()
    adv = db.execute("SELECT * FROM salary_advances WHERE id=?", (advance_id,)).fetchone()
    if adv:
        _notify(adv["employee_id"], f"Salary Advance {'Approved ✅' if data.get('approved') else 'Rejected ❌'}",
                f"Your salary advance request of {adv['amount']} was {status}", "ok" if data.get("approved") else "er")
    return {"message": f"Advance {status}"}

# ══════════════════════════════════════════════════════
# SALARY GRADES
# ══════════════════════════════════════════════════════
@app.get("/api/salary-grades/")
def get_salary_grades(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM salary_grades ORDER BY level").fetchall()
    return [dict(r) for r in rows]

# ══════════════════════════════════════════════════════
# PAYROLL RUNS
# ══════════════════════════════════════════════════════
@app.get("/api/payroll/runs")
def get_payroll_runs(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM payroll_runs ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/payroll/run")
def run_payroll(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    period = data.get("period","")
    payment_date = data.get("payment_date","")
    if not period: raise HTTPException(400, "Period is required")
    # Check not already run for this period
    existing = db.execute("SELECT id FROM payroll_runs WHERE period=? AND status != 'cancelled'", (period,)).fetchone()
    if existing: raise HTTPException(400, f"Payroll already run for {period}")
    # Create run record
    run_id = _id()
    emps = db.execute("SELECT * FROM employees WHERE status='active' AND salary > 0").fetchall()
    total_gross = total_tax = total_ni = total_pension = total_net = total_allowances = total_bonuses = 0
    for emp in emps:
        gross = emp["salary"] or 0
        # Get allowances
        emp_allowances = db.execute("SELECT SUM(amount) as total FROM allowances WHERE employee_id=? AND is_recurring=1", (emp["id"],)).fetchone()
        allowance_total = emp_allowances["total"] or 0 if emp_allowances else 0
        # Get bonuses for this period
        emp_bonuses = db.execute("SELECT SUM(amount) as total FROM bonuses WHERE employee_id=? AND (period=? OR is_recurring=1) AND status='approved'", (emp["id"], period)).fetchone()
        bonus_total = emp_bonuses["total"] or 0 if emp_bonuses else 0
        # Get approved advance deductions
        advance = db.execute("SELECT monthly_deduction FROM salary_advances WHERE employee_id=? AND status='approved' AND amount_repaid < amount ORDER BY created_at LIMIT 1", (emp["id"],)).fetchone()
        advance_deduction = advance["monthly_deduction"] if advance else 0
        # Calculate deductions on gross + allowances + bonuses
        total_income = gross + allowance_total + bonus_total
        tax = round(total_income * 0.20, 2)
        ni = round(total_income * 0.08, 2)
        pension = round(total_income * 0.05, 2)
        net = round(total_income - tax - ni - pension - advance_deduction, 2)
        # Save payroll record
        rec_id = _id()
        db.execute(
            "INSERT INTO payroll_records (id,employee_id,employee_name,department,period,run_id,gross,allowances,bonuses,tax,ni,pension,advance_deduction,net,status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,'pending')",
            (rec_id, emp["id"], emp["name"], emp["department"], period, run_id,
             gross, allowance_total, bonus_total, tax, ni, pension, advance_deduction, net)
        )
        # Update advance repayment
        if advance_deduction > 0 and advance:
            db.execute("UPDATE salary_advances SET amount_repaid=amount_repaid+? WHERE employee_id=? AND status='approved' AND amount_repaid < amount", (advance_deduction, emp["id"]))
        total_gross += gross; total_allowances += allowance_total; total_bonuses += bonus_total
        total_tax += tax; total_ni += ni; total_pension += pension; total_net += net
    # Save run summary
    db.execute(
        "INSERT INTO payroll_runs (id,period,payment_date,total_employees,total_gross,total_deductions,total_net,total_allowances,total_bonuses,status,run_by) VALUES (?,?,?,?,?,?,?,?,?,'completed',?)",
        (run_id, period, payment_date, len(emps), total_gross, total_tax+total_ni+total_pension, total_net, total_allowances, total_bonuses, user["email"])
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "PAYROLL_RUN", {"period": period, "employees": len(emps), "net": total_net})
    return {"message": f"Payroll run complete for {period}", "run_id": run_id, "employees": len(emps), "total_net": total_net}

@app.post("/api/payroll/runs/{run_id}/lock")
def lock_payroll(run_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    db.execute("UPDATE payroll_runs SET locked=1, locked_by=?, locked_at=? WHERE id=?",
               (user["email"], _now(), run_id))
    db.commit()
    return {"message": "Payroll period locked — no further changes allowed"}

@app.get("/api/payroll/records/{run_id}")
def get_payroll_records(run_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM payroll_records WHERE run_id=? ORDER BY department, employee_name", (run_id,)).fetchall()
    return [dict(r) for r in rows]


# ════════════════════════════════════════════════════════
# EXPENSE CLAIMS
# ════════════════════════════════════════════════════════
@app.get("/api/expenses/")
def get_expenses(status: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if status:
        rows = db.execute("SELECT * FROM expense_claims WHERE status=? ORDER BY created_at DESC", (status,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM expense_claims ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/expenses/me")
def my_expenses(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute("SELECT * FROM expense_claims WHERE employee_id=? ORDER BY created_at DESC", (emp["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/expenses/")
def submit_expense(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id, name, department FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    if not data.get("title"): raise HTTPException(400, "Title is required")
    if not data.get("amount") or data["amount"] <= 0: raise HTTPException(400, "Valid amount required")
    eid = _id()
    db.execute(
        "INSERT INTO expense_claims (id,employee_id,employee_name,department,title,amount,category,description,receipt_note,status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (eid, emp["id"], emp["name"], emp["department"], data["title"],
         data["amount"], data.get("category","General"), data.get("description",""),
         data.get("receipt_note",""), "pending")
    )
    db.commit()
    return {"message": "Expense claim submitted", "id": eid}

@app.post("/api/expenses/{expense_id}/action")
def action_expense(expense_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    approved = data.get("approved", False)
    status = "approved" if approved else "rejected"
    db.execute(
        "UPDATE expense_claims SET status=?, approved_by=?, approver_name=?, rejection_note=?, payment_date=? WHERE id=?",
        (status, user["id"], user.get("name","?"),
         data.get("note") if not approved else None,
         _now() if approved else None, expense_id)
    )
    db.commit()
    exp = db.execute("SELECT * FROM expense_claims WHERE id=?", (expense_id,)).fetchone()
    if exp:
        _notify(exp["employee_id"],
                "Expense Claim " + ("Approved" if approved else "Rejected"),
                "Your expense claim of " + str(exp["amount"]) + " for " + exp["title"] + " was " + status,
                "ok" if approved else "er")
    return {"message": "Expense claim " + status}

# ════════════════════════════════════════════════════════
# PETTY CASH
# ════════════════════════════════════════════════════════
@app.get("/api/petty-cash/")
def get_petty_cash(department: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if department:
        rows = db.execute("SELECT * FROM petty_cash WHERE department=? ORDER BY created_at DESC", (department,)).fetchall()
    else:
        rows = db.execute("SELECT * FROM petty_cash ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/petty-cash/funds")
def get_petty_cash_funds(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM petty_cash_funds ORDER BY department").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/petty-cash/")
def add_petty_cash(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    dept = data.get("department","")
    amount = data.get("amount", 0)
    fund = db.execute("SELECT * FROM petty_cash_funds WHERE department=?", (dept,)).fetchone()
    if not fund:
        db.execute("INSERT INTO petty_cash_funds (id,department,total_fund,current_balance) VALUES (?,?,0,0)", (_id(), dept))
        db.commit()
        fund = db.execute("SELECT * FROM petty_cash_funds WHERE department=?", (dept,)).fetchone()
    balance_before = fund["current_balance"]
    balance_after = balance_before - amount
    db.execute(
        "INSERT INTO petty_cash (id,department,description,amount,category,spent_by,spent_by_name,receipt_note,balance_before,balance_after) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (_id(), dept, data.get("description",""), amount, data.get("category","General"),
         user["id"], user.get("name","?"), data.get("receipt_note",""), balance_before, balance_after)
    )
    db.execute("UPDATE petty_cash_funds SET current_balance=?, updated_at=? WHERE department=?",
               (balance_after, _now(), dept))
    db.commit()
    return {"message": "Petty cash recorded", "balance": balance_after}

@app.post("/api/petty-cash/topup")
def topup_petty_cash(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    dept = data.get("department","")
    amount = data.get("amount", 0)
    fund = db.execute("SELECT * FROM petty_cash_funds WHERE department=?", (dept,)).fetchone()
    if not fund:
        db.execute("INSERT INTO petty_cash_funds (id,department,total_fund,current_balance) VALUES (?,?,?,?)", (_id(), dept, amount, amount))
    else:
        db.execute(
            "UPDATE petty_cash_funds SET current_balance=current_balance+?, total_fund=total_fund+?, last_topped_up=?, topped_up_by=?, updated_at=? WHERE department=?",
            (amount, amount, _now(), user["email"], _now(), dept)
        )
    db.commit()
    return {"message": "Petty cash topped up for " + dept}

# ════════════════════════════════════════════════════════
# INVOICES
# ════════════════════════════════════════════════════════
@app.get("/api/invoices/")
def get_invoices(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM invoices ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/invoices/")
def create_invoice(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    iid = _id()
    count = db.execute("SELECT COUNT(*) as c FROM invoices").fetchone()["c"]
    inv_num = "GE-INV-" + str(count+1).zfill(4)
    tax = data.get("tax_amount", 0)
    total = data.get("amount", 0) + tax
    db.execute(
        "INSERT INTO invoices (id,invoice_number,department,vendor,title,amount,tax_amount,total_amount,due_date,status,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        (iid, inv_num, data.get("department",""), data.get("vendor",""), data.get("title",""),
         data.get("amount",0), tax, total, data.get("due_date",""), "unpaid",
         data.get("notes",""), user["email"])
    )
    db.commit()
    return {"message": "Invoice created", "id": iid, "invoice_number": inv_num}

@app.patch("/api/invoices/{invoice_id}")
def update_invoice_status(invoice_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if "status" in data:
        db.execute("UPDATE invoices SET status=? WHERE id=?", (data["status"], invoice_id))
        db.commit()
    return {"message": "Invoice updated"}

# ════════════════════════════════════════════════════════
# VENDORS
# ════════════════════════════════════════════════════════
@app.get("/api/vendors/")
def get_vendors(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM vendors WHERE status='active' ORDER BY name").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/vendors/")
def create_vendor(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if not data.get("name"): raise HTTPException(400, "Vendor name required")
    vid = _id()
    db.execute(
        "INSERT INTO vendors (id,name,category,contact_person,phone,email,address,bank_name,account_number,account_name,status,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (vid, data["name"], data.get("category",""), data.get("contact_person",""),
         data.get("phone",""), data.get("email",""), data.get("address",""),
         data.get("bank_name",""), data.get("account_number",""), data.get("account_name",""),
         "active", data.get("notes",""), user["email"])
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "VENDOR_CREATED", {"name": data["name"]})
    return {"message": "Vendor added", "id": vid}

@app.patch("/api/vendors/{vendor_id}")
def update_vendor(vendor_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    allowed = ["name","category","contact_person","phone","email","address","bank_name","account_number","account_name","status","notes"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        sets = ", ".join(k + "=?" for k in updates)
        db.execute("UPDATE vendors SET " + sets + " WHERE id=?", list(updates.values()) + [vendor_id])
        db.commit()
    return {"message": "Vendor updated"}


# ════════════════════════════════════════════════════════
# ONBOARDING CHECKLISTS
# ════════════════════════════════════════════════════════
@app.get("/api/onboarding/")
def get_onboarding(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM onboarding_checklists ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/onboarding/{employee_id}")
def get_employee_onboarding(employee_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    row = db.execute("SELECT * FROM onboarding_checklists WHERE employee_id=?", (employee_id,)).fetchone()
    return dict(row) if row else {}

@app.post("/api/onboarding/")
def create_onboarding(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    emp_id = data.get("employee_id","")
    emp = db.execute("SELECT name, department FROM employees WHERE id=?", (emp_id,)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    existing = db.execute("SELECT id FROM onboarding_checklists WHERE employee_id=?", (emp_id,)).fetchone()
    if existing: raise HTTPException(400, "Onboarding checklist already exists for this employee")
    oid = _id()
    db.execute(
        "INSERT INTO onboarding_checklists (id,employee_id,employee_name,department) VALUES (?,?,?,?)",
        (oid, emp_id, emp["name"], emp["department"])
    )
    db.commit()
    return {"message": "Onboarding checklist created", "id": oid}

@app.patch("/api/onboarding/{checklist_id}")
def update_onboarding(checklist_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    fields = ["id_submitted","contract_signed","orientation_done","email_setup",
              "equipment_assigned","system_access","bank_details","tax_form","notes"]
    updates = {k: v for k, v in data.items() if k in fields}
    # Check if all done
    checklist = db.execute("SELECT * FROM onboarding_checklists WHERE id=?", (checklist_id,)).fetchone()
    if checklist:
        merged = dict(checklist)
        merged.update(updates)
        all_done = all(merged.get(f,0) for f in ["id_submitted","contract_signed","orientation_done",
                                                   "email_setup","equipment_assigned","system_access",
                                                   "bank_details","tax_form"])
        updates["completed"] = 1 if all_done else 0
        updates["updated_at"] = _now()
    if updates:
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE onboarding_checklists SET "+sets+" WHERE id=?", list(updates.values())+[checklist_id])
        db.commit()
    return {"message": "Checklist updated", "completed": updates.get("completed",0)}

# ════════════════════════════════════════════════════════
# DISCIPLINARY RECORDS
# ════════════════════════════════════════════════════════
@app.get("/api/disciplinary/")
def get_disciplinary(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM disciplinary_records ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/disciplinary/{employee_id}")
def get_employee_disciplinary(employee_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM disciplinary_records WHERE employee_id=? ORDER BY created_at DESC", (employee_id,)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/disciplinary/")
def create_disciplinary(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    emp = db.execute("SELECT name, department FROM employees WHERE id=?", (data.get("employee_id",""),)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    did = _id()
    db.execute(
        "INSERT INTO disciplinary_records (id,employee_id,employee_name,department,type,description,action_taken,issued_by,issuer_name,status) VALUES (?,?,?,?,?,?,?,?,?,?)",
        (did, data["employee_id"], emp["name"], emp["department"],
         data.get("type","Verbal Warning"), data.get("description",""),
         data.get("action_taken",""), user["id"], user.get("name","?"), "active")
    )
    db.commit()
    _notify(data["employee_id"], "Disciplinary Notice",
            "A disciplinary record has been issued. Please contact HR.", "er")
    _log(db, user["id"], user["email"], user["role"], "DISCIPLINARY_ISSUED",
         {"employee": emp["name"], "type": data.get("type","")})
    return {"message": "Disciplinary record created", "id": did}

# ════════════════════════════════════════════════════════
# EXIT MANAGEMENT
# ════════════════════════════════════════════════════════
@app.get("/api/exit/")
def get_exits(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM exit_management ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/exit/")
def create_exit(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    emp = db.execute("SELECT name, department FROM employees WHERE id=?", (data.get("employee_id",""),)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    eid = _id()
    db.execute(
        "INSERT INTO exit_management (id,employee_id,employee_name,department,exit_type,exit_date,reason,processed_by) VALUES (?,?,?,?,?,?,?,?)",
        (eid, data["employee_id"], emp["name"], emp["department"],
         data.get("exit_type","Resignation"), data.get("exit_date",""),
         data.get("reason",""), user["email"])
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "EXIT_INITIATED",
         {"employee": emp["name"], "type": data.get("exit_type","")})
    return {"message": "Exit process initiated", "id": eid}

@app.patch("/api/exit/{exit_id}")
def update_exit(exit_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    fields = ["handover_done","equipment_returned","clearance_done",
              "final_payment_done","exit_interview_done","notes","status","exit_date"]
    updates = {k: v for k, v in data.items() if k in fields}
    if updates:
        # Auto complete if all steps done
        exit_rec = db.execute("SELECT * FROM exit_management WHERE id=?", (exit_id,)).fetchone()
        if exit_rec:
            merged = dict(exit_rec); merged.update(updates)
            if all(merged.get(f,0) for f in ["handover_done","equipment_returned",
                                               "clearance_done","final_payment_done","exit_interview_done"]):
                updates["status"] = "completed"
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE exit_management SET "+sets+" WHERE id=?", list(updates.values())+[exit_id])
        db.commit()
    return {"message": "Exit record updated"}

# ════════════════════════════════════════════════════════
# PERFORMANCE REVIEWS
# ════════════════════════════════════════════════════════
@app.get("/api/performance/")
def get_performance_reviews(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM performance_reviews ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/performance/me")
def my_performance(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute("SELECT * FROM performance_reviews WHERE employee_id=? ORDER BY created_at DESC", (emp["id"],)).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/performance/")
def create_review(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    emp = db.execute("SELECT name, department FROM employees WHERE id=?", (data.get("employee_id",""),)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    scores = [data.get("work_quality",3), data.get("punctuality",3),
              data.get("teamwork",3), data.get("communication",3), data.get("initiative",3)]
    overall = round(sum(scores)/len(scores), 1)
    rid = _id()
    db.execute(
        """INSERT INTO performance_reviews 
           (id,employee_id,employee_name,department,review_period,reviewer_id,reviewer_name,
            work_quality,punctuality,teamwork,communication,initiative,overall_score,
            strengths,improvements,goals,comments,status)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'published')""",
        (rid, data["employee_id"], emp["name"], emp["department"],
         data.get("review_period",""), user["id"], user.get("name","?"),
         data.get("work_quality",3), data.get("punctuality",3),
         data.get("teamwork",3), data.get("communication",3), data.get("initiative",3),
         overall, data.get("strengths",""), data.get("improvements",""),
         data.get("goals",""), data.get("comments",""))
    )
    db.commit()
    _notify(data["employee_id"], "Performance Review Published",
            "Your performance review for "+data.get("review_period","")+" is available", "in")
    return {"message": "Performance review created", "id": rid, "overall_score": overall}

# ════════════════════════════════════════════════════════
# JOB POSTINGS
# ════════════════════════════════════════════════════════
@app.get("/api/jobs/")
def get_jobs(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM job_postings ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/jobs/")
def create_job(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Job title required")
    jid = _id()
    db.execute(
        "INSERT INTO job_postings (id,title,department,type,location,description,requirements,salary_range,deadline,status,posted_by) VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        (jid, data["title"], data.get("department",""), data.get("type","Full-time"),
         data.get("location","Lagos"), data.get("description",""), data.get("requirements",""),
         data.get("salary_range",""), data.get("deadline",""), "open", user["email"])
    )
    db.commit()
    return {"message": "Job posting created", "id": jid}

@app.patch("/api/jobs/{job_id}")
def update_job(job_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    if "status" in data:
        db.execute("UPDATE job_postings SET status=? WHERE id=?", (data["status"], job_id))
        db.commit()
    return {"message": "Job updated"}

@app.get("/api/jobs/{job_id}/applications")
def job_applications(job_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    job = db.execute("SELECT title FROM job_postings WHERE id=?", (job_id,)).fetchone()
    if not job: raise HTTPException(404, "Job not found")
    rows = db.execute("SELECT * FROM job_applications WHERE role_applied=? ORDER BY created_at DESC",
                      (job["title"],)).fetchall()
    return [dict(r) for r in rows]

# ════════════════════════════════════════════════════════
# CONTRACT EXPIRY (uses employees table)
# ════════════════════════════════════════════════════════
@app.get("/api/employees/expiring-contracts")
def expiring_contracts(days: int = 30, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    import datetime as _dt
    cutoff = (_dt.datetime.now() + _dt.timedelta(days=days)).strftime("%Y-%m-%d")
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    rows = db.execute(
        "SELECT id, name, department, title, contract_end_date, join_date FROM employees WHERE contract_end_date IS NOT NULL AND contract_end_date != '' AND contract_end_date <= ? AND contract_end_date >= ? AND status='active' ORDER BY contract_end_date",
        (cutoff, today)
    ).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/employees/birthdays")
def upcoming_birthdays(days: int = 7, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    import datetime as _dt
    rows = db.execute("SELECT id, name, department, date_of_birth FROM employees WHERE date_of_birth IS NOT NULL AND date_of_birth != '' AND status='active'").fetchall()
    upcoming = []
    today = _dt.datetime.now()
    for emp in rows:
        try:
            dob = _dt.datetime.strptime(emp["date_of_birth"], "%Y-%m-%d")
            bday_this_year = dob.replace(year=today.year)
            if bday_this_year < today:
                bday_this_year = dob.replace(year=today.year+1)
            delta = (bday_this_year - today).days
            if 0 <= delta <= days:
                d = dict(emp)
                d["days_until"] = delta
                d["birthday"] = bday_this_year.strftime("%Y-%m-%d")
                upcoming.append(d)
        except:
            pass
    return sorted(upcoming, key=lambda x: x["days_until"])


# ════════════════════════════════════════════════════════
# IT ASSETS
# ════════════════════════════════════════════════════════
@app.get("/api/it/assets")
def get_assets(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM it_assets ORDER BY category, name").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/it/assets")
def create_asset(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    if not data.get("name"): raise HTTPException(400, "Asset name required")
    if not data.get("category"): raise HTTPException(400, "Category required")
    aid = _id()
    # Auto-generate asset tag
    count = db.execute("SELECT COUNT(*) as c FROM it_assets").fetchone()["c"]
    tag = "GE-" + (data.get("category","AST")[:3]).upper() + str(count+1).zfill(4)
    db.execute(
        """INSERT INTO it_assets (id,asset_tag,name,category,brand,model,serial_number,
           condition,status,purchase_date,purchase_price,warranty_expiry,location,notes,created_by)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (aid, data.get("asset_tag", tag), data["name"], data["category"],
         data.get("brand",""), data.get("model",""), data.get("serial_number",""),
         data.get("condition","good"), "available", data.get("purchase_date",""),
         data.get("purchase_price",0), data.get("warranty_expiry",""),
         data.get("location",""), data.get("notes",""), user["email"])
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "ASSET_CREATED",
         {"name": data["name"], "tag": tag})
    return {"message": "Asset added", "id": aid, "asset_tag": tag}

@app.post("/api/it/assets/{asset_id}/assign")
def assign_asset(asset_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    emp_id = data.get("employee_id","")
    emp = db.execute("SELECT name FROM employees WHERE id=?", (emp_id,)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    db.execute(
        "UPDATE it_assets SET status='assigned', assigned_to=?, assigned_to_name=?, assigned_date=? WHERE id=?",
        (emp_id, emp["name"], _now(), asset_id)
    )
    db.commit()
    _notify(emp_id, "Asset Assigned",
            "A " + (data.get("asset_name","device")) + " has been assigned to you", "in")
    _log(db, user["id"], user["email"], user["role"], "ASSET_ASSIGNED",
         {"asset_id": asset_id, "employee": emp["name"]})
    return {"message": "Asset assigned to " + emp["name"]}

@app.post("/api/it/assets/{asset_id}/return")
def return_asset(asset_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    db.execute(
        "UPDATE it_assets SET status='available', assigned_to=NULL, assigned_to_name=NULL, assigned_date=NULL WHERE id=?",
        (asset_id,)
    )
    db.commit()
    return {"message": "Asset returned to inventory"}

@app.patch("/api/it/assets/{asset_id}")
def update_asset(asset_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    allowed = ["name","category","brand","model","serial_number","condition","status",
               "location","notes","warranty_expiry","purchase_price"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE it_assets SET "+sets+" WHERE id=?", list(updates.values())+[asset_id])
        db.commit()
    return {"message": "Asset updated"}

@app.get("/api/it/assets/stats")
def asset_stats(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    total = db.execute("SELECT COUNT(*) as c FROM it_assets").fetchone()["c"]
    assigned = db.execute("SELECT COUNT(*) as c FROM it_assets WHERE status='assigned'").fetchone()["c"]
    available = db.execute("SELECT COUNT(*) as c FROM it_assets WHERE status='available'").fetchone()["c"]
    maintenance = db.execute("SELECT COUNT(*) as c FROM it_assets WHERE status='maintenance'").fetchone()["c"]
    by_category = db.execute("SELECT category, COUNT(*) as count FROM it_assets GROUP BY category").fetchall()
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    cutoff = (_dt.datetime.now() + _dt.timedelta(days=90)).strftime("%Y-%m-%d")
    warranty_expiring = db.execute(
        "SELECT COUNT(*) as c FROM it_assets WHERE warranty_expiry IS NOT NULL AND warranty_expiry != '' AND warranty_expiry <= ? AND warranty_expiry >= ?",
        (cutoff, today)
    ).fetchone()["c"]
    return {
        "total": total, "assigned": assigned, "available": available,
        "maintenance": maintenance, "warranty_expiring": warranty_expiring,
        "by_category": [dict(r) for r in by_category]
    }

# ════════════════════════════════════════════════════════
# SOFTWARE LICENSES
# ════════════════════════════════════════════════════════
@app.get("/api/it/licenses")
def get_licenses(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    rows = db.execute("SELECT * FROM software_licenses ORDER BY name").fetchall()
    result = []
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    cutoff30 = (_dt.datetime.now() + _dt.timedelta(days=30)).strftime("%Y-%m-%d")
    for r in rows:
        d = dict(r)
        if r["expiry_date"] and r["expiry_date"] <= today:
            d["expiry_status"] = "expired"
        elif r["expiry_date"] and r["expiry_date"] <= cutoff30:
            d["expiry_status"] = "expiring_soon"
        else:
            d["expiry_status"] = "ok"
        result.append(d)
    return result

@app.post("/api/it/licenses")
def create_license(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    if not data.get("name"): raise HTTPException(400, "License name required")
    lid = _id()
    db.execute(
        """INSERT INTO software_licenses (id,name,vendor,license_key,seats,seats_used,
           expiry_date,cost,category,status,notes,created_by) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (lid, data["name"], data.get("vendor",""), data.get("license_key",""),
         data.get("seats",1), data.get("seats_used",0), data.get("expiry_date",""),
         data.get("cost",0), data.get("category","Productivity"), "active",
         data.get("notes",""), user["email"])
    )
    db.commit()
    return {"message": "License added", "id": lid}

@app.patch("/api/it/licenses/{license_id}")
def update_license(license_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    allowed = ["name","vendor","seats","seats_used","expiry_date","cost","category","status","notes"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE software_licenses SET "+sets+" WHERE id=?", list(updates.values())+[license_id])
        db.commit()
    return {"message": "License updated"}

# ════════════════════════════════════════════════════════
# IT REMOTE NOTES
# ════════════════════════════════════════════════════════
@app.get("/api/it/remote-notes")
def get_remote_notes(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    rows = db.execute("SELECT * FROM it_remote_notes ORDER BY created_at DESC LIMIT 100").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/it/remote-notes")
def create_remote_note(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    if not data.get("work_done"): raise HTTPException(400, "Work done description required")
    nid = _id()
    emp_name = ""
    if data.get("employee_id"):
        emp = db.execute("SELECT name FROM employees WHERE id=?", (data["employee_id"],)).fetchone()
        emp_name = emp["name"] if emp else ""
    db.execute(
        """INSERT INTO it_remote_notes (id,ticket_id,employee_id,employee_name,
           session_type,work_done,duration_minutes,technician_id,technician_name)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (nid, data.get("ticket_id",""), data.get("employee_id",""), emp_name,
         data.get("session_type","Remote"), data["work_done"],
         data.get("duration_minutes",0), user["id"], user.get("name",user["email"]))
    )
    db.commit()
    return {"message": "Session note saved", "id": nid}

# ════════════════════════════════════════════════════════
# IT TICKET SLA
# ════════════════════════════════════════════════════════
@app.get("/api/it/tickets/overdue")
def overdue_tickets(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    import datetime as _dt
    rows = db.execute(
        "SELECT * FROM it_tickets WHERE status IN ('open','in_progress') ORDER BY created_at"
    ).fetchall()
    sla_hours = {"urgent": 4, "high": 8, "medium": 24, "low": 48}
    now = _dt.datetime.now()
    overdue = []
    for t in rows:
        try:
            created = _dt.datetime.strptime(t["created_at"][:19], "%Y-%m-%d %H:%M:%S")
            hours_open = (now - created).total_seconds() / 3600
            sla = sla_hours.get(t["priority"], 24)
            if hours_open > sla:
                d = dict(t)
                d["hours_open"] = round(hours_open, 1)
                d["sla_hours"] = sla
                d["hours_overdue"] = round(hours_open - sla, 1)
                overdue.append(d)
        except:
            pass
    return sorted(overdue, key=lambda x: x["hours_overdue"], reverse=True)

@app.get("/api/it/tickets/stats")
def ticket_stats(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    total = db.execute("SELECT COUNT(*) as c FROM it_tickets").fetchone()["c"]
    open_t = db.execute("SELECT COUNT(*) as c FROM it_tickets WHERE status='open'").fetchone()["c"]
    in_prog = db.execute("SELECT COUNT(*) as c FROM it_tickets WHERE status='in_progress'").fetchone()["c"]
    resolved = db.execute("SELECT COUNT(*) as c FROM it_tickets WHERE status='resolved'").fetchone()["c"]
    by_priority = db.execute("SELECT priority, COUNT(*) as count FROM it_tickets WHERE status != 'resolved' GROUP BY priority").fetchall()
    by_dept = db.execute("SELECT department, COUNT(*) as count FROM it_tickets GROUP BY department ORDER BY count DESC LIMIT 5").fetchall()
    return {
        "total": total, "open": open_t, "in_progress": in_prog, "resolved": resolved,
        "by_priority": [dict(r) for r in by_priority],
        "by_dept": [dict(r) for r in by_dept]
    }


# ════════════════════════════════════════════════════════
# MEETINGS
# ════════════════════════════════════════════════════════
@app.get("/api/meetings/")
def get_meetings(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute(
        "SELECT * FROM meetings ORDER BY scheduled_date DESC, scheduled_time DESC"
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/meetings/")
def create_meeting(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Meeting title required")
    if not data.get("scheduled_date"): raise HTTPException(400, "Scheduled date required")
    mid = _id()
    db.execute(
        """INSERT INTO meetings (id,title,description,organizer_id,organizer_name,department,
           meeting_type,scheduled_date,scheduled_time,duration_minutes,location,attendees,agenda,status)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'scheduled')""",
        (mid, data["title"], data.get("description",""),
         user["id"], user.get("name", user["email"]),
         data.get("department",""), data.get("meeting_type","Team Meeting"),
         data["scheduled_date"], data.get("scheduled_time","09:00"),
         data.get("duration_minutes",60), data.get("location","Conference Room"),
         data.get("attendees",""), data.get("agenda",""))
    )
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "MEETING_CREATED",
         {"title": data["title"], "date": data["scheduled_date"]})
    return {"message": "Meeting scheduled", "id": mid}

@app.patch("/api/meetings/{meeting_id}")
def update_meeting(meeting_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    allowed = ["title","description","scheduled_date","scheduled_time","duration_minutes",
               "location","attendees","agenda","minutes","status"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE meetings SET "+sets+" WHERE id=?", list(updates.values())+[meeting_id])
        db.commit()
    return {"message": "Meeting updated"}

# ════════════════════════════════════════════════════════
# DEPARTMENT TARGETS
# ════════════════════════════════════════════════════════
@app.get("/api/targets/")
def get_targets(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM dept_targets ORDER BY period DESC, department").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/targets/")
def create_target(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Target title required")
    if not data.get("period"): raise HTTPException(400, "Period required")
    tid = _id()
    db.execute(
        """INSERT INTO dept_targets (id,department,title,description,target_value,current_value,
           unit,period,status,set_by) VALUES (?,?,?,?,?,?,?,?,?,?)""",
        (tid, data.get("department",""),data["title"],data.get("description",""),
         data.get("target_value",0), data.get("current_value",0),
         data.get("unit","units"), data["period"], "active", user["email"])
    )
    db.commit()
    return {"message": "Target set", "id": tid}

@app.patch("/api/targets/{target_id}")
def update_target(target_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    allowed = ["title","description","target_value","current_value","unit","status"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        updates["updated_at"] = _now()
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE dept_targets SET "+sets+" WHERE id=?", list(updates.values())+[target_id])
        db.commit()
    return {"message": "Target updated"}

# ════════════════════════════════════════════════════════
# OVERTIME REQUESTS
# ════════════════════════════════════════════════════════
@app.get("/api/overtime/")
def get_overtime(status: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if status:
        rows = db.execute(
            "SELECT * FROM overtime_requests WHERE status=? ORDER BY created_at DESC", (status,)
        ).fetchall()
    else:
        rows = db.execute("SELECT * FROM overtime_requests ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/overtime/me")
def my_overtime(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute(
        "SELECT * FROM overtime_requests WHERE employee_id=? ORDER BY created_at DESC", (emp["id"],)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/overtime/")
def submit_overtime(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute(
        "SELECT id, name, department, salary FROM employees WHERE user_id=?", (user["id"],)
    ).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    hours = data.get("hours", 0)
    if hours <= 0: raise HTTPException(400, "Hours must be greater than 0")
    hourly_rate = round((emp["salary"] or 0) / 160, 2)
    total = round(hourly_rate * hours * 1.5, 2)
    oid = _id()
    db.execute(
        """INSERT INTO overtime_requests (id,employee_id,employee_name,department,date,hours,
           reason,hourly_rate,total_amount,status) VALUES (?,?,?,?,?,?,?,?,?,'pending')""",
        (oid, emp["id"], emp["name"], emp["department"],
         data.get("date", _now()[:10]), hours,
         data.get("reason",""), hourly_rate, total)
    )
    db.commit()
    return {"message": "Overtime submitted", "id": oid, "total_amount": total, "hourly_rate": hourly_rate}

@app.post("/api/overtime/{ot_id}/action")
def action_overtime(ot_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    approved = data.get("approved", False)
    status = "approved" if approved else "rejected"
    db.execute(
        "UPDATE overtime_requests SET status=?, approved_by=?, approver_name=?, rejection_note=? WHERE id=?",
        (status, user["id"], user.get("name","?"),
         data.get("note") if not approved else None, ot_id)
    )
    db.commit()
    ot = db.execute("SELECT * FROM overtime_requests WHERE id=?", (ot_id,)).fetchone()
    if ot:
        _notify(ot["employee_id"],
                "Overtime " + ("Approved ✅" if approved else "Rejected ❌"),
                str(ot["hours"]) + "hrs overtime on " + ot["date"] + " was " + status,
                "ok" if approved else "er")
    return {"message": "Overtime " + status}

# ════════════════════════════════════════════════════════
# TASK PERFORMANCE STATS
# ════════════════════════════════════════════════════════
@app.get("/api/tasks/stats")
def task_stats(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    total = db.execute("SELECT COUNT(*) as c FROM tasks").fetchone()["c"]
    done = db.execute("SELECT COUNT(*) as c FROM tasks WHERE status='done'").fetchone()["c"]
    pending = db.execute("SELECT COUNT(*) as c FROM tasks WHERE status='pending'").fetchone()["c"]
    in_prog = db.execute("SELECT COUNT(*) as c FROM tasks WHERE status='in-progress'").fetchone()["c"]
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    overdue = db.execute(
        "SELECT COUNT(*) as c FROM tasks WHERE due_date < ? AND status != 'done'", (today,)
    ).fetchone()["c"]
    by_emp = db.execute(
        """SELECT assigned_to_name, 
           COUNT(*) as total,
           SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as completed,
           SUM(CASE WHEN due_date < ? AND status != 'done' THEN 1 ELSE 0 END) as overdue
           FROM tasks GROUP BY assigned_to_name ORDER BY total DESC LIMIT 10""",
        (today,)
    ).fetchall()
    return {
        "total": total, "done": done, "pending": pending,
        "in_progress": in_prog, "overdue": overdue,
        "completion_rate": round(done/total*100, 1) if total > 0 else 0,
        "by_employee": [dict(r) for r in by_emp]
    }

# ════════════════════════════════════════════════════════
# APPROVAL DELEGATIONS
# ════════════════════════════════════════════════════════
@app.get("/api/delegations/")
def get_delegations(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute(
        "SELECT * FROM approval_delegations WHERE delegator_id=? ORDER BY created_at DESC",
        (user["id"],)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/delegations/")
def create_delegation(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    delegate = db.execute("SELECT name FROM employees WHERE id=?", (data.get("delegate_id",""),)).fetchone()
    if not delegate: raise HTTPException(404, "Delegate not found")
    did = _id()
    db.execute(
        """INSERT INTO approval_delegations (id,delegator_id,delegator_name,delegate_id,
           delegate_name,approval_types,from_date,to_date,reason,status) VALUES (?,?,?,?,?,?,?,?,?,'active')""",
        (did, user["id"], user.get("name", user["email"]),
         data["delegate_id"], delegate["name"],
         data.get("approval_types","leave,budget"),
         data.get("from_date",""), data.get("to_date",""), data.get("reason",""))
    )
    db.commit()
    return {"message": "Delegation created", "id": did}

@app.delete("/api/delegations/{delegation_id}")
def revoke_delegation(delegation_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    db.execute("UPDATE approval_delegations SET status='revoked' WHERE id=? AND delegator_id=?",
               (delegation_id, user["id"]))
    db.commit()
    return {"message": "Delegation revoked"}


# ════════════════════════════════════════════════════════
# COLLEAGUE DIRECTORY
# ════════════════════════════════════════════════════════
@app.get("/api/employees/directory")
def employee_directory(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    rows = db.execute(
        """SELECT e.name, e.department, e.title, e.role, e.email, e.phone,
                  e.join_date, e.color
           FROM employees e
           WHERE e.status='active'
           ORDER BY e.department, e.name"""
    ).fetchall()
    return [dict(r) for r in rows]

# ════════════════════════════════════════════════════════
# COMPANY EVENTS / CALENDAR
# ════════════════════════════════════════════════════════
@app.get("/api/events/")
def get_events(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    rows = db.execute(
        "SELECT * FROM company_events ORDER BY date"
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/events/")
def create_event(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Event title required")
    if not data.get("date"): raise HTTPException(400, "Date required")
    eid = _id()
    db.execute(
        """INSERT INTO company_events (id,title,event_type,date,end_date,description,
           is_public_holiday,created_by) VALUES (?,?,?,?,?,?,?,?)""",
        (eid, data["title"], data.get("event_type","event"),
         data["date"], data.get("end_date",""),
         data.get("description",""),
         1 if data.get("is_public_holiday") else 0,
         user["email"])
    )
    db.commit()
    return {"message": "Event created", "id": eid}

@app.delete("/api/events/{event_id}")
def delete_event(event_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    db.execute("DELETE FROM company_events WHERE id=?", (event_id,))
    db.commit()
    return {"message": "Event deleted"}

# ════════════════════════════════════════════════════════
# EMPLOYEE DOCUMENTS
# ════════════════════════════════════════════════════════
@app.get("/api/documents/me")
def my_documents(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute(
        "SELECT * FROM employee_documents WHERE employee_id=? ORDER BY created_at DESC",
        (emp["id"],)
    ).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/documents/{employee_id}")
def employee_documents(employee_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    rows = db.execute(
        "SELECT * FROM employee_documents WHERE employee_id=? ORDER BY created_at DESC",
        (employee_id,)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/documents/")
def add_document(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    did = _id()
    db.execute(
        """INSERT INTO employee_documents (id,employee_id,title,doc_type,file_note,uploaded_by)
           VALUES (?,?,?,?,?,?)""",
        (did, data.get("employee_id",""), data.get("title",""),
         data.get("doc_type","Other"), data.get("file_note",""), user["email"])
    )
    db.commit()
    return {"message": "Document record added", "id": did}

@app.delete("/api/documents/{doc_id}")
def delete_document(doc_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    db.execute("DELETE FROM employee_documents WHERE id=?", (doc_id,))
    db.commit()
    return {"message": "Document removed"}

# ════════════════════════════════════════════════════════
# TRAINING REQUESTS
# ════════════════════════════════════════════════════════
@app.get("/api/training/")
def get_training(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute("SELECT * FROM training_requests ORDER BY created_at DESC").fetchall()
    return [dict(r) for r in rows]

@app.get("/api/training/me")
def my_training(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute(
        "SELECT * FROM training_requests WHERE employee_id=? ORDER BY created_at DESC",
        (emp["id"],)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/training/")
def request_training(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute(
        "SELECT id, name, department FROM employees WHERE user_id=?", (user["id"],)
    ).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    if not data.get("course_title"): raise HTTPException(400, "Course title required")
    tid = _id()
    db.execute(
        """INSERT INTO training_requests (id,employee_id,employee_name,department,
           course_title,provider,cost,duration,reason,status)
           VALUES (?,?,?,?,?,?,?,?,?,'pending')""",
        (tid, emp["id"], emp["name"], emp["department"],
         data["course_title"], data.get("provider",""),
         data.get("cost",0), data.get("duration",""),
         data.get("reason",""))
    )
    db.commit()
    return {"message": "Training request submitted", "id": tid}

@app.post("/api/training/{training_id}/action")
def action_training(training_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    approved = data.get("approved", False)
    status = "approved" if approved else "rejected"
    db.execute(
        "UPDATE training_requests SET status=?, approved_by=?, approver_name=?, rejection_note=? WHERE id=?",
        (status, user["id"], user.get("name","?"),
         data.get("note") if not approved else None, training_id)
    )
    db.commit()
    tr = db.execute("SELECT * FROM training_requests WHERE id=?", (training_id,)).fetchone()
    if tr:
        _notify(tr["employee_id"],
                "Training Request " + ("Approved ✅" if approved else "Rejected ❌"),
                "Your request for " + tr["course_title"] + " was " + status,
                "ok" if approved else "er")
    return {"message": "Training request " + status}

# ════════════════════════════════════════════════════════
# PAYSLIP HISTORY (last 12 months)
# ════════════════════════════════════════════════════════
@app.get("/api/payroll/my-history")
def my_payroll_history(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    rows = db.execute(
        """SELECT * FROM payroll_records WHERE employee_id=? 
           ORDER BY created_at DESC LIMIT 12""",
        (emp["id"],)
    ).fetchall()
    return [dict(r) for r in rows]

# ════════════════════════════════════════════════════════
# UPDATE EMPLOYEE PROFILE (extended fields)
# ════════════════════════════════════════════════════════
@app.patch("/api/employees/me/profile")
def update_my_profile(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: raise HTTPException(404, "Employee not found")
    allowed = ["phone","address","emergency_contact_name","emergency_contact_phone",
               "emergency_contact_relation","date_of_birth"]
    updates = {k: v for k, v in data.items() if k in allowed}
    if updates:
        updates["updated_at"] = _now()
        sets = ", ".join(k+"=?" for k in updates)
        db.execute("UPDATE employees SET "+sets+" WHERE id=?",
                   list(updates.values()) + [emp["id"]])
        db.commit()
    return {"message": "Profile updated successfully"}

# ════════════════════════════════════════════════════════
# ANONYMOUS COMPLAINTS
# ════════════════════════════════════════════════════════
@app.post("/api/complaints/anonymous")
def submit_anonymous_complaint(data: dict, db=Depends(get_db)):
    if not data.get("subject"): raise HTTPException(400, "Subject required")
    if not data.get("detail"): raise HTTPException(400, "Details required")
    cid = _id()
    db.execute(
        """INSERT INTO complaints (id,subject,detail,filer_id,filer_name,
           against_name,department,severity,status)
           VALUES (?,?,?,?,?,?,?,?,'open')""",
        (cid, data["subject"], data["detail"],
         "anonymous", "Anonymous", data.get("against_name",""),
         data.get("department",""), data.get("severity","medium"))
    )
    db.commit()
    return {"message": "Complaint submitted anonymously", "id": cid}


# ════════════════════════════════════════════════════════
# ANNOUNCEMENT READ RECEIPTS
# ════════════════════════════════════════════════════════
@app.post("/api/announcements/{ann_id}/read")
def mark_announcement_read(ann_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    emp = db.execute("SELECT id, name FROM employees WHERE user_id=?", (user["id"],)).fetchone()
    if not emp: return {"message": "ok"}
    try:
        db.execute(
            "INSERT OR IGNORE INTO announcement_reads (id,announcement_id,employee_id,employee_name) VALUES (?,?,?,?)",
            (_id(), ann_id, emp["id"], emp["name"])
        )
        db.commit()
    except:
        pass
    return {"message": "Marked as read"}

@app.get("/api/announcements/{ann_id}/reads")
def get_announcement_reads(ann_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    reads = db.execute(
        "SELECT employee_name, read_at FROM announcement_reads WHERE announcement_id=? ORDER BY read_at",
        (ann_id,)
    ).fetchall()
    ann = db.execute("SELECT * FROM announcements WHERE id=?", (ann_id,)).fetchone()
    total_staff = db.execute("SELECT COUNT(*) as c FROM employees WHERE status='active'").fetchone()["c"]
    return {
        "announcement": dict(ann) if ann else {},
        "reads": [dict(r) for r in reads],
        "read_count": len(reads),
        "total_staff": total_staff
    }

# ════════════════════════════════════════════════════════
# EMERGENCY BROADCAST
# ════════════════════════════════════════════════════════
@app.post("/api/announcements/emergency")
def emergency_broadcast(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager"], db)
    if not data.get("message"): raise HTTPException(400, "Message required")
    # Post as urgent announcement
    ann_id = _id()
    db.execute(
        "INSERT INTO announcements (id,title,body,type,department,posted_by,poster_name) VALUES (?,?,?,'er','all',?,?)",
        (ann_id, "🚨 EMERGENCY: " + data.get("title","Urgent Notice"),
         data["message"], user["id"], user.get("name", user["email"]))
    )
    # Notify ALL active employees
    emps = db.execute("SELECT id FROM employees WHERE status='active'").fetchall()
    for emp in emps:
        _notify(emp["id"], "🚨 Emergency Alert",
                data.get("title","Urgent Notice") + ": " + data["message"][:100], "er")
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "EMERGENCY_BROADCAST",
         {"title": data.get("title",""), "recipients": len(emps)})
    return {"message": f"Emergency broadcast sent to {len(emps)} staff", "announcement_id": ann_id}

# ════════════════════════════════════════════════════════
# NOTICE BOARD
# ════════════════════════════════════════════════════════
@app.get("/api/notice-board/")
def get_notices(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = db.execute(
        """SELECT * FROM notice_board 
           WHERE active=1 AND (expires_at IS NULL OR expires_at='' OR expires_at > ?)
           ORDER BY priority DESC, created_at DESC""",
        (today,)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/notice-board/")
def create_notice(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    if not data.get("title"): raise HTTPException(400, "Title required")
    if not data.get("content"): raise HTTPException(400, "Content required")
    nid = _id()
    db.execute(
        "INSERT INTO notice_board (id,title,content,priority,pinned_by,expires_at) VALUES (?,?,?,?,?,?)",
        (nid, data["title"], data["content"],
         data.get("priority", 0), user["email"],
         data.get("expires_at", ""))
    )
    db.commit()
    return {"message": "Notice posted", "id": nid}

@app.delete("/api/notice-board/{notice_id}")
def delete_notice(notice_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    db.execute("UPDATE notice_board SET active=0 WHERE id=?", (notice_id,))
    db.commit()
    return {"message": "Notice removed"}

# ════════════════════════════════════════════════════════
# INTERNAL MESSAGING
# ════════════════════════════════════════════════════════
@app.get("/api/messages/rooms")
def get_rooms(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    rows = db.execute(
        "SELECT * FROM message_rooms WHERE participants LIKE ? ORDER BY last_message_at DESC",
        ("%" + user["id"] + "%",)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/messages/rooms")
def create_room(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    participants = data.get("participants", [])
    if user["id"] not in participants:
        participants.append(user["id"])
    # For DMs check if room already exists
    if data.get("room_type","direct") == "direct" and len(participants) == 2:
        existing = db.execute(
            "SELECT id FROM message_rooms WHERE room_type='direct' AND participants LIKE ? AND participants LIKE ?",
            ("%" + participants[0] + "%", "%" + participants[1] + "%")
        ).fetchone()
        if existing:
            return {"message": "Room exists", "room_id": existing["id"]}
    rid = _id()
    db.execute(
        "INSERT INTO message_rooms (id,name,room_type,participants,created_by,last_message_at) VALUES (?,?,?,?,?,?)",
        (rid, data.get("name",""), data.get("room_type","direct"),
         ",".join(participants), user["id"], _now())
    )
    db.commit()
    return {"message": "Room created", "room_id": rid}

@app.get("/api/messages/{room_id}")
def get_messages(room_id: str, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    rows = db.execute(
        "SELECT * FROM messages WHERE room_id=? ORDER BY created_at ASC LIMIT 100",
        (room_id,)
    ).fetchall()
    return [dict(r) for r in rows]

@app.post("/api/messages/{room_id}")
def send_message(room_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    if not data.get("content"): raise HTTPException(400, "Message content required")
    mid = _id()
    db.execute(
        "INSERT INTO messages (id,room_id,sender_id,sender_name,content) VALUES (?,?,?,?,?)",
        (mid, room_id, user["id"], user.get("name",user["email"]), data["content"])
    )
    db.execute(
        "UPDATE message_rooms SET last_message=?, last_message_at=? WHERE id=?",
        (data["content"][:50], _now(), room_id)
    )
    db.commit()
    return {"message": "Message sent", "id": mid}

@app.get("/api/messages/unread/count")
def unread_messages(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["employee","dept_manager","manager","admin"], db)
    # Simple count of rooms with recent activity not by user
    count = db.execute(
        "SELECT COUNT(*) as c FROM message_rooms WHERE participants LIKE ? AND last_message_at > datetime('now','-1 day')",
        ("%" + user["id"] + "%",)
    ).fetchone()["c"]
    return {"count": count}


# ════════════════════════════════════════════════════════
# ENHANCED REPORTS & ANALYTICS
# ════════════════════════════════════════════════════════
@app.get("/api/reports/headcount-trend")
def headcount_trend(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute(
        """SELECT strftime('%Y-%m', created_at) as month, COUNT(*) as count
           FROM employees GROUP BY month ORDER BY month DESC LIMIT 12"""
    ).fetchall()
    by_dept = db.execute(
        "SELECT department, COUNT(*) as count FROM employees WHERE status='active' GROUP BY department ORDER BY count DESC"
    ).fetchall()
    by_role = db.execute(
        "SELECT role, COUNT(*) as count FROM employees WHERE status='active' GROUP BY role ORDER BY count DESC"
    ).fetchall()
    return {
        "monthly": [dict(r) for r in reversed(rows)],
        "by_department": [dict(r) for r in by_dept],
        "by_role": [dict(r) for r in by_role]
    }

@app.get("/api/reports/attendance-trends")
def attendance_trends(months: int = 3, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    rows = db.execute(
        """SELECT date, 
           SUM(CASE WHEN status='present' THEN 1 ELSE 0 END) as present,
           SUM(CASE WHEN status='late' THEN 1 ELSE 0 END) as late,
           SUM(CASE WHEN status='absent' THEN 1 ELSE 0 END) as absent
           FROM attendance 
           WHERE date >= date('now', ? || ' months')
           GROUP BY date ORDER BY date""",
        (str(-months),)
    ).fetchall()
    # Late by employee
    late_leaders = db.execute(
        """SELECT employee_name, COUNT(*) as late_count 
           FROM attendance WHERE status='late' AND date >= date('now', '-90 days')
           GROUP BY employee_name ORDER BY late_count DESC LIMIT 10"""
    ).fetchall()
    # Present rate by dept
    dept_rates = db.execute(
        """SELECT department, 
           ROUND(100.0 * SUM(CASE WHEN status='present' THEN 1 ELSE 0 END) / COUNT(*), 1) as rate
           FROM attendance WHERE date >= date('now', '-30 days')
           GROUP BY department ORDER BY rate DESC"""
    ).fetchall()
    return {
        "daily": [dict(r) for r in rows],
        "late_leaders": [dict(r) for r in late_leaders],
        "dept_rates": [dict(r) for r in dept_rates]
    }

@app.get("/api/reports/payroll-trends")
def payroll_trends(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    # Monthly payroll cost from records
    monthly = db.execute(
        """SELECT period, SUM(gross) as gross, SUM(net) as net, SUM(tax) as tax,
           SUM(allowances) as allowances, SUM(bonuses) as bonuses, COUNT(*) as employees
           FROM payroll_records GROUP BY period ORDER BY created_at DESC LIMIT 12"""
    ).fetchall()
    # Salary by dept
    by_dept = db.execute(
        """SELECT department, SUM(salary) as total, AVG(salary) as avg, COUNT(*) as count
           FROM employees WHERE status='active' AND salary > 0
           GROUP BY department ORDER BY total DESC"""
    ).fetchall()
    # Top earners
    top = db.execute(
        "SELECT name, department, salary FROM employees WHERE status='active' ORDER BY salary DESC LIMIT 10"
    ).fetchall()
    return {
        "monthly": [dict(r) for r in reversed(list(monthly))],
        "by_department": [dict(r) for r in by_dept],
        "top_earners": [dict(r) for r in top]
    }

@app.get("/api/reports/leave-analysis")
def leave_analysis(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    by_type = db.execute(
        "SELECT type, COUNT(*) as count, SUM(days) as total_days FROM leave_requests GROUP BY type ORDER BY count DESC"
    ).fetchall()
    by_status = db.execute(
        "SELECT status, COUNT(*) as count FROM leave_requests GROUP BY status"
    ).fetchall()
    by_dept = db.execute(
        """SELECT department, COUNT(*) as requests, SUM(days) as total_days
           FROM leave_requests WHERE status='approved'
           GROUP BY department ORDER BY total_days DESC"""
    ).fetchall()
    monthly = db.execute(
        """SELECT strftime('%Y-%m', created_at) as month, COUNT(*) as count
           FROM leave_requests GROUP BY month ORDER BY month DESC LIMIT 12"""
    ).fetchall()
    return {
        "by_type": [dict(r) for r in by_type],
        "by_status": [dict(r) for r in by_status],
        "by_department": [dict(r) for r in by_dept],
        "monthly": [dict(r) for r in reversed(list(monthly))]
    }

@app.get("/api/reports/task-analytics")
def task_analytics(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    import datetime as _dt
    today = _dt.datetime.now().strftime("%Y-%m-%d")
    by_status = db.execute(
        "SELECT status, COUNT(*) as count FROM tasks GROUP BY status"
    ).fetchall()
    by_priority = db.execute(
        "SELECT priority, COUNT(*) as count FROM tasks GROUP BY priority ORDER BY count DESC"
    ).fetchall()
    by_dept = db.execute(
        "SELECT department, COUNT(*) as count, SUM(CASE WHEN status='done' THEN 1 ELSE 0 END) as done FROM tasks GROUP BY department ORDER BY count DESC"
    ).fetchall()
    overdue = db.execute(
        "SELECT COUNT(*) as c FROM tasks WHERE due_date < ? AND status != 'done'", (today,)
    ).fetchone()["c"]
    completion_rate = db.execute(
        "SELECT ROUND(100.0*SUM(CASE WHEN status='done' THEN 1 ELSE 0 END)/COUNT(*),1) as rate FROM tasks"
    ).fetchone()
    return {
        "by_status": [dict(r) for r in by_status],
        "by_priority": [dict(r) for r in by_priority],
        "by_department": [dict(r) for r in by_dept],
        "overdue": overdue,
        "completion_rate": completion_rate["rate"] if completion_rate else 0
    }

@app.get("/api/reports/finance-overview")
def finance_overview(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","manager","dept_manager"], db)
    budget_total = db.execute("SELECT SUM(amount) as t FROM budget_requests WHERE status='approved'").fetchone()
    expense_total = db.execute("SELECT SUM(amount) as t FROM expense_claims WHERE status='approved'").fetchone()
    transfer_total = db.execute("SELECT SUM(amount) as t FROM fund_transfers").fetchone()
    advance_total = db.execute("SELECT SUM(amount) as t FROM salary_advances WHERE status='approved'").fetchone()
    by_dept = db.execute(
        "SELECT department, SUM(amount) as total FROM budget_requests WHERE status='approved' GROUP BY department ORDER BY total DESC"
    ).fetchall()
    monthly_exp = db.execute(
        """SELECT strftime('%Y-%m', created_at) as month, SUM(amount) as total
           FROM expense_claims WHERE status='approved'
           GROUP BY month ORDER BY month DESC LIMIT 6"""
    ).fetchall()
    return {
        "approved_budgets": budget_total["t"] or 0,
        "approved_expenses": expense_total["t"] or 0,
        "total_transfers": transfer_total["t"] or 0,
        "total_advances": advance_total["t"] or 0,
        "budget_by_dept": [dict(r) for r in by_dept],
        "monthly_expenses": [dict(r) for r in reversed(list(monthly_exp))]
    }

@app.get("/api/reports/it-overview")
def it_overview(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    ticket_stats = db.execute(
        "SELECT status, COUNT(*) as count FROM it_tickets GROUP BY status"
    ).fetchall()
    by_priority = db.execute(
        "SELECT priority, COUNT(*) as count FROM it_tickets WHERE status != 'resolved' GROUP BY priority ORDER BY count DESC"
    ).fetchall()
    by_dept = db.execute(
        "SELECT department, COUNT(*) as count FROM it_tickets GROUP BY department ORDER BY count DESC LIMIT 8"
    ).fetchall()
    asset_stats = db.execute(
        "SELECT status, COUNT(*) as count FROM it_assets GROUP BY status"
    ).fetchall()
    license_count = db.execute("SELECT COUNT(*) as c FROM software_licenses WHERE status='active'").fetchone()["c"]
    return {
        "ticket_by_status": [dict(r) for r in ticket_stats],
        "ticket_by_priority": [dict(r) for r in by_priority],
        "tickets_by_dept": [dict(r) for r in by_dept],
        "asset_by_status": [dict(r) for r in asset_stats],
        "active_licenses": license_count
    }

@app.get("/api/reports/export/employees")
def export_employees(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    rows = db.execute(
        """SELECT e.name, e.email, e.phone, e.department, e.role, e.title,
                  e.salary, e.contract_type, e.join_date, e.status,
                  u.last_login
           FROM employees e LEFT JOIN users u ON e.user_id=u.id
           ORDER BY e.department, e.name"""
    ).fetchall()
    return [dict(r) for r in rows]

@app.get("/api/reports/export/attendance")
def export_attendance(month: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    if month:
        rows = db.execute(
            "SELECT * FROM attendance WHERE date LIKE ? ORDER BY date, employee_name",
            (month + "%",)
        ).fetchall()
    else:
        rows = db.execute(
            "SELECT * FROM attendance ORDER BY date DESC, employee_name LIMIT 1000"
        ).fetchall()
    return [dict(r) for r in rows]


# ════════════════════════════════════════════════════════
# INTEGRATIONS - PAYSTACK
# ════════════════════════════════════════════════════════
@app.post("/api/integrations/paystack/payment-link")
def create_paystack_link(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager","manager"], db)
    import urllib.request, json as _json
    paystack_key = db.execute(
        "SELECT value FROM portal_settings WHERE key='paystack_secret_key'"
    ).fetchone()
    if not paystack_key or not paystack_key["value"]:
        raise HTTPException(400, "Paystack secret key not configured. Add it in Admin > Integrations.")
    amount = int(data.get("amount", 0) * 100)  # Paystack uses kobo
    if amount <= 0:
        raise HTTPException(400, "Invalid amount")
    payload = _json.dumps({
        "amount": amount,
        "currency": "NGN",
        "email": data.get("email", user["email"]),
        "metadata": {
            "department": data.get("department", ""),
            "purpose": data.get("purpose", ""),
            "approved_by": user["email"]
        }
    }).encode()
    try:
        req = urllib.request.Request(
            "https://api.paystack.co/transaction/initialize",
            data=payload,
            headers={
                "Authorization": f"Bearer {paystack_key['value']}",
                "Content-Type": "application/json"
            }
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = _json.loads(resp.read())
        if result.get("status"):
            _log(db, user["id"], user["email"], user["role"], "PAYSTACK_LINK_CREATED",
                 {"amount": data.get("amount"), "department": data.get("department","")})
            return {"payment_url": result["data"]["authorization_url"], "reference": result["data"]["reference"]}
        else:
            raise HTTPException(400, result.get("message","Paystack error"))
    except Exception as e:
        raise HTTPException(400, f"Paystack connection failed: {str(e)}")

@app.post("/api/integrations/paystack/webhook")
def paystack_webhook(request_body: dict, db=Depends(get_db)):
    event = request_body.get("event","")
    data = request_body.get("data",{})
    if event == "charge.success":
        ref = data.get("reference","")
        amount = data.get("amount",0) / 100
        _log(db, "system", "paystack@webhook", "system", "PAYSTACK_PAYMENT_CONFIRMED",
             {"reference": ref, "amount": amount, "status": "success"})
    return {"status": "ok"}

@app.get("/api/integrations/settings")
def get_integration_settings(authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    keys = ["paystack_secret_key","paystack_public_key","twilio_account_sid",
            "twilio_auth_token","twilio_whatsapp_from","smtp_host","smtp_user",
            "smtp_password","company_tin","company_nhis_code","company_nsitf_code"]
    result = {}
    for key in keys:
        row = db.execute("SELECT value FROM portal_settings WHERE key=?", (key,)).fetchone()
        val = row["value"] if row else ""
        # Mask sensitive values
        if "key" in key or "password" in key or "token" in key or "secret" in key:
            result[key] = "****" + val[-4:] if val and len(val) > 4 else ("set" if val else "")
        else:
            result[key] = val or ""
    return result

@app.post("/api/integrations/settings")
def update_integration_settings(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    allowed_keys = ["paystack_secret_key","paystack_public_key","twilio_account_sid",
                    "twilio_auth_token","twilio_whatsapp_from","smtp_host","smtp_user",
                    "smtp_password","company_tin","company_nhis_code","company_nsitf_code"]
    updated = []
    for key, value in data.items():
        if key in allowed_keys and value and not value.startswith("****"):
            existing = db.execute("SELECT key FROM portal_settings WHERE key=?", (key,)).fetchone()
            if existing:
                db.execute("UPDATE portal_settings SET value=?, updated_by=?, updated_at=? WHERE key=?",
                           (value, user["email"], _now(), key))
            else:
                db.execute("INSERT INTO portal_settings (key,value,updated_by,updated_at) VALUES (?,?,?,?)",
                           (key, value, user["email"], _now()))
            updated.append(key)
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "INTEGRATION_SETTINGS_UPDATED", {"keys": updated})
    return {"message": f"Updated {len(updated)} integration settings"}

# ════════════════════════════════════════════════════════
# STATUTORY REPORTS (NHIS, PAYE, PENSION, ITF, NSITF)
# ════════════════════════════════════════════════════════
@app.get("/api/reports/statutory/paye")
def paye_report(month: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    from datetime import datetime as _datetime
    if not month:
        month = _datetime.now().strftime("%Y-%m")
    emps = db.execute(
        "SELECT name, department, salary FROM employees WHERE status='active' AND salary > 0 ORDER BY department, name"
    ).fetchall()
    rows = []
    for emp in emps:
        gross = emp["salary"]
        tax = round(gross * 0.20, 2)
        rows.append({
            "employee_name": emp["name"],
            "department": emp["department"],
            "gross_income": gross,
            "paye_tax": tax,
            "month": month
        })
    total_paye = sum(r["paye_tax"] for r in rows)
    return {"month": month, "employees": rows, "total_paye": total_paye, "employee_count": len(rows)}

@app.get("/api/reports/statutory/pension")
def pension_report(month: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    from datetime import datetime as _datetime
    if not month:
        month = _datetime.now().strftime("%Y-%m")
    emps = db.execute(
        "SELECT name, department, salary FROM employees WHERE status='active' AND salary > 0 ORDER BY name"
    ).fetchall()
    rows = []
    for emp in emps:
        gross = emp["salary"]
        employee_contrib = round(gross * 0.08, 2)
        employer_contrib = round(gross * 0.10, 2)
        rows.append({
            "employee_name": emp["name"],
            "department": emp["department"],
            "gross_salary": gross,
            "employee_contribution_8pct": employee_contrib,
            "employer_contribution_10pct": employer_contrib,
            "total_contribution": employee_contrib + employer_contrib
        })
    return {
        "month": month,
        "employees": rows,
        "total_employee": sum(r["employee_contribution_8pct"] for r in rows),
        "total_employer": sum(r["employer_contribution_10pct"] for r in rows),
        "grand_total": sum(r["total_contribution"] for r in rows)
    }

@app.get("/api/reports/statutory/nhis")
def nhis_report(month: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    from datetime import datetime as _datetime
    if not month:
        month = _datetime.now().strftime("%Y-%m")
    emps = db.execute(
        "SELECT name, department, salary FROM employees WHERE status='active' AND salary > 0"
    ).fetchall()
    rows = []
    for emp in emps:
        employee_contrib = round(emp["salary"] * 0.05 / 2, 2)
        employer_contrib = round(emp["salary"] * 0.05 / 2 * 1.5, 2)
        rows.append({
            "employee_name": emp["name"],
            "department": emp["department"],
            "monthly_salary": emp["salary"],
            "employee_nhis": employee_contrib,
            "employer_nhis": employer_contrib
        })
    return {"month": month, "employees": rows,
            "total_employee": sum(r["employee_nhis"] for r in rows),
            "total_employer": sum(r["employer_nhis"] for r in rows)}

# ════════════════════════════════════════════════════════
# BIOMETRIC ATTENDANCE IMPORT
# ════════════════════════════════════════════════════════
@app.post("/api/attendance/bulk-import")
def bulk_import_attendance(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    records = data.get("records", [])
    if not records:
        raise HTTPException(400, "No records provided")
    imported = 0
    errors = []
    for rec in records:
        try:
            emp_name = rec.get("employee_name","").strip()
            date = rec.get("date","").strip()
            clock_in = rec.get("clock_in","").strip()
            clock_out = rec.get("clock_out","").strip()
            if not emp_name or not date:
                errors.append(f"Missing name or date: {rec}")
                continue
            emp = db.execute("SELECT id, department FROM employees WHERE name=? COLLATE NOCASE", (emp_name,)).fetchone()
            if not emp:
                errors.append(f"Employee not found: {emp_name}")
                continue
            # Determine status
            status = "absent"
            if clock_in:
                try:
                    h, m = map(int, clock_in.split(":")[:2])
                    status = "late" if (h > 9 or (h == 9 and m > 0)) else "present"
                except:
                    status = "present"
            # Check existing
            existing = db.execute("SELECT id FROM attendance WHERE employee_id=? AND date=?", (emp["id"], date)).fetchone()
            if existing:
                db.execute("UPDATE attendance SET clock_in=?, clock_out=?, status=? WHERE employee_id=? AND date=?",
                           (clock_in, clock_out, status, emp["id"], date))
            else:
                db.execute(
                    "INSERT INTO attendance (id,employee_id,employee_name,department,date,clock_in,clock_out,status) VALUES (?,?,?,?,?,?,?,?)",
                    (_id(), emp["id"], emp_name, emp["department"], date, clock_in, clock_out, status)
                )
            imported += 1
        except Exception as e:
            errors.append(str(e))
    db.commit()
    _log(db, user["id"], user["email"], user["role"], "BULK_ATTENDANCE_IMPORT",
         {"imported": imported, "errors": len(errors)})
    return {"message": f"Imported {imported} records", "errors": errors[:10], "imported": imported}

# ════════════════════════════════════════════════════════
# BANK TRANSFER FILE (for payroll disbursement)
# ════════════════════════════════════════════════════════
@app.get("/api/payroll/bank-transfer-file")
def bank_transfer_file(period: str = None, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin","dept_manager"], db)
    from datetime import datetime as _datetime
    if not period:
        period = _datetime.now().strftime("%B %Y")
    emps = db.execute(
        """SELECT e.name, e.department, u.email,
                  e.salary,
                  (e.salary - ROUND(e.salary*0.20,2) - ROUND(e.salary*0.08,2) - ROUND(e.salary*0.05,2)) as net_pay
           FROM employees e LEFT JOIN users u ON e.user_id=u.id
           WHERE e.status='active' AND e.salary > 0
           ORDER BY e.department, e.name"""
    ).fetchall()
    import hashlib
    rows = []
    for emp in emps:
        ref = "GE-" + period.replace(" ","-").upper() + "-" + hashlib.md5(emp["name"].encode()).hexdigest()[:6].upper()
        rows.append({
            "beneficiary_name": emp["name"],
            "department": emp["department"],
            "net_salary": round(emp["net_pay"] or 0, 2),
            "payment_reference": ref,
            "narration": f"Salary {period} - {emp['name']}"
        })
    total = sum(r["net_salary"] for r in rows)
    return {
        "period": period,
        "generated_at": _now(),
        "generated_by": user["email"],
        "total_amount": total,
        "employee_count": len(rows),
        "transfers": rows
    }

# ── PORTAL SETTINGS (Admin only) ──
@app.get("/api/portal/status")
def get_portal_status(db=Depends(get_db)):
    settings = db.execute("SELECT key, value FROM portal_settings").fetchall()
    result = {row["key"]: row["value"] for row in settings}
    return result

@app.post("/api/portal/toggle")
def toggle_portal(data: dict, authorization: str = Header(None), db=Depends(get_db)):
    user = _require_role(authorization, ["admin"], db)
    is_open = data.get("open", True)
    message = data.get("message", "The portal is currently closed for maintenance.")
    db.execute("UPDATE portal_settings SET value=?, updated_by=?, updated_at=? WHERE key='portal_open'",
               ("1" if is_open else "0", user["email"], _now()))
    db.execute("UPDATE portal_settings SET value=?, updated_by=?, updated_at=? WHERE key='portal_closed_message'",
               (message, user["email"], _now()))
    db.commit()
    action = "PORTAL_OPENED" if is_open else "PORTAL_CLOSED"
    _log(db, user["id"], user["email"], user["role"], action, {"message": message})
    return {"message": f"Portal {'opened' if is_open else 'closed'} successfully"}

# ── ACCOUNT LOCK (IT/Admin only) ──
@app.post("/api/users/{user_id}/lock")
def lock_account(user_id: str, data: dict, authorization: str = Header(None), db=Depends(get_db)):
    actor = _require_role(authorization, ["admin", "dept_manager"], db)
    reason = data.get("reason", "Account locked by IT Support. Please contact IT to resolve.")
    db.execute("UPDATE users SET is_locked=1, lock_reason=?, locked_by=?, locked_at=? WHERE id=?",
               (reason, actor["email"], _now(), user_id))
    db.execute("UPDATE sessions SET is_active=0, force_logout=1, logout_reason='Account locked' WHERE user_id=?", (user_id,))
    db.commit()
    target = db.execute("SELECT email FROM users WHERE id=?", (user_id,)).fetchone()
    _log(db, actor["id"], actor["email"], actor["role"], "ACCOUNT_LOCKED", 
         {"locked_user": target["email"] if target else user_id, "reason": reason})
    return {"message": "Account locked and user logged out"}

@app.post("/api/users/{user_id}/unlock")
def unlock_account(user_id: str, authorization: str = Header(None), db=Depends(get_db)):
    actor = _require_role(authorization, ["admin", "dept_manager"], db)
    db.execute("UPDATE users SET is_locked=0, lock_reason=NULL, locked_by=NULL, locked_at=NULL, login_attempts=0, locked_until=NULL WHERE id=?", (user_id,))
    db.commit()
    target = db.execute("SELECT email FROM users WHERE id=?", (user_id,)).fetchone()
    _log(db, actor["id"], actor["email"], actor["role"], "ACCOUNT_UNLOCKED",
         {"unlocked_user": target["email"] if target else user_id})
    return {"message": "Account unlocked successfully"}

@app.get("/api/users/locked")
def get_locked_users(authorization: str = Header(None), db=Depends(get_db)):
    actor = _require_role(authorization, ["admin", "dept_manager"], db)
    rows = db.execute("""SELECT u.id, u.email, u.role, u.department, u.is_locked, 
                          u.lock_reason, u.locked_by, u.locked_at,
                          e.name FROM users u LEFT JOIN employees e ON u.id=e.user_id
                          WHERE u.is_locked=1""").fetchall()
    return [dict(r) for r in rows]

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


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main_single:app", host="0.0.0.0", port=port)
