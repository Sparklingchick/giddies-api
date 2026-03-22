import sqlite3
import os
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", "giddies.db")

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()

    c.execute("""CREATE TABLE IF NOT EXISTS users (
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
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS employees (
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
        avatar TEXT,
        color TEXT DEFAULT '#FF6B00',
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
        token TEXT UNIQUE NOT NULL,
        ip_address TEXT,
        user_agent TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        expires_at TEXT NOT NULL,
        is_active INTEGER DEFAULT 1,
        force_logout INTEGER DEFAULT 0,
        logout_reason TEXT
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS activity_log (
        id TEXT PRIMARY KEY,
        user_id TEXT,
        user_name TEXT,
        user_role TEXT,
        action TEXT NOT NULL,
        resource TEXT,
        resource_id TEXT,
        details TEXT,
        ip_address TEXT,
        status TEXT DEFAULT 'success',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS user_requests (
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
        hr_note TEXT,
        created_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS departments (
        id TEXT PRIMARY KEY,
        name TEXT UNIQUE NOT NULL,
        icon TEXT DEFAULT '🏢',
        head_user_id TEXT,
        head_name TEXT,
        budget REAL DEFAULT 0,
        spent REAL DEFAULT 0,
        description TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS attendance (
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
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS leave_requests (
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
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS budget_requests (
        id TEXT PRIMARY KEY,
        department TEXT NOT NULL,
        title TEXT,
        amount REAL NOT NULL,
        reason TEXT,
        submitted_by TEXT,
        submitter_name TEXT,
        status TEXT DEFAULT 'pending',
        manager_approved_by TEXT,
        admin_approved_by TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS fund_transfers (
        id TEXT PRIMARY KEY,
        from_dept TEXT DEFAULT 'Finance',
        to_dept TEXT NOT NULL,
        amount REAL NOT NULL,
        type TEXT,
        note TEXT,
        reference TEXT,
        sent_by TEXT,
        sent_by_name TEXT,
        status TEXT DEFAULT 'pending',
        approved_by TEXT,
        approver_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS spending_requests (
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
        approver_name TEXT,
        rejection_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS announcements (
        id TEXT PRIMARY KEY,
        title TEXT NOT NULL,
        body TEXT NOT NULL,
        type TEXT DEFAULT 'info',
        department TEXT DEFAULT 'all',
        posted_by TEXT,
        poster_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS notifications (
        id TEXT PRIMARY KEY,
        employee_id TEXT NOT NULL,
        title TEXT NOT NULL,
        message TEXT,
        type TEXT DEFAULT 'in',
        read INTEGER DEFAULT 0,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS it_tickets (
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
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY,
        room_id TEXT NOT NULL,
        sender_id TEXT NOT NULL,
        sender_name TEXT,
        content TEXT NOT NULL,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS tasks (
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
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS payroll_records (
        id TEXT PRIMARY KEY,
        employee_id TEXT REFERENCES employees(id),
        period TEXT NOT NULL,
        gross REAL DEFAULT 0,
        tax REAL DEFAULT 0,
        ni REAL DEFAULT 0,
        pension REAL DEFAULT 0,
        net REAL DEFAULT 0,
        paid_date TEXT,
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS supply_requests (
        id TEXT PRIMARY KEY,
        item_name TEXT NOT NULL,
        quantity INTEGER DEFAULT 1,
        reason TEXT,
        department TEXT,
        requested_by TEXT,
        requester_name TEXT,
        urgency TEXT DEFAULT 'Normal',
        status TEXT DEFAULT 'pending',
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS complaints (
        id TEXT PRIMARY KEY,
        subject TEXT NOT NULL,
        detail TEXT,
        filer_id TEXT,
        filer_name TEXT,
        against_id TEXT,
        against_name TEXT,
        department TEXT,
        severity TEXT DEFAULT 'medium',
        status TEXT DEFAULT 'open',
        hr_note TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    c.execute("""CREATE TABLE IF NOT EXISTS job_applications (
        id TEXT PRIMARY KEY,
        full_name TEXT NOT NULL,
        email TEXT,
        phone TEXT,
        role_applied TEXT,
        department TEXT,
        cover_letter TEXT,
        status TEXT DEFAULT 'pending',
        reviewed_by TEXT,
        reviewer_name TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")

    conn.commit()

    # ── SEED: ONLY admin, everything else starts empty ──
    count = c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if count == 0:
        seed_admin(conn)

    # Seed default departments (no staff, just the structure)
    dept_count = c.execute("SELECT COUNT(*) FROM departments").fetchone()[0]
    if dept_count == 0:
        seed_departments(conn)

    conn.close()
    print("✅ Database ready")

def seed_admin(conn):
    import uuid
    from auth_utils import hash_password
    c = conn.cursor()
    
    uid = str(uuid.uuid4())
    eid = str(uuid.uuid4())
    
    # Admin user - email can be anything, role determines access
    c.execute("""INSERT INTO users (id,email,password_hash,role,department,is_active)
                 VALUES (?,?,?,?,?,1)""",
              (uid, "admin@giddiesexpress.com", hash_password("Admin@1234"),
               "admin", "Administration"))
    
    c.execute("""INSERT INTO employees (id,user_id,name,email,department,role,title,status,color)
                 VALUES (?,?,?,?,?,?,?,'active','#F59E0B')""",
              (eid, uid, "System Administrator", "admin@giddiesexpress.com",
               "Administration", "admin", "System Administrator"))
    
    conn.commit()
    print("✅ Admin account created")
    print("   Email: admin@giddiesexpress.com")
    print("   Password: Admin@1234")
    print("   CHANGE THIS PASSWORD IMMEDIATELY AFTER FIRST LOGIN")

def seed_departments(conn):
    import uuid
    c = conn.cursor()
    depts = [
        ("HR", "👥"), ("Finance", "💰"), ("IT Support", "💻"),
        ("Payroll", "🏦"), ("Logistics", "🚚"), ("Warehouse", "📦"),
        ("Customer Service", "🤝"), ("Marketing", "📣"), ("Legal", "⚖️"),
        ("Management", "🏢"), ("Administration", "⚙️"), ("Recruitment", "📢"),
    ]
    for name, icon in depts:
        c.execute("INSERT OR IGNORE INTO departments (id,name,icon) VALUES (?,?,?)",
                  (str(uuid.uuid4()), name, icon))
    conn.commit()
    print(f"✅ {len(depts)} departments created")
