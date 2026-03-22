# Giddies Express API

## Setup
1. Push this folder to GitHub
2. Deploy to Railway.app — it auto-detects Python
3. Set environment variable: SECRET_KEY=your-secret

## Local Testing
```bash
pip install -r requirements.txt
python main.py
```
API runs at: http://localhost:8000
Docs at: http://localhost:8000/docs

## Endpoints
- POST /api/auth/login
- POST /api/auth/logout  
- GET  /api/employees/
- POST /api/attendance/clock-in
- POST /api/leave/
- GET  /api/reports/dashboard-stats
- GET  /api/it/system-health
- POST /api/users/request-new-user
- POST /api/users/create
- POST /api/users/{id}/suspend
- POST /api/users/{id}/reset-password
- GET  /api/users/active-sessions
- GET  /api/it/logs
