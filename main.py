from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
from database import init_db
from routes import auth, users, employees, attendance, leave, budgets, announcements, notifications, it_support, departments, reports, messages, tasks

app = FastAPI(title="Giddies Express API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()

@app.get("/")
def root():
    return {"status": "Giddies Express API is running", "version": "1.0.0"}

@app.get("/health")
def health():
    return {"status": "ok"}

app.include_router(auth.router, prefix="/api/auth", tags=["Auth"])
app.include_router(users.router, prefix="/api/users", tags=["Users"])
app.include_router(employees.router, prefix="/api/employees", tags=["Employees"])
app.include_router(attendance.router, prefix="/api/attendance", tags=["Attendance"])
app.include_router(leave.router, prefix="/api/leave", tags=["Leave"])
app.include_router(budgets.router, prefix="/api/budgets", tags=["Budgets"])
app.include_router(announcements.router, prefix="/api/announcements", tags=["Announcements"])
app.include_router(notifications.router, prefix="/api/notifications", tags=["Notifications"])
app.include_router(it_support.router, prefix="/api/it", tags=["IT Support"])
app.include_router(departments.router, prefix="/api/departments", tags=["Departments"])
app.include_router(reports.router, prefix="/api/reports", tags=["Reports"])
app.include_router(messages.router, prefix="/api/messages", tags=["Messages"])
app.include_router(tasks.router, prefix="/api/tasks", tags=["Tasks"])

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
