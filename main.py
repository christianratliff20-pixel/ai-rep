"""
main.py — FastAPI root

Rename this file to main.py before deploying.

Install deps:
  pip install -r requirements.txt

Run locally:
  uvicorn main:app --reload --port 8000

Deploy on Render:
  Build command: pip install -r requirements.txt
  Start command: uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import os
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from auth import router as auth_router
from calls import router as calls_router
from database import engine, Base
from seed import seed_database


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create all DB tables if they don't exist
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Seed operator account + state compliance data
    # Safe to run on every startup — checks if already exists first
    await seed_database()

    yield
    # Shutdown: nothing to clean up


app = FastAPI(
    title="AI Receptionist API",
    version="1.0.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────
# Set FRONTEND_URL in your environment to your Netlify/Bolt URL
# Never use "*" in production
FRONTEND_URL = os.environ.get("FRONTEND_URL", "http://localhost:3000")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        FRONTEND_URL,
        "http://localhost:3000",
        "http://localhost:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── ROUTERS ───────────────────────────────────────────────────
app.include_router(auth_router, prefix="/auth", tags=["auth"])
app.include_router(calls_router, tags=["calls"])


@app.get("/health")
async def health():
    """Render pings this to verify the service is up."""
    return {"status": "ok", "service": "AI Receptionist API"}
