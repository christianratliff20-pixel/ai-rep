"""
main.py — FastAPI root

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
from billing import router as billing_router
from database import engine, Base
from seed import seed_database


@asynccontextmanager
async def lifespan(app: FastAPI):
    # 1. Create all DB tables if they don't exist (includes new billing tables)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # 2. Seed operator account + state compliance data
    await seed_database()

    yield


app = FastAPI(
    title="AI Receptionist API",
    version="1.1.0",
    lifespan=lifespan,
)

# ── CORS ──────────────────────────────────────────────────────
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
app.include_router(billing_router, tags=["billing"])


@app.get("/health")
async def health():
    return {"status": "ok", "service": "AI Receptionist API"}
