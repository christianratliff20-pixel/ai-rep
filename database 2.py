"""
database.py — SQLAlchemy async ORM models

These are the Python representations of the tables in schema.sql.
FastAPI endpoints import get_db as a dependency to get a session.

Usage in any endpoint:
    from database import get_db, Organization, User
    
    @router.get("/something")
    async def something(db: AsyncSession = Depends(get_db)):
        result = await db.execute(select(User).where(User.email == email))
        user = result.scalar_one_or_none()
"""

import os
from datetime import datetime, timezone
from typing import AsyncGenerator
from uuid import uuid4

from sqlalchemy import (
    Boolean, Column, DateTime, Float, ForeignKey,
    Integer, String, Text, UniqueConstraint, event
)
from sqlalchemy.dialects.postgresql import INET, JSONB, UUID
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, relationship

# ── DATABASE ENGINE ────────────────────────────────────────────
# asyncpg driver — required for async SQLAlchemy with PostgreSQL
# DATABASE_URL must use postgresql+asyncpg:// scheme
DATABASE_URL = os.environ["DATABASE_URL"]

# If your DATABASE_URL starts with "postgres://" (Render/Heroku format),
# convert it to the asyncpg format automatically
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

engine = create_async_engine(
    DATABASE_URL,
    echo=os.environ.get("SQL_ECHO", "false").lower() == "true",  # set SQL_ECHO=true to log all queries
    pool_size=10,          # number of persistent connections in pool
    max_overflow=20,       # additional connections allowed beyond pool_size under load
    pool_pre_ping=True,    # verify connections are alive before using (prevents stale connection errors)
    pool_recycle=3600,     # recycle connections every hour (prevents PostgreSQL idle timeout kills)
)

SessionLocal = async_sessionmaker(
    engine,
    class_=AsyncSession,
    expire_on_commit=False,  # prevent "DetachedInstanceError" after commit in async context
)


class Base(DeclarativeBase):
    pass


# ── DEPENDENCY ─────────────────────────────────────────────────
async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """
    FastAPI dependency that yields a database session.
    Session is automatically committed and closed after the request.
    If an exception occurs, the transaction is rolled back.
    
    Usage: db: AsyncSession = Depends(get_db)
    """
    async with SessionLocal() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()


# ── MODELS ─────────────────────────────────────────────────────

class Organization(Base):
    __tablename__ = "organizations"

    id                      = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    name                    = Column(Text, nullable=False)
    business_type           = Column(Text, nullable=False, default="hvac")
    plan                    = Column(Text, nullable=False, default="trial")
    plan_status             = Column(Text, nullable=False, default="active")
    stripe_customer_id      = Column(Text, unique=True)
    stripe_subscription_id  = Column(Text, unique=True)
    trial_ends_at           = Column(DateTime(timezone=True))
    onboarding_complete     = Column(Boolean, nullable=False, default=False)
    onboarding_step         = Column(Integer, nullable=False, default=1)
    settings                = Column(JSONB, nullable=False, default=dict)
    twilio_phone_number     = Column(Text)
    twilio_phone_sid        = Column(Text)
    created_at              = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at              = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    # Relationships
    users           = relationship("User", back_populates="org", cascade="all, delete-orphan")
    knowledge_base  = relationship("KnowledgeBaseChunk", back_populates="org", cascade="all, delete-orphan")
    call_logs       = relationship("CallLog", back_populates="org", cascade="all, delete-orphan")
    callback_queue  = relationship("CallbackQueue", back_populates="org", cascade="all, delete-orphan")
    integrations    = relationship("Integration", back_populates="org", cascade="all, delete-orphan")


class User(Base):
    __tablename__ = "users"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    org_id          = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    email           = Column(Text, nullable=False, unique=True)
    password_hash   = Column(Text, nullable=False)
    name            = Column(Text, nullable=False)
    role            = Column(Text, nullable=False, default="owner")
    is_active       = Column(Boolean, nullable=False, default=True)
    last_login_at   = Column(DateTime(timezone=True))
    invited_by      = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    invite_token    = Column(Text, unique=True)
    invite_accepted = Column(Boolean, nullable=False, default=False)
    created_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    # Relationships
    org             = relationship("Organization", back_populates="users")
    refresh_tokens  = relationship("RefreshToken", back_populates="user", cascade="all, delete-orphan")


class RefreshToken(Base):
    __tablename__ = "refresh_tokens"

    id          = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id     = Column(UUID(as_uuid=True), ForeignKey("users.id", ondelete="CASCADE"), nullable=False)
    token_hash  = Column(Text, nullable=False, unique=True)
    expires_at  = Column(DateTime(timezone=True), nullable=False)
    revoked     = Column(Boolean, nullable=False, default=False)
    revoked_at  = Column(DateTime(timezone=True))
    user_agent  = Column(Text)
    ip_address  = Column(INET)
    created_at  = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    user = relationship("User", back_populates="refresh_tokens")


class KnowledgeBaseChunk(Base):
    __tablename__ = "knowledge_base"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    org_id          = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    section         = Column(Text, nullable=False)
    text            = Column(Text, nullable=False)
    source          = Column(Text, nullable=False, default="manual")
    source_url      = Column(Text)
    confidence      = Column(Float, nullable=False, default=1.0)
    needs_review    = Column(Boolean, nullable=False, default=False)
    conflict_with   = Column(UUID(as_uuid=True), ForeignKey("knowledge_base.id"))
    is_locked       = Column(Boolean, nullable=False, default=False)
    is_active       = Column(Boolean, nullable=False, default=True)
    version         = Column(Integer, nullable=False, default=1)
    created_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    org = relationship("Organization", back_populates="knowledge_base")


class CallLog(Base):
    __tablename__ = "call_logs"

    id                  = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    org_id              = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    call_sid            = Column(Text, unique=True, nullable=False)
    caller_number       = Column(Text, nullable=False)
    caller_name         = Column(Text)
    direction           = Column(Text, nullable=False, default="inbound")
    status              = Column(Text, nullable=False, default="active")
    outcome             = Column(Text)
    intent              = Column(Text)
    duration_seconds    = Column(Integer)
    recording_url       = Column(Text)
    transcript          = Column(JSONB, nullable=False, default=list)
    summary             = Column(Text)
    sentiment           = Column(Text, default="neutral")
    lead_score          = Column(Integer)
    competitor_mentioned = Column(Text)
    follow_up_tasks     = Column(JSONB, default=list)
    whispers            = Column(JSONB, default=list)
    is_test_call        = Column(Boolean, nullable=False, default=False)
    started_at          = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    ended_at            = Column(DateTime(timezone=True))
    created_at          = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    org = relationship("Organization", back_populates="call_logs")


class CallbackQueue(Base):
    __tablename__ = "callback_queue"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    org_id          = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    call_log_id     = Column(UUID(as_uuid=True), ForeignKey("call_logs.id"))
    caller_name     = Column(Text)
    caller_number   = Column(Text, nullable=False)
    reason          = Column(Text)
    best_time       = Column(Text)
    priority        = Column(Text, nullable=False, default="medium")
    status          = Column(Text, nullable=False, default="pending")
    assigned_to     = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    completed_at    = Column(DateTime(timezone=True))
    completed_by    = Column(UUID(as_uuid=True), ForeignKey("users.id"))
    created_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    org = relationship("Organization", back_populates="callback_queue")


class Integration(Base):
    __tablename__ = "integrations"

    id              = Column(UUID(as_uuid=True), primary_key=True, default=uuid4)
    org_id          = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False)
    provider        = Column(Text, nullable=False)
    status          = Column(Text, nullable=False, default="active")
    config          = Column(JSONB, nullable=False, default=dict)
    last_sync_at    = Column(DateTime(timezone=True))
    created_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at      = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))

    __table_args__ = (UniqueConstraint("org_id", "provider"),)

    org = relationship("Organization", back_populates="integrations")
