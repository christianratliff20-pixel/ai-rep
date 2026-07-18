"""
auth.py — Real authentication backend for AI Receptionist SaaS

What this does:
  - Signup creates a real org + user row in PostgreSQL
  - Login verifies bcrypt password hash, issues JWT access token + refresh token
  - Access tokens expire in 15 minutes (short, so compromised tokens die fast)
  - Refresh tokens expire in 30 days, stored hashed in DB
  - /auth/me returns the current user from the JWT — this is how the
    frontend knows who you are on page reload
  - /auth/refresh issues a new access token using a valid refresh token
  - /auth/logout revokes the refresh token in the database
  - Every protected endpoint calls get_current_user() as a FastAPI dependency

Paste this into your FastAPI project and add to main.py:
  from auth import router as auth_router
  app.include_router(auth_router, prefix="/auth")

REQUIRED ENV VARS:
  DATABASE_URL=postgresql+asyncpg://user:pass@host:5432/dbname
  JWT_SECRET=<64-character random string>  # python -c "import secrets; print(secrets.token_hex(32))"
  JWT_ALGORITHM=HS256
  ACCESS_TOKEN_EXPIRE_MINUTES=15
  REFRESH_TOKEN_EXPIRE_DAYS=30
  FRONTEND_URL=https://your-frontend.netlify.app  (for CORS)
"""

import hashlib
import os
import secrets
import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError, jwt
from pydantic import BaseModel, EmailStr, field_validator
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, Organization, User, RefreshToken

router = APIRouter()
bearer_scheme = HTTPBearer(auto_error=False)

# ── CONFIG ────────────────────────────────────────────────────
JWT_SECRET              = os.environ["JWT_SECRET"]
JWT_ALGORITHM           = os.environ.get("JWT_ALGORITHM", "HS256")
ACCESS_TOKEN_MINUTES    = int(os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", "15"))
REFRESH_TOKEN_DAYS      = int(os.environ.get("REFRESH_TOKEN_EXPIRE_DAYS", "30"))


# ══════════════════════════════════════════════════════════════
# REQUEST / RESPONSE MODELS
# ══════════════════════════════════════════════════════════════

class SignupRequest(BaseModel):
    owner_name: str
    owner_email: EmailStr
    password: str
    organization_name: str
    business_type: str = "hvac"

    @field_validator("password")
    @classmethod
    def password_strength(cls, v):
        if len(v) < 8:
            raise ValueError("Password must be at least 8 characters")
        return v

    @field_validator("owner_name", "organization_name")
    @classmethod
    def not_empty(cls, v):
        if not v.strip():
            raise ValueError("This field cannot be empty")
        return v.strip()


class LoginRequest(BaseModel):
    email: EmailStr
    password: str


class RefreshRequest(BaseModel):
    refresh_token: str


class UserOut(BaseModel):
    id: str
    name: str
    email: str
    role: str
    org_id: str
    org_name: str
    plan: str
    onboarding_complete: bool
    onboarding_step: int

    class Config:
        from_attributes = True


class AuthResponse(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    expires_in: int  # seconds
    user: UserOut


# ══════════════════════════════════════════════════════════════
# PASSWORD HASHING
# bcrypt with cost factor 12 — slow enough to resist brute force,
# fast enough not to impact UX (< 200ms per hash)
# ══════════════════════════════════════════════════════════════

def hash_password(password: str) -> str:
    """Hash a plaintext password. Never call this with an already-hashed value."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode("utf-8"), salt).decode("utf-8")


def verify_password(password: str, hashed: str) -> bool:
    """Verify a plaintext password against its stored bcrypt hash."""
    try:
        return bcrypt.checkpw(password.encode("utf-8"), hashed.encode("utf-8"))
    except Exception:
        return False


# ══════════════════════════════════════════════════════════════
# JWT TOKENS
# Access token: short-lived, carries user identity + role
# Refresh token: long-lived, opaque random bytes stored hashed in DB
# ══════════════════════════════════════════════════════════════

def create_access_token(user_id: str, org_id: str, role: str) -> str:
    """
    Creates a signed JWT access token.
    Payload includes user_id, org_id, role, and expiry.
    The frontend sends this as: Authorization: Bearer <token>
    """
    now = datetime.now(timezone.utc)
    payload = {
        "sub":    user_id,           # subject — always the user ID
        "org":    org_id,            # organization ID
        "role":   role,              # owner, manager, receptionist, billing_contact, operator
        "iat":    now,               # issued at
        "exp":    now + timedelta(minutes=ACCESS_TOKEN_MINUTES),
        "type":   "access",
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)


def create_refresh_token() -> tuple[str, str]:
    """
    Creates a cryptographically random refresh token.
    Returns (raw_token, hashed_token).
    raw_token is sent to the client and never stored.
    hashed_token is stored in the database.
    This means a database breach can't be used to replay refresh tokens.
    """
    raw = secrets.token_urlsafe(64)
    hashed = hashlib.sha256(raw.encode()).hexdigest()
    return raw, hashed


def decode_access_token(token: str) -> dict:
    """
    Decodes and validates a JWT access token.
    Raises HTTPException 401 if invalid, expired, or wrong type.
    """
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        if payload.get("type") != "access":
            raise HTTPException(status_code=401, detail="Invalid token type")
        return payload
    except JWTError as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalid or expired",
            headers={"WWW-Authenticate": "Bearer"},
        )


# ══════════════════════════════════════════════════════════════
# DEPENDENCY — get_current_user
# Every protected endpoint uses this as a FastAPI Depends().
# It extracts the JWT from the Authorization header and
# returns the full User + Organization objects.
# ══════════════════════════════════════════════════════════════

async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(bearer_scheme),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """
    FastAPI dependency. Verifies the Bearer token and returns
    the current user and their organization.

    Usage in any endpoint:
        @router.get("/my-endpoint")
        async def my_endpoint(current_user = Depends(get_current_user)):
            org_id = current_user["org"].id
            user_role = current_user["user"].role
    """
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentication required")

    payload = decode_access_token(credentials.credentials)
    user_id = payload.get("sub")
    org_id = payload.get("org")

    if not user_id or not org_id:
        raise HTTPException(status_code=401, detail="Malformed token")

    # Load user from DB — this validates the user still exists and is active
    user = await db.get(User, user_id)
    if not user or not user.is_active:
        raise HTTPException(status_code=401, detail="User not found or deactivated")

    org = await db.get(Organization, org_id)
    if not org:
        raise HTTPException(status_code=401, detail="Organization not found")

    return {"user": user, "org": org}


def require_role(*roles: str):
    """
    Role-based access control dependency factory.
    Usage:
        @router.delete("/users/{user_id}")
        async def delete_user(..., current = Depends(require_role("owner", "operator"))):
    """
    async def _check(current_user=Depends(get_current_user)):
        if current_user["user"].role not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires one of these roles: {', '.join(roles)}"
            )
        return current_user
    return _check


def _build_user_out(user: User, org: Organization) -> UserOut:
    """Converts DB objects into the API response model."""
    return UserOut(
        id=str(user.id),
        name=user.name,
        email=user.email,
        role=user.role,
        org_id=str(org.id),
        org_name=org.name,
        plan=org.plan,
        onboarding_complete=org.onboarding_complete,
        onboarding_step=org.onboarding_step,
    )


# ══════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════

@router.post("/signup", response_model=AuthResponse, status_code=201)
async def signup(body: SignupRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """
    Creates a new organization and owner user.

    Steps:
    1. Check email isn't already registered
    2. Create organization row
    3. Create user row with bcrypt-hashed password
    4. Issue access token + refresh token
    5. Return tokens + user object to frontend
    """
    # 1. Check for duplicate email
    existing = await db.execute(select(User).where(User.email == body.owner_email.lower()))
    if existing.scalar_one_or_none():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="An account with this email already exists. Try logging in."
        )

    # 2. Create organization
    org = Organization(
        id=uuid.uuid4(),
        name=body.organization_name.strip(),
        business_type=body.business_type,
        plan="trial",
        plan_status="active",
        onboarding_complete=False,
        onboarding_step=1,
        settings={},
    )
    db.add(org)

    # 3. Create owner user
    user = User(
        id=uuid.uuid4(),
        org_id=org.id,
        email=body.owner_email.lower().strip(),
        password_hash=hash_password(body.password),
        name=body.owner_name.strip(),
        role="owner",
        is_active=True,
    )
    db.add(user)

    # 4. Create refresh token record
    raw_refresh, hashed_refresh = create_refresh_token()
    refresh_record = RefreshToken(
        id=uuid.uuid4(),
        user_id=user.id,
        token_hash=hashed_refresh,
        expires_at=datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_DAYS),
        user_agent=request.headers.get("user-agent", ""),
        ip_address=request.client.host if request.client else None,
    )
    db.add(refresh_record)

    await db.commit()

    # 5. Return tokens
    access_token = create_access_token(str(user.id), str(org.id), user.role)

    return AuthResponse(
        access_token=access_token,
        refresh_token=raw_refresh,
        expires_in=ACCESS_TOKEN_MINUTES * 60,
        user=_build_user_out(user, org),
    )


@router.post("/login", response_model=AuthResponse)
async def login(body: LoginRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """
    Authenticates a user and issues tokens.

    Deliberately uses the same error message for "user not found" and
    "wrong password" — this prevents user enumeration attacks where an
    attacker can tell whether an email is registered based on the error.
    """
    # Load user
    result = await db.execute(select(User).where(User.email == body.email.lower()))
    user = result.scalar_one_or_none()

    # Verify password — run bcrypt even if user not found to prevent timing attacks
    # (an attacker can measure response time to determine if email exists)
    dummy_hash = "$2b$12$invalidhashfortimingprotection000000000000000000000000"
    password_correct = verify_password(
        body.password,
        user.password_hash if user else dummy_hash
    )

    if not user or not password_correct or not user.is_active:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect email or password"
        )

    # Load org
    org = await db.get(Organization, user.org_id)
    if not org:
        raise HTTPException(status_code=500, detail="Account configuration error")

    # Update last login
    await db.execute(
        update(User)
        .where(User.id == user.id)
        .values(last_login_at=datetime.now(timezone.utc))
    )

    # Issue new refresh token (invalidate old ones optionally — see security note)
    raw_refresh, hashed_refresh = create_refresh_token()
    refresh_record = RefreshToken(
        id=uuid.uuid4(),
        user_id=user.id,
        token_hash=hashed_refresh,
        expires_at=datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_DAYS),
        user_agent=request.headers.get("user-agent", ""),
        ip_address=request.client.host if request.client else None,
    )
    db.add(refresh_record)
    await db.commit()

    access_token = create_access_token(str(user.id), str(org.id), user.role)

    return AuthResponse(
        access_token=access_token,
        refresh_token=raw_refresh,
        expires_in=ACCESS_TOKEN_MINUTES * 60,
        user=_build_user_out(user, org),
    )


@router.post("/refresh", response_model=AuthResponse)
async def refresh_token(body: RefreshRequest, request: Request, db: AsyncSession = Depends(get_db)):
    """
    Issues a new access token given a valid refresh token.
    The frontend calls this automatically when the access token expires.
    Implements refresh token rotation — the old refresh token is revoked
    and a new one is issued. This limits the blast radius of token theft.
    """
    # Hash the incoming token to look it up
    token_hash = hashlib.sha256(body.refresh_token.encode()).hexdigest()

    result = await db.execute(
        select(RefreshToken).where(
            RefreshToken.token_hash == token_hash,
            RefreshToken.revoked == False,
            RefreshToken.expires_at > datetime.now(timezone.utc),
        )
    )
    record = result.scalar_one_or_none()

    if not record:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Refresh token invalid or expired — please log in again"
        )

    # Revoke old refresh token (rotation)
    record.revoked = True
    record.revoked_at = datetime.now(timezone.utc)

    # Load user and org
    user = await db.get(User, record.user_id)
    if not user or not user.is_active:
        await db.commit()
        raise HTTPException(status_code=401, detail="User not found or deactivated")

    org = await db.get(Organization, user.org_id)

    # Issue new refresh token
    raw_refresh, hashed_refresh = create_refresh_token()
    new_record = RefreshToken(
        id=uuid.uuid4(),
        user_id=user.id,
        token_hash=hashed_refresh,
        expires_at=datetime.now(timezone.utc) + timedelta(days=REFRESH_TOKEN_DAYS),
        user_agent=request.headers.get("user-agent", ""),
        ip_address=request.client.host if request.client else None,
    )
    db.add(new_record)
    await db.commit()

    access_token = create_access_token(str(user.id), str(org.id), user.role)

    return AuthResponse(
        access_token=access_token,
        refresh_token=raw_refresh,
        expires_in=ACCESS_TOKEN_MINUTES * 60,
        user=_build_user_out(user, org),
    )


@router.get("/me", response_model=UserOut)
async def me(current=Depends(get_current_user)):
    """
    Returns the current logged-in user.
    The frontend calls this on every page load to rehydrate auth state.
    This is the source of truth — not localStorage.
    """
    return _build_user_out(current["user"], current["org"])


@router.post("/logout", status_code=204)
async def logout(body: RefreshRequest, db: AsyncSession = Depends(get_db)):
    """
    Revokes the refresh token. The access token will expire naturally.
    Frontend should delete both tokens from localStorage on receipt.
    """
    token_hash = hashlib.sha256(body.refresh_token.encode()).hexdigest()

    result = await db.execute(
        select(RefreshToken).where(RefreshToken.token_hash == token_hash)
    )
    record = result.scalar_one_or_none()

    if record:
        record.revoked = True
        record.revoked_at = datetime.now(timezone.utc)
        await db.commit()

    # Return 204 regardless — idempotent, no need to error on missing token


@router.post("/invite/accept")
async def accept_invite(
    body: dict,
    db: AsyncSession = Depends(get_db),
):
    """
    Accepts a team invite. Sets the user's password and marks invite accepted.
    Called when an invited team member clicks the link in their email and
    creates their password.
    """
    token = body.get("invite_token")
    password = body.get("password")
    name = body.get("name", "")

    if not token or not password:
        raise HTTPException(400, "invite_token and password are required")

    if len(password) < 8:
        raise HTTPException(400, "Password must be at least 8 characters")

    # Find user by invite token
    result = await db.execute(
        select(User).where(
            User.invite_token == token,
            User.invite_accepted == False,
        )
    )
    user = result.scalar_one_or_none()

    if not user:
        raise HTTPException(404, "Invite not found or already accepted")

    # Activate the user
    user.password_hash = hash_password(password)
    user.invite_accepted = True
    user.invite_token = None
    user.is_active = True
    if name:
        user.name = name.strip()

    await db.commit()

    return {"message": "Account activated successfully — you can now log in"}
