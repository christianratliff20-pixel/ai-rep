"""
seed.py — Seeds the operator account and initial data into the database.

Run this ONCE after deploying:
    python seed.py

Or call seed_database() from main.py lifespan on first startup.
The operator account is the ONLY way to access the god panel.
It cannot be created through signup — it only exists because of this seed.
"""

import asyncio
import os
import uuid
from datetime import datetime, timezone, timedelta

import bcrypt
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker, AsyncSession

# ── CONFIG ─────────────────────────────────────────────────────
DATABASE_URL = os.environ.get("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql+asyncpg://", 1)
elif DATABASE_URL.startswith("postgresql://"):
    DATABASE_URL = DATABASE_URL.replace("postgresql://", "postgresql+asyncpg://", 1)

# ── OPERATOR CREDENTIALS ────────────────────────────────────────
# These are hardcoded. The only way to change them is to update
# this file and re-run the seed. Not exposed through any API.
OPERATOR_EMAIL    = "christianratliff20@gmail.com"
OPERATOR_PASSWORD = "Tweetybird#20"
OPERATOR_NAME     = "Chris Ratliff"

# Pre-computed bcrypt hash of Tweetybird#20 (cost factor 12)
# Generated offline — never computed at runtime so the plaintext
# password never needs to exist in memory on the server
OPERATOR_HASH = "$2b$12$4zHaAbtnehTAPB.odc3GL.3RidaJFSoBgpMuTVTFguVd3A50iBVDK"

# ── STATE COMPLIANCE DATA ───────────────────────────────────────
# All 50 states + DC — one-party vs two-party consent for call recording
STATE_COMPLIANCE = [
    ("AL","Alabama","one_party","This call may be recorded for quality assurance purposes.",""),
    ("AK","Alaska","one_party","This call may be recorded for quality assurance purposes.",""),
    ("AZ","Arizona","one_party","This call may be recorded for quality assurance purposes.",""),
    ("AR","Arkansas","one_party","This call may be recorded for quality assurance purposes.",""),
    ("CA","California","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("CO","Colorado","one_party","This call may be recorded for quality assurance purposes.",""),
    ("CT","Connecticut","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("DE","Delaware","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("DC","District of Columbia","one_party","This call may be recorded for quality assurance purposes.",""),
    ("FL","Florida","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("GA","Georgia","one_party","This call may be recorded for quality assurance purposes.",""),
    ("HI","Hawaii","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("ID","Idaho","one_party","This call may be recorded for quality assurance purposes.",""),
    ("IL","Illinois","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("IN","Indiana","one_party","This call may be recorded for quality assurance purposes.",""),
    ("IA","Iowa","one_party","This call may be recorded for quality assurance purposes.",""),
    ("KS","Kansas","one_party","This call may be recorded for quality assurance purposes.",""),
    ("KY","Kentucky","one_party","This call may be recorded for quality assurance purposes.",""),
    ("LA","Louisiana","one_party","This call may be recorded for quality assurance purposes.",""),
    ("ME","Maine","one_party","This call may be recorded for quality assurance purposes.",""),
    ("MD","Maryland","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("MA","Massachusetts","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("MI","Michigan","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("MN","Minnesota","one_party","This call may be recorded for quality assurance purposes.",""),
    ("MS","Mississippi","one_party","This call may be recorded for quality assurance purposes.",""),
    ("MO","Missouri","one_party","This call may be recorded for quality assurance purposes.",""),
    ("MT","Montana","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("NE","Nebraska","one_party","This call may be recorded for quality assurance purposes.",""),
    ("NV","Nevada","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("NH","New Hampshire","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("NJ","New Jersey","one_party","This call may be recorded for quality assurance purposes.",""),
    ("NM","New Mexico","one_party","This call may be recorded for quality assurance purposes.",""),
    ("NY","New York","one_party","This call may be recorded for quality assurance purposes.",""),
    ("NC","North Carolina","one_party","This call may be recorded for quality assurance purposes.",""),
    ("ND","North Dakota","one_party","This call may be recorded for quality assurance purposes.",""),
    ("OH","Ohio","one_party","This call may be recorded for quality assurance purposes.",""),
    ("OK","Oklahoma","one_party","This call may be recorded for quality assurance purposes.",""),
    ("OR","Oregon","two_party","By continuing this call, you consent to being recorded.","Two-party state"),
    ("PA","Pennsylvania","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("RI","Rhode Island","one_party","This call may be recorded for quality assurance purposes.",""),
    ("SC","South Carolina","one_party","This call may be recorded for quality assurance purposes.",""),
    ("SD","South Dakota","one_party","This call may be recorded for quality assurance purposes.",""),
    ("TN","Tennessee","one_party","This call may be recorded for quality assurance purposes.",""),
    ("TX","Texas","one_party","This call may be recorded for quality assurance purposes.",""),
    ("UT","Utah","one_party","This call may be recorded for quality assurance purposes.",""),
    ("VT","Vermont","one_party","This call may be recorded for quality assurance purposes.",""),
    ("VA","Virginia","one_party","This call may be recorded for quality assurance purposes.",""),
    ("WA","Washington","two_party","By continuing this call, you consent to being recorded. If you do not consent, please hang up now.","Strict two-party state"),
    ("WV","West Virginia","one_party","This call may be recorded for quality assurance purposes.",""),
    ("WI","Wisconsin","one_party","This call may be recorded for quality assurance purposes.",""),
    ("WY","Wyoming","one_party","This call may be recorded for quality assurance purposes.",""),
]


async def seed_database():
    if not DATABASE_URL:
        print("[SEED] No DATABASE_URL set — skipping seed")
        return

    engine = create_async_engine(DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with Session() as db:
        # ── Check if operator already exists ────────────────────
        result = await db.execute(
            text("SELECT id FROM users WHERE email = :email"),
            {"email": OPERATOR_EMAIL}
        )
        existing = result.fetchone()

        if existing:
            print(f"[SEED] Operator account already exists — skipping")
        else:
            # ── Create operator org ─────────────────────────────
            org_id = uuid.uuid4()
            now = datetime.now(timezone.utc)
            await db.execute(text("""
                INSERT INTO organizations (id, name, business_type, plan, plan_status, onboarding_complete, onboarding_step, settings, created_at, updated_at)
                VALUES (:id, :name, :type, :plan, :status, :onboarding, :step, CAST(:settings AS jsonb), :created_at, :updated_at)
            """), {
                "id": str(org_id),
                "name": "Platform Operator",
                "type": "operator",
                "plan": "operator",
                "status": "active",
                "onboarding": True,
                "step": 1,
                "settings": "{}",
                "created_at": now,
                "updated_at": now,
            })

            # ── Create operator user ────────────────────────────
            user_id = uuid.uuid4()
            await db.execute(text("""
                INSERT INTO users (id, org_id, email, password_hash, name, role, is_active, invite_accepted, created_at, updated_at)
                VALUES (:id, :org_id, :email, :hash, :name, :role, true, true, :created_at, :updated_at)
            """), {
                "id": str(user_id),
                "org_id": str(org_id),
                "email": OPERATOR_EMAIL,
                "hash": OPERATOR_HASH,
                "name": OPERATOR_NAME,
                "role": "operator",
                "created_at": now,
                "updated_at": now,
            })

            print(f"[SEED] ✓ Operator account created: {OPERATOR_EMAIL}")

        # ── Seed state compliance if table is empty ──────────────
        try:
            result = await db.execute(text("SELECT COUNT(*) FROM state_compliance"))
            count = result.scalar()
        except Exception as e:
            print(f"[SEED] state_compliance table not found — skipping ({e})")
            await db.rollback()
            count = None

        if count == 0:
            for state in STATE_COMPLIANCE:
                await db.execute(text("""
                    INSERT INTO state_compliance (state_code, state_name, consent_type, disclosure_script, notes)
                    VALUES (:code, :name, :consent, :script, :notes)
                    ON CONFLICT (state_code) DO NOTHING
                """), {
                    "code": state[0], "name": state[1], "consent": state[2],
                    "script": state[3], "notes": state[4],
                })
            print(f"[SEED] ✓ State compliance data seeded (51 entries)")
        else:
            print(f"[SEED] State compliance already seeded — skipping")

        await db.commit()

    await engine.dispose()
    print("[SEED] Complete")


if __name__ == "__main__":
    asyncio.run(seed_database())
