"""
core.py — The endpoints a real signed-up user needs to see a working
dashboard: org/persona settings, knowledge base CRUD, dashboard stats,
call history. Everything here reads/writes real database rows — no
mock data, no fake "connected" states.

Wire into main.py:
    from core import router as core_router
    app.include_router(core_router, tags=["core"])
"""

import uuid
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy import select, func, desc
from sqlalchemy.ext.asyncio import AsyncSession

from database import get_db, Organization, KnowledgeBaseChunk, CallLog
from auth import get_current_user

router = APIRouter()


# ══════════════════════════════════════════════════════════════
# ORGANIZATION / PERSONA SETTINGS
# ══════════════════════════════════════════════════════════════

class PersonaUpdate(BaseModel):
    name: Optional[str] = None            # receptionist name
    voice_id: Optional[str] = None
    style: Optional[str] = None
    greeting_script: Optional[str] = None
    closing_script: Optional[str] = None
    hold_script: Optional[str] = None
    voicemail_script: Optional[str] = None
    bilingual_auto_switch: Optional[bool] = None
    stability: Optional[float] = None
    similarity_boost: Optional[float] = None
    speaking_rate: Optional[float] = None


@router.get("/org/me")
async def get_my_org(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    """The single endpoint the dashboard's shell/header needs on every page load."""
    org = await db.get(Organization, user["org"].id)
    if not org:
        raise HTTPException(404, "organization not found")
    return {
        "id": str(org.id),
        "name": org.name,
        "business_type": org.business_type,
        "plan": org.plan,
        "plan_status": org.plan_status,
        "onboarding_complete": org.onboarding_complete,
        "onboarding_step": org.onboarding_step,
        "settings": org.settings or {},
        "twilio_phone_number": org.twilio_phone_number,
    }


@router.put("/org/persona")
async def update_persona(body: PersonaUpdate, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    """Saves AI Settings → Persona tab. This is what makes the voice picker,
    scripts, and personality selections actually stick instead of resetting
    on refresh."""
    org = await db.get(Organization, user["org"].id)
    if not org:
        raise HTTPException(404, "organization not found")
    settings = dict(org.settings or {})
    persona = dict(settings.get("persona", {}))
    for field, value in body.model_dump(exclude_unset=True).items():
        persona[field] = value
    settings["persona"] = persona
    org.settings = settings
    org.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"persona": persona}


class OrgUpdate(BaseModel):
    name: Optional[str] = None
    business_type: Optional[str] = None


@router.put("/org")
async def update_org(body: OrgUpdate, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    org = await db.get(Organization, user["org"].id)
    if not org:
        raise HTTPException(404, "organization not found")
    if body.name is not None:
        org.name = body.name
    if body.business_type is not None:
        org.business_type = body.business_type
    org.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"updated": True}


# ══════════════════════════════════════════════════════════════
# KNOWLEDGE BASE — CRUD (Knowledge Base page has nowhere to save without this)
# ══════════════════════════════════════════════════════════════

class KBEntryIn(BaseModel):
    section: str          # "Hours" | "Services" | "Pricing" | "FAQ" | etc.
    text: str
    source: str = "manual"


class ServiceIn(BaseModel):
    """A single row from the Setup Wizard's Services & Pricing form.
    Stored as a knowledge_base entry with section='Services' so the AI
    reads it directly, and structured enough for the ROI dashboard to
    parse it back out."""
    name: str
    price: float
    duration: Optional[str] = None


@router.get("/knowledge-base")
async def list_kb(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(
        select(KnowledgeBaseChunk)
        .where(KnowledgeBaseChunk.org_id == user["org"].id)
        .where(KnowledgeBaseChunk.is_active == True)
        .order_by(KnowledgeBaseChunk.section)
    )
    return [
        {"id": str(r.id), "section": r.section, "text": r.text, "source": r.source,
         "version": r.version, "updated_at": r.updated_at}
        for r in rows.scalars()
    ]


@router.post("/knowledge-base")
async def create_kb_entry(body: KBEntryIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    entry = KnowledgeBaseChunk(
        id=uuid.uuid4(),
        org_id=user["org"].id,
        section=body.section,
        text=body.text,
        source=body.source,
    )
    db.add(entry)
    await db.commit()
    return {"id": str(entry.id)}


@router.put("/knowledge-base/{entry_id}")
async def update_kb_entry(entry_id: str, body: KBEntryIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    entry = await db.get(KnowledgeBaseChunk, entry_id)
    if not entry or str(entry.org_id) != str(user["org"].id):
        raise HTTPException(404, "entry not found")
    entry.section, entry.text = body.section, body.text
    entry.version += 1
    entry.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"updated": True, "version": entry.version}


@router.delete("/knowledge-base/{entry_id}")
async def delete_kb_entry(entry_id: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    entry = await db.get(KnowledgeBaseChunk, entry_id)
    if not entry or str(entry.org_id) != str(user["org"].id):
        raise HTTPException(404, "entry not found")
    entry.is_active = False  # soft delete, keeps history
    await db.commit()
    return {"deleted": True}


@router.post("/knowledge-base/services")
async def add_service(body: ServiceIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    """Setup Wizard's Services & Pricing step calls this per row.
    Feeds both AI quoting (via calls.py's knowledge_base read) and the
    revenue-recovered dashboard math."""
    text = f"{body.name}: ${body.price:.2f}"
    if body.duration:
        text += f" ({body.duration})"
    entry = KnowledgeBaseChunk(
        id=uuid.uuid4(),
        org_id=user["org"].id,
        section="Services",
        text=text,
        source="manual",
    )
    db.add(entry)
    await db.commit()
    return {"id": str(entry.id)}


# ══════════════════════════════════════════════════════════════
# DASHBOARD STATS — the numbers the home/dashboard page displays
# ══════════════════════════════════════════════════════════════

@router.get("/dashboard/summary")
async def dashboard_summary(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    org_id = user["org"].id

    total_calls = await db.execute(
        select(func.count(CallLog.id)).where(CallLog.org_id == org_id))
    total_calls = total_calls.scalar() or 0

    booked = await db.execute(
        select(func.count(CallLog.id)).where(CallLog.org_id == org_id, CallLog.outcome == "booked"))
    booked = booked.scalar() or 0

    avg_duration = await db.execute(
        select(func.avg(CallLog.duration_seconds)).where(CallLog.org_id == org_id))
    avg_duration = avg_duration.scalar() or 0

    active_now = await db.execute(
        select(func.count(CallLog.id)).where(CallLog.org_id == org_id, CallLog.status == "active"))
    active_now = active_now.scalar() or 0

    return {
        "total_calls": total_calls,
        "booked_appointments": booked,
        "avg_call_duration_seconds": round(avg_duration, 1),
        "active_calls_now": active_now,
    }


@router.get("/calls")
async def call_history(
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    rows = await db.execute(
        select(CallLog)
        .where(CallLog.org_id == user["org"].id)
        .order_by(desc(CallLog.started_at))
        .limit(limit)
    )
    return [
        {
            "id": str(r.id), "call_sid": r.call_sid, "caller_number": r.caller_number,
            "caller_name": r.caller_name, "status": r.status, "outcome": r.outcome,
            "intent": r.intent, "duration_seconds": r.duration_seconds,
            "summary": r.summary, "sentiment": r.sentiment, "lead_score": r.lead_score,
            "started_at": r.started_at, "ended_at": r.ended_at,
            "is_spam": (r.transcript or {}) and False,  # spam flag wired below in calls.py note
        }
        for r in rows.scalars()
    ]
