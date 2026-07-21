"""
billing.py — Plan builder, pricing engine, minutes balance, setup drafts,
custom roles, owner-defined intents, and operating modes.

Wire into main.py:
    from billing import router as billing_router
    app.include_router(billing_router, tags=["billing"])

Tables are created automatically by main.py's lifespan (Base.metadata.create_all)
since these models use the same Base from database.py.

NOTE: Payment execution (Stripe) is NOT wired here yet — endpoints compute and
record everything (quotes, balances, proration) so the frontend works end to end;
swap the marked stub when Stripe keys are ready.
"""

import os
import uuid
from datetime import datetime, timezone, date
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import (
    Column, String, Integer, Float, Boolean, DateTime, Date, Text, JSON,
    ForeignKey, select
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.asyncio import AsyncSession

from database import Base, get_db
from auth import get_current_user  # assumes auth.py exposes this dependency

router = APIRouter()

# ══════════════════════════════════════════════════════════════
# RATE CARD — single source of truth for all pricing
# ══════════════════════════════════════════════════════════════

RATES = {
    "seat": 20.0,               # per seat / month
    "phone_number": 10.0,       # per number / month
    "minute": 0.30,             # per minute, prepaid balance (no blocks)
    "after_hours": 40.0,
    "calendar_sync": 15.0,
    "crm": 25.0,
    "sms": 20.0,                # base; per-message usage billed separately
    "sms_per_message": 0.05,
    "outbound": 50.0,
    "premium_voice": 25.0,
    "retention_1yr": 15.0,
    "retention_forever": 30.0,
    "api_included": 5,          # integrations included free
    "api_each": 5.0,            # per integration past included
}

SETUP_FEE = {
    "base": 150.0,
    "per_seat": 25.0,
    "per_number": 15.0,
    "per_integration_connected": 50.0,  # calendar, CRM, SMS each count as one
}

FREQ_FACTOR = {
    "monthly": 1.0,
    "daily": 12.0 / 365.0,
    "weekly": 12.0 / 52.0,
    "yearly": 12.0 * 0.90,      # 10% discount for prepaying the year
}


# ══════════════════════════════════════════════════════════════
# MODELS
# ══════════════════════════════════════════════════════════════

class PlanConfig(Base):
    __tablename__ = "plan_configs"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), unique=True, nullable=False)
    seats = Column(Integer, nullable=False, default=1)
    phone_numbers = Column(Integer, nullable=False, default=1)
    after_hours = Column(Boolean, nullable=False, default=False)
    calendar_sync = Column(Boolean, nullable=False, default=False)
    crm = Column(Boolean, nullable=False, default=False)
    sms = Column(Boolean, nullable=False, default=False)
    outbound = Column(Boolean, nullable=False, default=False)
    premium_voice = Column(Boolean, nullable=False, default=False)
    retention = Column(String, nullable=False, default="30d")  # 30d | 1yr | forever
    api_count = Column(Integer, nullable=False, default=5)
    frequency = Column(String, nullable=False, default="monthly")  # daily|weekly|monthly|yearly
    activated_at = Column(DateTime(timezone=True), nullable=True)
    refund_window_ends = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class MinutesLedger(Base):
    __tablename__ = "minutes_ledger"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    delta_minutes = Column(Float, nullable=False)   # + topup, - usage
    amount_paid = Column(Float, nullable=True)      # dollars, for topups
    reason = Column(String, nullable=False)         # topup | call_usage | refund_adjustment
    call_sid = Column(String, nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class SetupDraft(Base):
    """Auto-saved drafts for both the plan builder and the setup wizard.
    One row per org per kind; content is the full form state as JSON."""
    __tablename__ = "setup_drafts"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    kind = Column(String, nullable=False)  # plan_builder | setup_wizard
    content = Column(JSON, nullable=False, default=dict)
    step = Column(String, nullable=True)   # wizard resume point
    updated_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc), onupdate=lambda: datetime.now(timezone.utc))


class CustomRole(Base):
    """Owner-created roles. permissions JSON shape:
    { "pages": {"billing": true, ...},
      "features": {"call_history.delete_recordings": false, ...} }"""
    __tablename__ = "custom_roles"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    permissions = Column(JSON, nullable=False, default=dict)
    is_template = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, default=lambda: datetime.now(timezone.utc))


class CallIntent(Base):
    """Owner-defined intents with routing action."""
    __tablename__ = "call_intents"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    label = Column(String, nullable=False)
    routing = Column(String, nullable=False, default="ai_handles")
    # ai_handles | transfer | voicemail | take_message | callback | escalate
    sort_order = Column(Integer, nullable=False, default=0)


class OperatingMode(Base):
    """Named bundles of autonomy settings. Normal / After-Hours / Vacation / custom."""
    __tablename__ = "operating_modes"
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    org_id = Column(UUID(as_uuid=True), ForeignKey("organizations.id", ondelete="CASCADE"), nullable=False, index=True)
    name = Column(String, nullable=False)
    settings = Column(JSON, nullable=False, default=dict)
    # e.g. {"booking":"confirm_first","lead_capture":"full_auto",
    #       "emergency_escalation_number":"+1...","custom_message":"..."}
    is_active = Column(Boolean, nullable=False, default=False)
    scheduled_start = Column(Date, nullable=True)
    scheduled_end = Column(Date, nullable=True)   # auto-revert date
    is_builtin = Column(Boolean, nullable=False, default=False)


# ══════════════════════════════════════════════════════════════
# Pydantic shapes
# ══════════════════════════════════════════════════════════════

class PlanSelection(BaseModel):
    seats: int = Field(ge=1, default=1)
    phone_numbers: int = Field(ge=1, default=1)
    after_hours: bool = False
    calendar_sync: bool = False
    crm: bool = False
    sms: bool = False
    outbound: bool = False
    premium_voice: bool = False
    retention: str = "30d"
    api_count: int = Field(ge=0, default=5)
    frequency: str = "monthly"
    initial_minutes_dollars: float = Field(ge=0, default=0)  # any amount they type


class DraftIn(BaseModel):
    kind: str
    content: dict
    step: Optional[str] = None


class RoleIn(BaseModel):
    name: str
    permissions: dict


class IntentIn(BaseModel):
    label: str
    routing: str = "ai_handles"


class ModeIn(BaseModel):
    name: str
    settings: dict = {}
    scheduled_start: Optional[date] = None
    scheduled_end: Optional[date] = None


# ══════════════════════════════════════════════════════════════
# PRICING ENGINE
# ══════════════════════════════════════════════════════════════

def compute_recurring_monthly(sel: PlanSelection) -> dict:
    lines = {}
    lines["seats"] = sel.seats * RATES["seat"]
    lines["phone_numbers"] = sel.phone_numbers * RATES["phone_number"]
    if sel.after_hours: lines["after_hours"] = RATES["after_hours"]
    if sel.calendar_sync: lines["calendar_sync"] = RATES["calendar_sync"]
    if sel.crm: lines["crm"] = RATES["crm"]
    if sel.sms: lines["sms"] = RATES["sms"]
    if sel.outbound: lines["outbound"] = RATES["outbound"]
    if sel.premium_voice: lines["premium_voice"] = RATES["premium_voice"]
    if sel.retention == "1yr": lines["retention"] = RATES["retention_1yr"]
    elif sel.retention == "forever": lines["retention"] = RATES["retention_forever"]
    extra_apis = max(0, sel.api_count - RATES["api_included"])
    if extra_apis: lines["apis"] = extra_apis * RATES["api_each"]
    return lines


def compute_setup_fee(sel: PlanSelection) -> dict:
    integrations = sum([sel.calendar_sync, sel.crm, sel.sms])
    return {
        "base": SETUP_FEE["base"],
        "seats": sel.seats * SETUP_FEE["per_seat"],
        "numbers": sel.phone_numbers * SETUP_FEE["per_number"],
        "integrations": integrations * SETUP_FEE["per_integration_connected"],
    }


@router.get("/billing/rates")
async def get_rates():
    """Frontend reads this so prices are never hardcoded in the UI."""
    return {"rates": RATES, "setup_fee": SETUP_FEE, "frequencies": list(FREQ_FACTOR)}


@router.post("/billing/quote")
async def quote(sel: PlanSelection):
    """Live price calculation for the plan builder. No auth required —
    prospects use this before signup."""
    if sel.frequency not in FREQ_FACTOR:
        raise HTTPException(400, "frequency must be daily|weekly|monthly|yearly")
    monthly_lines = compute_recurring_monthly(sel)
    monthly_total = round(sum(monthly_lines.values()), 2)
    factor = FREQ_FACTOR[sel.frequency]
    setup_lines = compute_setup_fee(sel)
    setup_total = round(sum(setup_lines.values()), 2)
    minutes = round(sel.initial_minutes_dollars / RATES["minute"], 1) if sel.initial_minutes_dollars else 0
    recurring_at_frequency = round(monthly_total * factor, 2)
    return {
        "recurring_monthly_breakdown": monthly_lines,
        "recurring_monthly_total": monthly_total,
        "frequency": sel.frequency,
        "recurring_at_frequency": recurring_at_frequency,
        "setup_fee_breakdown": setup_lines,
        "setup_fee_total": setup_total,
        "initial_minutes_dollars": sel.initial_minutes_dollars,
        "initial_minutes": minutes,
        "minute_rate": RATES["minute"],
        "first_payment_total": round(setup_total + recurring_at_frequency + sel.initial_minutes_dollars, 2),
        "notes": [
            "Calls never cut off mid-call — balance can dip below zero and settles on next top-up.",
            "Spam and robocalls never count against your minutes.",
            "7-day money-back window: cancel within 7 days of activation for a full refund minus actual usage.",
        ],
    }


@router.post("/billing/activate")
async def activate_plan(sel: PlanSelection, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    """Saves the chosen plan and starts the 7-day refund window.
    STUB: payment capture (Stripe) goes here before the save when keys are ready."""
    existing = await db.execute(select(PlanConfig).where(PlanConfig.org_id == user.org_id))
    plan = existing.scalar_one_or_none()
    now = datetime.now(timezone.utc)
    if plan is None:
        plan = PlanConfig(org_id=user.org_id)
        db.add(plan)
    for f in ["seats","phone_numbers","after_hours","calendar_sync","crm","sms",
              "outbound","premium_voice","retention","api_count","frequency"]:
        setattr(plan, f, getattr(sel, f))
    if plan.activated_at is None:
        plan.activated_at = now
        from datetime import timedelta
        plan.refund_window_ends = now + timedelta(days=7)
    if sel.initial_minutes_dollars > 0:
        db.add(MinutesLedger(
            org_id=user.org_id,
            delta_minutes=sel.initial_minutes_dollars / RATES["minute"],
            amount_paid=sel.initial_minutes_dollars,
            reason="topup",
        ))
    await db.commit()
    return {"status": "activated", "refund_window_ends": plan.refund_window_ends}


@router.get("/billing/minutes")
async def minutes_balance(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(select(MinutesLedger.delta_minutes).where(MinutesLedger.org_id == user.org_id))
    balance = round(sum(r[0] for r in rows.all()), 1)
    return {"balance_minutes": balance, "minute_rate": RATES["minute"]}


@router.post("/billing/minutes/topup")
async def topup(amount_dollars: float, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    if amount_dollars <= 0:
        raise HTTPException(400, "amount must be positive")
    # STUB: charge card via Stripe here
    db.add(MinutesLedger(
        org_id=user.org_id,
        delta_minutes=amount_dollars / RATES["minute"],
        amount_paid=amount_dollars,
        reason="topup",
    ))
    await db.commit()
    return {"added_minutes": round(amount_dollars / RATES["minute"], 1)}


@router.post("/billing/proration-preview")
async def proration_preview(current: PlanSelection, proposed: PlanSelection):
    """Shows the credit/charge effect of a mid-cycle change before confirming."""
    cur = sum(compute_recurring_monthly(current).values())
    new = sum(compute_recurring_monthly(proposed).values())
    diff = round(new - cur, 2)
    return {
        "monthly_delta": diff,
        "explanation": (
            f"Adding ${diff}/mo, charged prorated from today" if diff > 0
            else f"Removing ${-diff}/mo — unused days credited on your next bill" if diff < 0
            else "No price change"
        ),
    }


# ══════════════════════════════════════════════════════════════
# DRAFT AUTOSAVE (plan builder + setup wizard)
# ══════════════════════════════════════════════════════════════

@router.put("/setup/draft")
async def save_draft(body: DraftIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    if body.kind not in ("plan_builder", "setup_wizard"):
        raise HTTPException(400, "kind must be plan_builder or setup_wizard")
    existing = await db.execute(
        select(SetupDraft).where(SetupDraft.org_id == user.org_id, SetupDraft.kind == body.kind))
    draft = existing.scalar_one_or_none()
    if draft is None:
        draft = SetupDraft(org_id=user.org_id, kind=body.kind)
        db.add(draft)
    draft.content = body.content
    draft.step = body.step
    await db.commit()
    return {"saved": True}


@router.get("/setup/draft/{kind}")
async def get_draft(kind: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    existing = await db.execute(
        select(SetupDraft).where(SetupDraft.org_id == user.org_id, SetupDraft.kind == kind))
    draft = existing.scalar_one_or_none()
    if draft is None:
        return {"content": None, "step": None}
    return {"content": draft.content, "step": draft.step, "updated_at": draft.updated_at}


# ══════════════════════════════════════════════════════════════
# CUSTOM ROLES (two-level permissions: pages + features)
# ══════════════════════════════════════════════════════════════

@router.get("/roles")
async def list_roles(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(select(CustomRole).where(CustomRole.org_id == user.org_id))
    return [{"id": str(r.id), "name": r.name, "permissions": r.permissions, "is_template": r.is_template}
            for r in rows.scalars()]


@router.post("/roles")
async def create_role(body: RoleIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    role = CustomRole(org_id=user.org_id, name=body.name, permissions=body.permissions)
    db.add(role)
    await db.commit()
    return {"id": str(role.id)}


@router.put("/roles/{role_id}")
async def update_role(role_id: str, body: RoleIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(CustomRole, role_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "role not found")
    row.name, row.permissions = body.name, body.permissions
    await db.commit()
    return {"updated": True}


@router.delete("/roles/{role_id}")
async def delete_role(role_id: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(CustomRole, role_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "role not found")
    await db.delete(row)
    await db.commit()
    return {"deleted": True}


# ══════════════════════════════════════════════════════════════
# OWNER-DEFINED INTENTS
# ══════════════════════════════════════════════════════════════

VALID_ROUTING = {"ai_handles", "transfer", "voicemail", "take_message", "callback", "escalate"}


@router.get("/intents")
async def list_intents(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(
        select(CallIntent).where(CallIntent.org_id == user.org_id).order_by(CallIntent.sort_order))
    return [{"id": str(r.id), "label": r.label, "routing": r.routing} for r in rows.scalars()]


@router.post("/intents")
async def create_intent(body: IntentIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    if body.routing not in VALID_ROUTING:
        raise HTTPException(400, f"routing must be one of {sorted(VALID_ROUTING)}")
    intent = CallIntent(org_id=user.org_id, label=body.label, routing=body.routing)
    db.add(intent)
    await db.commit()
    return {"id": str(intent.id)}


@router.put("/intents/{intent_id}")
async def update_intent(intent_id: str, body: IntentIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(CallIntent, intent_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "intent not found")
    if body.routing not in VALID_ROUTING:
        raise HTTPException(400, f"routing must be one of {sorted(VALID_ROUTING)}")
    row.label, row.routing = body.label, body.routing
    await db.commit()
    return {"updated": True}


@router.delete("/intents/{intent_id}")
async def delete_intent(intent_id: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(CallIntent, intent_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "intent not found")
    await db.delete(row)
    await db.commit()
    return {"deleted": True}


# ══════════════════════════════════════════════════════════════
# OPERATING MODES
# ══════════════════════════════════════════════════════════════

@router.get("/modes")
async def list_modes(db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(select(OperatingMode).where(OperatingMode.org_id == user.org_id))
    return [{"id": str(m.id), "name": m.name, "settings": m.settings, "is_active": m.is_active,
             "scheduled_start": m.scheduled_start, "scheduled_end": m.scheduled_end,
             "is_builtin": m.is_builtin} for m in rows.scalars()]


@router.post("/modes")
async def create_mode(body: ModeIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    mode = OperatingMode(org_id=user.org_id, name=body.name, settings=body.settings,
                         scheduled_start=body.scheduled_start, scheduled_end=body.scheduled_end)
    db.add(mode)
    await db.commit()
    return {"id": str(mode.id)}


@router.post("/modes/{mode_id}/activate")
async def activate_mode(mode_id: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    rows = await db.execute(select(OperatingMode).where(OperatingMode.org_id == user.org_id))
    target = None
    for m in rows.scalars():
        m.is_active = (str(m.id) == mode_id)
        if m.is_active:
            target = m
    if target is None:
        raise HTTPException(404, "mode not found")
    await db.commit()
    return {"active_mode": target.name}


@router.put("/modes/{mode_id}")
async def update_mode(mode_id: str, body: ModeIn, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(OperatingMode, mode_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "mode not found")
    row.name, row.settings = body.name, body.settings
    row.scheduled_start, row.scheduled_end = body.scheduled_start, body.scheduled_end
    await db.commit()
    return {"updated": True}


@router.delete("/modes/{mode_id}")
async def delete_mode(mode_id: str, db: AsyncSession = Depends(get_db), user=Depends(get_current_user)):
    row = await db.get(OperatingMode, mode_id)
    if row is None or row.org_id != user.org_id:
        raise HTTPException(404, "mode not found")
    if row.is_builtin:
        raise HTTPException(400, "built-in modes cannot be deleted")
    await db.delete(row)
    await db.commit()
    return {"deleted": True}


# ══════════════════════════════════════════════════════════════
# ENV KEY HEALTH — verifies API keys are present (never exposes values)
# ══════════════════════════════════════════════════════════════

@router.get("/health/keys")
async def key_health():
    keys = ["ANTHROPIC_API_KEY", "DEEPGRAM_API_KEY", "ELEVENLABS_API_KEY",
            "TWILIO_ACCOUNT_SID", "TWILIO_AUTH_TOKEN", "TWILIO_PHONE_NUMBER",
            "DATABASE_URL", "BASE_URL"]
    return {k: bool(os.environ.get(k)) for k in keys}
