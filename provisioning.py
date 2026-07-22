"""
provisioning.py — Automated Twilio phone number provisioning.

Replaces the manual "buy a number in Twilio console, set the webhook by hand"
workflow. When a client finishes setup, this buys them a real number and
wires it to their org automatically.

Wire into main.py:
    from provisioning import router as provisioning_router
    app.include_router(provisioning_router, tags=["provisioning"])

REQUIRED ENV VARS (already in use by calls.py):
    TWILIO_ACCOUNT_SID
    TWILIO_AUTH_TOKEN
    BASE_URL
"""

import os
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from twilio.rest import Client as TwilioClient
from twilio.base.exceptions import TwilioRestException

from database import get_db, Organization
from auth import get_current_user
from sqlalchemy.ext.asyncio import AsyncSession

router = APIRouter()

twilio_client = TwilioClient(
    os.environ["TWILIO_ACCOUNT_SID"],
    os.environ["TWILIO_AUTH_TOKEN"],
)


class ProvisionRequest(BaseModel):
    area_code: str | None = None   # optional — e.g. "615". If omitted, any number.
    country: str = "US"


class ProvisionResult(BaseModel):
    phone_number: str
    phone_sid: str
    webhook_url: str


def _build_webhook_url(org_id: str) -> str:
    base_url = os.environ.get("BASE_URL", "").rstrip("/")
    if not base_url:
        raise HTTPException(500, "BASE_URL is not configured on the server")
    return f"{base_url}/calls/inbound/{org_id}"


@router.get("/provisioning/available-numbers")
async def search_available_numbers(
    area_code: str | None = None,
    country: str = "US",
    user=Depends(get_current_user),
):
    """
    Lets the owner preview a few available numbers before committing —
    optional UI step. Returns up to 5 candidates, never purchases anything.
    """
    try:
        kwargs = {"limit": 5}
        if area_code:
            kwargs["area_code"] = area_code
        numbers = twilio_client.available_phone_numbers(country).local.list(**kwargs)
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio search failed: {e.msg}")

    return [
        {"phone_number": n.phone_number, "locality": n.locality, "region": n.region}
        for n in numbers
    ]


@router.post("/provisioning/buy-number", response_model=ProvisionResult)
async def buy_number(
    body: ProvisionRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """
    Buys a real Twilio number, points its Voice webhook at this org's
    inbound-call endpoint, and saves it to the organization row.

    This is the automated replacement for the manual console steps:
    'Phone Numbers → Manage → Buy a Number' + 'Voice Configuration → Webhook'.
    """
    org = await db.get(Organization, user["org"].id)
    if not org:
        raise HTTPException(404, "organization not found")

    if org.twilio_phone_number:
        raise HTTPException(
            409,
            f"This organization already has a number: {org.twilio_phone_number}. "
            f"Use /provisioning/add-number to provision an additional one."
        )

    webhook_url = _build_webhook_url(str(org.id))

    # 1. Find an available number matching the request
    try:
        search_kwargs = {"limit": 1, "voice_enabled": True}
        if body.area_code:
            search_kwargs["area_code"] = body.area_code
        candidates = twilio_client.available_phone_numbers(body.country).local.list(**search_kwargs)
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio number search failed: {e.msg}")

    if not candidates:
        raise HTTPException(404, "No available numbers found for that area code — try without one")

    chosen = candidates[0].phone_number

    # 2. Purchase it, with the webhook set at creation time (single API call,
    #    no separate configuration step needed)
    try:
        purchased = twilio_client.incoming_phone_numbers.create(
            phone_number=chosen,
            voice_url=webhook_url,
            voice_method="POST",
        )
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio purchase failed: {e.msg}")

    # 3. Save to the org row — this is what calls.py and billing.py both read
    org.twilio_phone_number = purchased.phone_number
    org.twilio_phone_sid = purchased.sid
    org.updated_at = datetime.now(timezone.utc)
    await db.commit()

    return ProvisionResult(
        phone_number=purchased.phone_number,
        phone_sid=purchased.sid,
        webhook_url=webhook_url,
    )


@router.post("/provisioning/add-number", response_model=ProvisionResult)
async def add_additional_number(
    body: ProvisionRequest,
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """
    For orgs that already have a primary number and are buying an
    additional one (multi-department routing, extra line, etc.) via the
    plan builder's phone-number stepper. Same webhook target — calls.py
    already routes by org_id in the URL path, so multiple numbers can
    point at the same org safely.
    """
    org = await db.get(Organization, user["org"].id)
    if not org:
        raise HTTPException(404, "organization not found")

    webhook_url = _build_webhook_url(str(org.id))

    try:
        search_kwargs = {"limit": 1, "voice_enabled": True}
        if body.area_code:
            search_kwargs["area_code"] = body.area_code
        candidates = twilio_client.available_phone_numbers(body.country).local.list(**search_kwargs)
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio number search failed: {e.msg}")

    if not candidates:
        raise HTTPException(404, "No available numbers found for that area code — try without one")

    try:
        purchased = twilio_client.incoming_phone_numbers.create(
            phone_number=candidates[0].phone_number,
            voice_url=webhook_url,
            voice_method="POST",
        )
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio purchase failed: {e.msg}")

    # NOTE: if you need to track multiple numbers per org individually
    # (not just the primary), add an org_phone_numbers table — for now
    # the primary field is what's on Organization, additional numbers
    # exist in Twilio and route correctly, but aren't yet listed anywhere
    # in the dashboard. Flagging this as a follow-up, not silently skipping it.
    return ProvisionResult(
        phone_number=purchased.phone_number,
        phone_sid=purchased.sid,
        webhook_url=webhook_url,
    )


@router.delete("/provisioning/release-number")
async def release_number(
    db: AsyncSession = Depends(get_db),
    user=Depends(get_current_user),
):
    """
    Releases (deletes) the org's primary Twilio number — e.g. on
    cancellation or plan downgrade. Twilio stops billing for it
    immediately once released.
    """
    org = await db.get(Organization, user["org"].id)
    if not org or not org.twilio_phone_sid:
        raise HTTPException(404, "no number provisioned for this organization")

    try:
        twilio_client.incoming_phone_numbers(org.twilio_phone_sid).delete()
    except TwilioRestException as e:
        raise HTTPException(502, f"Twilio release failed: {e.msg}")

    org.twilio_phone_number = None
    org.twilio_phone_sid = None
    org.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return {"released": True}
