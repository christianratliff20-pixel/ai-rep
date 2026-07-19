"""
calls.py — Real-time AI receptionist call handler

Pipeline:
  Twilio (inbound call)
  → TwiML routes to WebSocket
  → Deepgram STT (streaming, real-time)
  → Claude API (with KB context, conversation memory)
  → ElevenLabs TTS (streaming audio)
  → Audio bytes back to Twilio → caller hears AI voice

HOW TO WIRE THIS INTO YOUR FASTAPI MAIN.PY:
  from calls import router as calls_router
  app.include_router(calls_router)

ENVIRONMENT VARIABLES NEEDED:
  TWILIO_ACCOUNT_SID
  TWILIO_AUTH_TOKEN
  TWILIO_PHONE_NUMBER
  DEEPGRAM_API_KEY
  ANTHROPIC_API_KEY
  ELEVENLABS_API_KEY
  DATABASE_URL
"""

import asyncio
import base64
import json
import os
import time
import audioop
from datetime import datetime, timezone
from typing import Optional

import httpx
import websockets
from anthropic import AsyncAnthropic
from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Request, HTTPException, Depends
from fastapi.responses import Response
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from twilio.rest import Client as TwilioClient
from twilio.twiml.voice_response import VoiceResponse, Connect, Stream

from database import get_db, CallLog, Organization, KnowledgeBaseChunk

router = APIRouter()

# ── CLIENTS ────────────────────────────────────────────────
twilio_client = TwilioClient(
    os.environ["TWILIO_ACCOUNT_SID"],
    os.environ["TWILIO_AUTH_TOKEN"]
)
anthropic_client = AsyncAnthropic(api_key=os.environ["ANTHROPIC_API_KEY"])

# ── CONSTANTS ──────────────────────────────────────────────
DEEPGRAM_URL = "wss://api.deepgram.com/v1/listen"
ELEVENLABS_URL = "https://api.elevenlabs.io/v1/text-to-speech/{voice_id}/stream"
SAMPLE_RATE = 8000          # Twilio uses 8kHz mulaw
AUDIO_CHUNK_SIZE = 160      # 20ms at 8kHz = 160 bytes mulaw
SILENCE_THRESHOLD = 300     # RMS amplitude below this = silence
SILENCE_TIMEOUT = 1.2       # seconds of silence before treating utterance as complete
MAX_CALL_DURATION = 600     # 10 minutes hard limit


# ══════════════════════════════════════════════════════════════
# 1. TWILIO INBOUND WEBHOOK
#    Twilio hits this when a call comes in to your number.
#    We return TwiML that tells Twilio to connect to our WebSocket.
# ══════════════════════════════════════════════════════════════

@router.post("/calls/inbound/{client_id}")
async def inbound_call(client_id: str, request: Request):
    """
    Twilio webhook. Returns TwiML that opens a Media Stream
    to our WebSocket endpoint so we can process audio in real time.
    """
    form = await request.form()
    call_sid = form.get("CallSid", "")
    caller_number = form.get("From", "")

    # Build TwiML response
    response = VoiceResponse()
    connect = Connect()

    # This URL must be wss:// — Twilio only streams to WebSocket
    ws_url = f"wss://{request.headers.get('host')}/ws/call/{client_id}/{call_sid}"
    stream = Stream(url=ws_url)
    stream.parameter(name="client_id", value=client_id)
    stream.parameter(name="caller_number", value=caller_number)
    connect.append(stream)
    response.append(connect)

    return Response(content=str(response), media_type="application/xml")


# ══════════════════════════════════════════════════════════════
# 2. OUTBOUND TEST CALL
#    Initiates a call FROM your Twilio number TO the user's phone.
#    When they pick up, routes through the same AI pipeline.
# ══════════════════════════════════════════════════════════════

@router.post("/clients/{client_id}/test-call")
async def initiate_test_call(client_id: str, body: dict, db: AsyncSession = Depends(get_db)):
    """
    Places an outbound call to the provided phone number.
    When answered, it goes through the real AI pipeline so the
    business owner can hear exactly what callers will hear.
    """
    phone = body.get("phone")
    if not phone:
        raise HTTPException(400, "phone is required")

    # Build the inbound URL — this is the same TwiML endpoint
    # When the called person answers, Twilio treats it like an inbound call
    base_url = os.environ.get("BASE_URL", "https://yourapp.onrender.com")
    twiml_url = f"{base_url}/calls/inbound/{client_id}?test=true"

    call = twilio_client.calls.create(
        to=phone,
        from_=os.environ["TWILIO_PHONE_NUMBER"],
        url=twiml_url,
        timeout=30,
    )

    return {"call_sid": call.sid, "status": call.status, "message": "Test call initiated"}


# ══════════════════════════════════════════════════════════════
# 3. WEBSOCKET CALL HANDLER
#    This is the core of the entire system.
#    Runs for the full duration of every call.
# ══════════════════════════════════════════════════════════════

@router.websocket("/ws/call/{client_id}/{call_sid}")
async def call_websocket(websocket: WebSocket, client_id: str, call_sid: str, db: AsyncSession = Depends(get_db)):
    """
    Called for every active call. Orchestrates the full pipeline:
    Twilio audio → Deepgram STT → Claude → ElevenLabs → Twilio audio
    """
    await websocket.accept()

    # Load org settings and knowledge base from DB
    org = await db.get(Organization, client_id)
    if not org:
        await websocket.close(1008, "Organization not found")
        return

    kb_chunks = await db.execute(
        select(KnowledgeBaseChunk)
        .where(KnowledgeBaseChunk.org_id == client_id)
        .where(KnowledgeBaseChunk.is_active == True)
        .limit(40)  # Stay within Claude's context budget
    )
    knowledge_base = "\n\n".join(chunk.text for chunk in kb_chunks.scalars())

    # Build the system prompt — this is what makes your AI behave like this specific business
    settings = org.settings or {}
    persona = settings.get("persona", {})
    system_prompt = build_system_prompt(org, persona, knowledge_base)

    # Initialize call state
    state = CallState(
        client_id=client_id,
        call_sid=call_sid,
        client=org,
        persona=persona,
        system_prompt=system_prompt,
    )

    # Log call start
    call_log = CallLog(
        org_id=client_id,
        call_sid=call_sid,
        caller_number=state.caller_number,
        started_at=datetime.now(timezone.utc),
        status="active",
    )
    db.add(call_log)
    await db.commit()

    try:
        await run_call(websocket, state, db, call_log)
    except WebSocketDisconnect:
        pass
    except Exception as e:
        print(f"[CALL ERROR] {call_sid}: {e}")
    finally:
        # Finalize call log
        call_log.ended_at = datetime.now(timezone.utc)
        call_log.duration_seconds = int((call_log.ended_at - call_log.started_at).total_seconds())
        call_log.transcript = state.transcript
        call_log.status = "completed"
        call_log.outcome = state.outcome
        call_log.lead_score = await score_lead(state.transcript)
        call_log.sentiment = state.final_sentiment
        await db.commit()

        # Notify dashboard via separate WebSocket broadcast
        await broadcast_call_ended(client_id, call_sid, call_log)


async def run_call(websocket: WebSocket, state: "CallState", db: AsyncSession, call_log: CallLog):
    """
    Main call loop. Manages three concurrent tasks:
    1. receive_twilio_audio — reads audio from Twilio, pipes to Deepgram
    2. process_speech — reads Deepgram transcripts, runs Claude, generates TTS
    3. send_greeting — fires the opening greeting as soon as the call connects
    """
    # Open Deepgram STT connection
    dg_key = os.environ.get("DEEPGRAM_API_KEY", "")
    if not dg_key:
        print(f"[DEEPGRAM ERROR] DEEPGRAM_API_KEY is empty or not set in environment — cannot connect")
        return
    deepgram_headers = {
        "Authorization": f"Token {dg_key}"
    }
    deepgram_params = (
        "?model=nova-2"
        "&language=en-US"
        "&encoding=mulaw"
        f"&sample_rate={SAMPLE_RATE}"
        "&channels=1"
        "&interim_results=true"
        "&endpointing=true"
        "&utterance_end_ms=1000"
        "&filler_words=true"
    )

    try:
        async with asyncio.timeout(10):  # fail loudly instead of hanging forever
            async with websockets.connect(
                DEEPGRAM_URL + deepgram_params,
                extra_headers=deepgram_headers
            ) as dg_ws:
                state.deepgram_ws = dg_ws
                print(f"[DEEPGRAM] Connected for call {state.call_sid}")

                # Run tasks concurrently
                await asyncio.gather(
                    receive_twilio_audio(websocket, state),
                    process_speech(websocket, state, db),
                    send_greeting(websocket, state),
                )
    except asyncio.TimeoutError:
        print(f"[DEEPGRAM ERROR] Connection timed out after 10s for call {state.call_sid} — check DEEPGRAM_API_KEY")
    except Exception as e:
        print(f"[DEEPGRAM ERROR] Failed to connect: {type(e).__name__}: {e}")


async def receive_twilio_audio(websocket: WebSocket, state: "CallState"):
    """
    Reads raw audio from Twilio's Media Stream WebSocket.
    Twilio sends base64-encoded mulaw audio in JSON events.
    We decode and forward to Deepgram.
    """
    async for message in websocket.iter_text():
        try:
            data = json.loads(message)
            event = data.get("event")

            if event == "connected":
                print(f"[TWILIO] Connected: {state.call_sid}")

            elif event == "start":
                # Twilio tells us about the stream configuration
                stream_sid = data["start"]["streamSid"]
                state.stream_sid = stream_sid
                state.caller_number = data["start"]["customParameters"].get("caller_number", "Unknown")
                print(f"[TWILIO] Stream started: {stream_sid}")

            elif event == "media":
                # Decode audio and forward to Deepgram
                payload = data["media"]["payload"]
                audio_bytes = base64.b64decode(payload)

                # Track whether caller is speaking (for silence detection)
                rms = audioop.rms(audio_bytes, 1)
                if rms > SILENCE_THRESHOLD:
                    state.last_speech_time = time.time()
                    state.caller_speaking = True

                # Forward to Deepgram
                if state.deepgram_ws and not state.ai_speaking:
                    await state.deepgram_ws.send(audio_bytes)

            elif event == "stop":
                print(f"[TWILIO] Stream stopped: {state.call_sid}")
                state.call_ended = True
                break

        except Exception as e:
            print(f"[TWILIO AUDIO ERROR] {e}")


async def process_speech(websocket: WebSocket, state: "CallState", db: AsyncSession):
    """
    Reads transcripts from Deepgram and runs the full AI pipeline:
    Deepgram transcript → Claude API → ElevenLabs TTS → Twilio audio
    """
    async for message in state.deepgram_ws:
        try:
            result = json.loads(message)

            # Handle Deepgram transcript events
            if result.get("type") == "Results":
                alternatives = result.get("channel", {}).get("alternatives", [])
                if not alternatives:
                    continue

                transcript = alternatives[0].get("transcript", "").strip()
                is_final = result.get("is_final", False)
                speech_final = result.get("speech_final", False)

                # Update partial transcript for UI streaming
                if transcript:
                    state.current_utterance = transcript
                    # Broadcast partial transcript to dashboard
                    await broadcast_transcript_update(
                        state.client_id,
                        state.call_sid,
                        speaker="caller",
                        text=transcript,
                        is_final=False
                    )

                # Only process when Deepgram signals end of utterance
                if (is_final and speech_final and transcript and not state.ai_speaking):
                    state.caller_speaking = False

                    # Add to conversation history
                    state.transcript.append({
                        "speaker": "caller",
                        "text": transcript,
                        "time": round(time.time() - state.call_start_time, 1)
                    })

                    await broadcast_transcript_update(
                        state.client_id, state.call_sid,
                        speaker="caller", text=transcript, is_final=True
                    )

                    # Run Claude and speak the response
                    await generate_and_speak(websocket, state, transcript)

            # Handle Deepgram errors
            elif result.get("type") == "Error":
                print(f"[DEEPGRAM ERROR] {result}")

        except Exception as e:
            print(f"[SPEECH PROCESS ERROR] {e}")

        if state.call_ended:
            break


async def send_greeting(websocket: WebSocket, state: "CallState"):
    """
    Fires the opening greeting as soon as the stream is established.
    Waits until stream_sid is set (stream started event received).
    """
    # Wait for stream to start
    for _ in range(50):  # max 5 second wait
        if state.stream_sid:
            break
        await asyncio.sleep(0.1)

    if not state.stream_sid:
        return

    # Small pause before greeting — feels more natural
    await asyncio.sleep(0.3)

    greeting = state.persona.get("greeting_script", "Thank you for calling. How can I help you today?")
    greeting = greeting.replace("{business_name}", state.client.name or "us")
    greeting = greeting.replace("{receptionist_name}", state.persona.get("name", "your AI receptionist"))

    await speak_text(websocket, state, greeting, is_greeting=True)


async def generate_and_speak(websocket: WebSocket, state: "CallState", caller_text: str):
    """
    The core intelligence step:
    1. Build messages array with full conversation history
    2. Call Claude API with streaming
    3. Stream response to ElevenLabs TTS in chunks
    4. Stream audio back to Twilio in real time

    Latency target: < 800ms from end of caller speech to first AI audio byte
    """
    state.ai_speaking = True

    # Check for escalation triggers
    escalation = await check_escalation_triggers(state, caller_text)
    if escalation:
        await handle_escalation(websocket, state, escalation)
        state.ai_speaking = False
        return

    # Build Claude messages
    messages = []
    for turn in state.conversation_history:
        messages.append({"role": turn["role"], "content": turn["content"]})
    messages.append({"role": "user", "content": caller_text})

    # Add to history
    state.conversation_history.append({"role": "user", "content": caller_text})

    # Stream from Claude
    full_response = ""
    sentence_buffer = ""

    try:
        async with anthropic_client.messages.stream(
            model="claude-sonnet-4-6",
            max_tokens=400,  # Keep responses concise for voice
            system=state.system_prompt,
            messages=messages,
        ) as stream:
            async for text_chunk in stream.text_stream:
                full_response += text_chunk
                sentence_buffer += text_chunk

                # Send to TTS as soon as we have a complete sentence
                # This reduces latency significantly — don't wait for full response
                if any(sentence_buffer.rstrip().endswith(p) for p in ['.', '?', '!', ',', ';']):
                    if len(sentence_buffer.strip()) > 10:  # avoid tiny fragments
                        await stream_tts_chunk(websocket, state, sentence_buffer.strip())
                        sentence_buffer = ""

            # Flush any remaining text
            if sentence_buffer.strip():
                await stream_tts_chunk(websocket, state, sentence_buffer.strip())

    except Exception as e:
        print(f"[CLAUDE ERROR] {e}")
        # Fallback response
        await speak_text(websocket, state, "I'm sorry, I had trouble with that. Could you repeat what you said?")
        state.ai_speaking = False
        return

    # Save AI response to transcript and history
    state.conversation_history.append({"role": "assistant", "content": full_response})
    state.transcript.append({
        "speaker": "ai",
        "text": full_response,
        "time": round(time.time() - state.call_start_time, 1)
    })

    await broadcast_transcript_update(
        state.client_id, state.call_sid,
        speaker="ai", text=full_response, is_final=True
    )

    # Detect outcomes from Claude's response
    await detect_outcomes(state, full_response)

    state.ai_speaking = False


async def stream_tts_chunk(websocket: WebSocket, state: "CallState", text: str):
    """
    Sends a text chunk to ElevenLabs, receives streaming audio,
    converts from mp3 to mulaw, and sends to Twilio in real time.

    This is called sentence-by-sentence to minimize latency.
    """
    voice_id = state.persona.get("voice_id", "I571sUNz6E53D5YaJgVg")
    stability = state.persona.get("stability", 0.5)
    similarity_boost = state.persona.get("similarity_boost", 0.75)
    speaking_rate = state.persona.get("speaking_rate", 1.0)

    url = ELEVENLABS_URL.format(voice_id=voice_id)

    headers = {
        "xi-api-key": os.environ["ELEVENLABS_API_KEY"],
        "Content-Type": "application/json",
        "Accept": "audio/mpeg",
    }

    payload = {
        "text": text,
        "model_id": "eleven_turbo_v2",  # Lowest latency ElevenLabs model
        "voice_settings": {
            "stability": stability,
            "similarity_boost": similarity_boost,
            "speaking_rate": speaking_rate,
        },
        "output_format": "ulaw_8000",  # Direct mulaw output — no conversion needed
    }

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as response:
                if response.status_code != 200:
                    print(f"[ELEVENLABS ERROR] {response.status_code}")
                    return

                # Stream audio chunks to Twilio
                async for chunk in response.aiter_bytes(chunk_size=AUDIO_CHUNK_SIZE):
                    if not chunk or state.call_ended:
                        break

                    # Encode as base64 and send to Twilio Media Stream
                    audio_b64 = base64.b64encode(chunk).decode("utf-8")
                    await websocket.send_json({
                        "event": "media",
                        "streamSid": state.stream_sid,
                        "media": {
                            "payload": audio_b64,
                        }
                    })

                    # Small yield to prevent blocking the event loop
                    await asyncio.sleep(0)

    except Exception as e:
        print(f"[TTS STREAM ERROR] {e}")


async def speak_text(websocket: WebSocket, state: "CallState", text: str, is_greeting: bool = False):
    """Convenience wrapper for speaking a full piece of text."""
    await stream_tts_chunk(websocket, state, text)
    if is_greeting:
        state.transcript.append({
            "speaker": "ai", "text": text,
            "time": round(time.time() - state.call_start_time, 1)
        })
        state.conversation_history.append({"role": "assistant", "content": text})


# ══════════════════════════════════════════════════════════════
# 4. SYSTEM PROMPT BUILDER
#    This is what makes each business's AI unique.
# ══════════════════════════════════════════════════════════════

def build_system_prompt(client, persona: dict, knowledge_base: str) -> str:
    name = persona.get("name", "your AI receptionist")
    style = persona.get("style", "professional")
    language = persona.get("language_primary", "en")
    bilingual = persona.get("bilingual_auto_switch", False)

    style_instructions = {
        "professional": "Be precise, formal, and efficient. Avoid filler words and small talk. Mirror the vocabulary of a top-tier corporate receptionist.",
        "friendly":     "Be warm, conversational, and personable. Use natural language. It's okay to say 'of course!' and 'absolutely!' — make callers feel welcome.",
        "concise":      "Be extremely brief. Answer only what was asked. Never elaborate unless directly requested. Avoid any pleasantries beyond a professional greeting.",
        "empathetic":   "Always acknowledge the caller's emotional state before moving to the solution. If someone sounds frustrated, validate them first.",
        "energetic":    "Be upbeat, positive, and enthusiastic. Match high energy. Use affirmative language.",
        "bilingual":    "Detect the caller's preferred language within the first exchange and match it for the rest of the call.",
    }.get(style, "Be professional and helpful.")

    lang_instruction = ""
    if bilingual:
        lang_instruction = "\nIf the caller switches to Spanish, respond in Spanish for the remainder of the call. If they switch back to English, respond in English."

    return f"""You are {name}, the AI voice receptionist for {client.name or 'this business'}.

STYLE: {style_instructions}{lang_instruction}

CRITICAL VOICE RULES:
- You are speaking aloud. Never use markdown, bullet points, headers, or lists.
- Speak in complete natural sentences only.
- Keep responses under 3 sentences unless more detail is required to answer a specific question.
- Never say "I cannot" or "I don't have access to" — instead say "Let me find that out" or offer an alternative.
- If asked something you don't know, say: "That's a great question — let me make sure I get you accurate information. Can I have someone call you back with the exact details?"
- Always sound confident, never robotic.
- Do not repeat the caller's name more than once per response.
- Use natural filler phrases occasionally: "Of course", "Absolutely", "Let me check on that".

APPOINTMENTS:
- If the caller wants to schedule, collect: name, address (if field service), best callback number, brief description of issue.
- Check calendar availability before confirming. If calendar is not connected, offer to have someone call back within 2 hours to confirm.
- Confirm all appointment details before ending the call.

PRICING / QUOTES:
- Never quote specific prices unless the knowledge base explicitly states them.
- Instead say: "Pricing depends on the specifics of your situation — I can schedule a free assessment or have someone call you with an accurate quote."

EMERGENCIES:
- If the caller mentions gas leaks, flooding, no heat in winter, electrical hazards, or uses words like "emergency" or "urgent" — immediately escalate.
- Say: "This sounds like it needs immediate attention. I'm alerting our emergency team right now. Can I confirm your address and the best number to reach you?"

NEVER:
- Transfer without warning the caller.
- Put someone on hold for more than 30 seconds without checking in.
- End the call without confirming next steps.
- Mention that you are an AI unless directly and sincerely asked.

BUSINESS KNOWLEDGE BASE:
{knowledge_base if knowledge_base else "(No knowledge base configured — answer general questions only and route specifics to staff.)"}
"""


# ══════════════════════════════════════════════════════════════
# 5. ESCALATION HANDLING
# ══════════════════════════════════════════════════════════════

async def check_escalation_triggers(state: "CallState", caller_text: str) -> Optional[str]:
    """
    Checks whether any escalation trigger is met.
    Returns trigger type string or None.
    """
    client_settings = state.client.settings or {}
    escalation_cfg = client_settings.get("escalation", {})

    if not escalation_cfg.get("enabled", False):
        return None

    triggers = escalation_cfg.get("triggers", [])
    text_lower = caller_text.lower()

    if "emergency_keywords" in triggers:
        emergency_words = ["gas leak", "flooding", "fire", "no heat", "carbon monoxide", "emergency", "urgent help", "burst pipe"]
        if any(word in text_lower for word in emergency_words):
            return "emergency"

    if "legal_threat" in triggers:
        legal_words = ["lawyer", "attorney", "lawsuit", "sue", "court", "legal action"]
        if any(word in text_lower for word in legal_words):
            return "legal_threat"

    if "explicit_request" in triggers:
        human_requests = ["speak to a human", "talk to a person", "real person", "actual human", "manager", "supervisor"]
        if any(phrase in text_lower for phrase in human_requests):
            return "human_request"

    return None


async def handle_escalation(websocket: WebSocket, state: "CallState", trigger: str):
    """Handles an escalation event — speaks transition message and transfers call."""
    client_settings = state.client.settings or {}
    escalation_cfg = client_settings.get("escalation", {})
    transfer_number = escalation_cfg.get("escalation_number", "")

    messages = {
        "emergency":     "This sounds urgent. I'm connecting you with our emergency team right now — please hold.",
        "legal_threat":  "I understand this is a serious matter. I'm transferring you to our management team immediately.",
        "human_request": "Of course — let me connect you with a team member right now.",
    }

    await speak_text(websocket, state, messages.get(trigger, "One moment — I'm connecting you with someone now."))

    if transfer_number:
        try:
            twilio_client.calls(state.call_sid).update(
                twiml=f'<Response><Dial>{transfer_number}</Dial></Response>'
            )
        except Exception as e:
            print(f"[TRANSFER ERROR] {e}")

    state.outcome = f"transferred_{trigger}"


# ══════════════════════════════════════════════════════════════
# 6. OUTCOME & LEAD SCORING
# ══════════════════════════════════════════════════════════════

async def detect_outcomes(state: "CallState", ai_response: str):
    """Detects outcomes from the AI's response text."""
    response_lower = ai_response.lower()

    if any(phrase in response_lower for phrase in ["booked", "scheduled", "appointment", "confirmed for"]):
        state.outcome = "booked"
    elif any(phrase in response_lower for phrase in ["call you back", "someone will call", "callback"]):
        state.outcome = "callback"
    elif any(phrase in response_lower for phrase in ["transferring", "connecting you", "hold while i"]):
        state.outcome = "transferred"
    elif not state.outcome:
        state.outcome = "handled"


async def score_lead(transcript: list) -> int:
    """
    Uses Claude to score the lead quality 1-10 based on transcript.
    Called after call ends.
    """
    if not transcript or len(transcript) < 2:
        return 1

    transcript_text = "\n".join(f"{t['speaker'].upper()}: {t['text']}" for t in transcript)

    try:
        response = await anthropic_client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=50,
            messages=[{
                "role": "user",
                "content": f"""Score this call transcript on lead quality 1-10. 
10 = ready to buy / booked immediately. 
1 = wrong number or zero intent. 
Respond with ONLY a number.

{transcript_text}"""
            }]
        )
        score_text = response.content[0].text.strip()
        return min(10, max(1, int(score_text)))
    except:
        return 5


# ══════════════════════════════════════════════════════════════
# 7. DASHBOARD BROADCASTING
#    Sends live events to business owner's dashboard
# ══════════════════════════════════════════════════════════════

# In-memory store of dashboard WebSocket connections
# Key: org_id, Value: set of WebSocket connections
dashboard_connections: dict[str, set] = {}


@router.websocket("/ws/dashboard/{org_id}")
async def dashboard_websocket(websocket: WebSocket, org_id: str):
    """
    Business owner's dashboard connects here to receive live call events.
    Events: call_started, transcript_update, call_ended, sentiment_update
    """
    await websocket.accept()

    if org_id not in dashboard_connections:
        dashboard_connections[org_id] = set()
    dashboard_connections[org_id].add(websocket)

    try:
        # Keep alive — just wait for disconnect
        while True:
            # Ping every 30s to detect dropped connections
            await asyncio.sleep(30)
            await websocket.send_json({"type": "ping"})
    except WebSocketDisconnect:
        dashboard_connections[org_id].discard(websocket)
    except Exception:
        dashboard_connections[org_id].discard(websocket)


async def broadcast_to_dashboard(org_id: str, event: dict):
    """Send an event to all dashboard connections for an org."""
    if org_id not in dashboard_connections:
        return
    dead = set()
    for ws in dashboard_connections[org_id]:
        try:
            await ws.send_json(event)
        except:
            dead.add(ws)
    dashboard_connections[org_id] -= dead


async def broadcast_transcript_update(
    client_id: str, call_sid: str,
    speaker: str, text: str, is_final: bool
):
    await broadcast_to_dashboard(client_id, {
        "type": "transcript_update",
        "call_sid": call_sid,
        "speaker": speaker,
        "text": text,
        "is_final": is_final,
        "timestamp": time.time(),
    })


async def broadcast_call_ended(client_id: str, call_sid: str, call_log):
    await broadcast_to_dashboard(client_id, {
        "type": "call_ended",
        "call_sid": call_sid,
        "duration": call_log.duration_seconds,
        "outcome": call_log.outcome,
        "lead_score": call_log.lead_score,
        "sentiment": call_log.sentiment,
    })


# ══════════════════════════════════════════════════════════════
# 8. CALL STATE
# ══════════════════════════════════════════════════════════════

class CallState:
    """Holds all mutable state for a single active call."""

    def __init__(self, client_id, call_sid, client, persona, system_prompt):
        self.client_id = client_id
        self.call_sid = call_sid
        self.client = client
        self.persona = persona
        self.system_prompt = system_prompt

        self.stream_sid: Optional[str] = None
        self.caller_number: str = "Unknown"
        self.call_start_time: float = time.time()

        self.deepgram_ws = None
        self.ai_speaking: bool = False
        self.caller_speaking: bool = False
        self.last_speech_time: float = time.time()
        self.current_utterance: str = ""
        self.call_ended: bool = False

        self.conversation_history: list = []
        self.transcript: list = []

        self.outcome: Optional[str] = None
        self.final_sentiment: str = "neutral"


# ══════════════════════════════════════════════════════════════
# DEPLOYMENT CHECKLIST
# ══════════════════════════════════════════════════════════════
#
# 1. Environment variables (all required):
#    TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_PHONE_NUMBER
#    DEEPGRAM_API_KEY
#    ANTHROPIC_API_KEY
#    ELEVENLABS_API_KEY
#    DATABASE_URL
#    BASE_URL=https://your-deployed-backend.com
#
# 2. Your deployed backend URL MUST be HTTPS/WSS (not localhost).
#    Twilio webhooks require a public SSL URL.
#    Use Render, Railway, or Fly.io with their auto-SSL.
#
# 3. In Twilio console:
#    Phone Number → Voice → "A call comes in" → Webhook
#    Set to: https://your-backend.com/calls/inbound/{client_id}
#    Method: HTTP POST
#
# 4. Deepgram account:
#    Create a key at https://console.deepgram.com
#    nova-2 model is best for English phone audio
#
# 5. ElevenLabs account:
#    eleven_turbo_v2 is the lowest-latency model
#    Voice IDs are in your ElevenLabs voice library
#    ulaw_8000 output format = direct Twilio compatibility, no conversion
#
# 6. pip install requirements:
#    pip install fastapi uvicorn websockets httpx anthropic deepgram-sdk
#             python-twilio elevenlabs sqlalchemy asyncpg audioop-lts
#
# 7. Run with:
#    uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
#    (use multiple workers — each call is its own event loop)
