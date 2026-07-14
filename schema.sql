-- ═══════════════════════════════════════════════════════════════
-- AI Receptionist SaaS — Full Database Schema
-- PostgreSQL 14+
-- Run this once against a fresh database:
--   psql $DATABASE_URL -f schema.sql
-- ═══════════════════════════════════════════════════════════════

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ─────────────────────────────────────────────────────────────
-- ORGANIZATIONS
-- One row per business that signs up. The top-level entity.
-- Everything else belongs to an org.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE organizations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name            TEXT NOT NULL,
    business_type   TEXT NOT NULL DEFAULT 'hvac',  -- hvac, plumbing, roofing, medical, dental, legal, other
    plan            TEXT NOT NULL DEFAULT 'trial',  -- trial, basic, pro, enterprise
    plan_status     TEXT NOT NULL DEFAULT 'active', -- active, paused, cancelled, past_due
    stripe_customer_id      TEXT UNIQUE,
    stripe_subscription_id  TEXT UNIQUE,
    trial_ends_at   TIMESTAMPTZ DEFAULT (NOW() + INTERVAL '14 days'),
    onboarding_complete     BOOLEAN NOT NULL DEFAULT FALSE,
    onboarding_step         INTEGER NOT NULL DEFAULT 1,  -- which wizard step they're on
    settings        JSONB NOT NULL DEFAULT '{}',    -- ai persona, hours, escalation, etc.
    twilio_phone_number     TEXT,                   -- the number we provisioned for them
    twilio_phone_sid        TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
-- USERS
-- Humans who log in. Belong to an org. Multiple users per org.
-- Role controls what they can see in the dashboard.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE users (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    email           TEXT NOT NULL UNIQUE,
    password_hash   TEXT NOT NULL,                  -- bcrypt hash, never store plaintext
    name            TEXT NOT NULL,
    role            TEXT NOT NULL DEFAULT 'owner',  -- owner, manager, receptionist, billing_contact, operator
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    last_login_at   TIMESTAMPTZ,
    invited_by      UUID REFERENCES users(id),
    invite_token    TEXT UNIQUE,                    -- set when invited, cleared on first login
    invite_accepted BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for login lookups (most common query)
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_org_id ON users(org_id);

-- ─────────────────────────────────────────────────────────────
-- REFRESH TOKENS
-- JWT access tokens are short-lived (15 min).
-- Refresh tokens are long-lived (30 days), stored in DB so we
-- can invalidate them on logout or suspicious activity.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE refresh_tokens (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    token_hash      TEXT NOT NULL UNIQUE,           -- SHA-256 hash of the actual token
    expires_at      TIMESTAMPTZ NOT NULL DEFAULT (NOW() + INTERVAL '30 days'),
    revoked         BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at      TIMESTAMPTZ,
    user_agent      TEXT,                           -- browser/device for security display
    ip_address      INET,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_refresh_tokens_user_id ON refresh_tokens(user_id);
CREATE INDEX idx_refresh_tokens_hash ON refresh_tokens(token_hash);

-- ─────────────────────────────────────────────────────────────
-- KNOWLEDGE BASE
-- Chunks of text the AI uses to answer questions.
-- Each chunk has a source, section, and confidence score.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE knowledge_base (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    section         TEXT NOT NULL,                  -- Hours, Services, Pricing, FAQ, etc.
    text            TEXT NOT NULL,
    source          TEXT NOT NULL DEFAULT 'manual', -- website, pdf, manual
    source_url      TEXT,
    confidence      FLOAT NOT NULL DEFAULT 1.0,     -- 0.0 - 1.0
    needs_review    BOOLEAN NOT NULL DEFAULT FALSE,
    conflict_with   UUID REFERENCES knowledge_base(id),
    is_locked       BOOLEAN NOT NULL DEFAULT FALSE,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    version         INTEGER NOT NULL DEFAULT 1,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_kb_org_id ON knowledge_base(org_id);
CREATE INDEX idx_kb_active ON knowledge_base(org_id, is_active);

-- KB version history — every edit creates a row here
CREATE TABLE knowledge_base_history (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    kb_id           UUID NOT NULL REFERENCES knowledge_base(id) ON DELETE CASCADE,
    org_id          UUID NOT NULL,
    text_before     TEXT NOT NULL,
    text_after      TEXT NOT NULL,
    edited_by       UUID REFERENCES users(id),
    version         INTEGER NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
-- CALENDAR AUTH
-- OAuth tokens for Google Calendar / Outlook.
-- Tokens are encrypted at rest using Fernet (set ENCRYPTION_KEY env).
-- ─────────────────────────────────────────────────────────────
CREATE TABLE calendar_auth (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL DEFAULT 'google', -- google, outlook
    access_token    TEXT NOT NULL,                  -- Fernet encrypted
    refresh_token   TEXT NOT NULL,                  -- Fernet encrypted
    token_expiry    TIMESTAMPTZ NOT NULL,
    calendar_id     TEXT,                           -- which calendar to book into
    connected_email TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, provider)
);

-- ─────────────────────────────────────────────────────────────
-- CALL LOGS
-- Every call gets one row. Transcript stored as JSONB array.
-- Indexed for fast filtering by org, date, outcome, intent.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE call_logs (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    call_sid        TEXT UNIQUE NOT NULL,           -- Twilio call SID
    caller_number   TEXT NOT NULL,
    caller_name     TEXT,                           -- resolved from CRM or reverse lookup
    direction       TEXT NOT NULL DEFAULT 'inbound',-- inbound, outbound (test calls)
    status          TEXT NOT NULL DEFAULT 'active', -- active, completed, failed, missed
    outcome         TEXT,                           -- booked, resolved, callback, transferred, voicemail
    intent          TEXT,                           -- appointment, quote, emergency, general, complaint, etc.
    duration_seconds INTEGER,
    recording_url   TEXT,                           -- Twilio recording URL
    transcript      JSONB NOT NULL DEFAULT '[]',   -- [{speaker, text, time}, ...]
    summary         TEXT,                           -- AI-generated post-call summary
    sentiment       TEXT DEFAULT 'neutral',         -- positive, neutral, negative
    lead_score      INTEGER,                        -- 1-10
    competitor_mentioned TEXT,
    follow_up_tasks JSONB DEFAULT '[]',             -- [{text, completed}, ...]
    whispers        JSONB DEFAULT '[]',             -- whisper messages sent during call
    is_test_call    BOOLEAN NOT NULL DEFAULT FALSE,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_calls_org_id ON call_logs(org_id);
CREATE INDEX idx_calls_started_at ON call_logs(org_id, started_at DESC);
CREATE INDEX idx_calls_outcome ON call_logs(org_id, outcome);
CREATE INDEX idx_calls_intent ON call_logs(org_id, intent);
CREATE INDEX idx_calls_caller ON call_logs(org_id, caller_number);

-- ─────────────────────────────────────────────────────────────
-- CALLBACK QUEUE
-- Callers who were promised a callback.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE callback_queue (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    call_log_id     UUID REFERENCES call_logs(id),
    caller_name     TEXT,
    caller_number   TEXT NOT NULL,
    reason          TEXT,
    best_time       TEXT,
    priority        TEXT NOT NULL DEFAULT 'medium', -- high, medium, low
    status          TEXT NOT NULL DEFAULT 'pending',-- pending, completed, cancelled
    assigned_to     UUID REFERENCES users(id),
    completed_at    TIMESTAMPTZ,
    completed_by    UUID REFERENCES users(id),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_callbacks_org_id ON callback_queue(org_id, status);

-- ─────────────────────────────────────────────────────────────
-- TEAM INVITES (tracked via users table + invite_token)
-- But we also log invite sends here for audit trail
-- ─────────────────────────────────────────────────────────────
CREATE TABLE invite_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id),
    invited_email   TEXT NOT NULL,
    invited_role    TEXT NOT NULL,
    invited_by      UUID REFERENCES users(id),
    invite_token    TEXT NOT NULL,
    accepted_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─────────────────────────────────────────────────────────────
-- INTEGRATIONS
-- Which third-party services each org has connected.
-- Config and credentials stored encrypted in `config` JSONB.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE integrations (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,
    provider        TEXT NOT NULL,                  -- hubspot, salesforce, jobber, zapier, etc.
    status          TEXT NOT NULL DEFAULT 'active', -- active, disconnected, error
    config          JSONB NOT NULL DEFAULT '{}',    -- encrypted credentials + settings
    last_sync_at    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(org_id, provider)
);

-- ─────────────────────────────────────────────────────────────
-- AUDIT LOG
-- Who did what, when. Critical for multi-user environments.
-- ─────────────────────────────────────────────────────────────
CREATE TABLE audit_log (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    org_id          UUID NOT NULL REFERENCES organizations(id),
    user_id         UUID REFERENCES users(id),
    action          TEXT NOT NULL,                  -- e.g. "kb.update", "settings.save", "invite.send"
    entity_type     TEXT,                           -- "knowledge_base", "user", "settings"
    entity_id       UUID,
    before          JSONB,
    after           JSONB,
    ip_address      INET,
    user_agent      TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_audit_org_id ON audit_log(org_id, created_at DESC);

-- ─────────────────────────────────────────────────────────────
-- COMPLIANCE SCRIPTS
-- State-by-state recording disclosure scripts (all 50 + DC)
-- Pre-populated by seed script, read-only at runtime
-- ─────────────────────────────────────────────────────────────
CREATE TABLE state_compliance (
    state_code      CHAR(2) PRIMARY KEY,
    state_name      TEXT NOT NULL,
    consent_type    TEXT NOT NULL,          -- one_party, two_party
    disclosure_script TEXT NOT NULL,
    notes           TEXT
);

-- ─────────────────────────────────────────────────────────────
-- TRIGGERS — auto-update updated_at on every table that has it
-- ─────────────────────────────────────────────────────────────
CREATE OR REPLACE FUNCTION update_updated_at()
RETURNS TRIGGER AS $$
BEGIN NEW.updated_at = NOW(); RETURN NEW; END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_orgs_updated_at      BEFORE UPDATE ON organizations      FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_users_updated_at     BEFORE UPDATE ON users              FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_kb_updated_at        BEFORE UPDATE ON knowledge_base     FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_cal_updated_at       BEFORE UPDATE ON calendar_auth      FOR EACH ROW EXECUTE FUNCTION update_updated_at();
CREATE TRIGGER trg_integrations_updated BEFORE UPDATE ON integrations       FOR EACH ROW EXECUTE FUNCTION update_updated_at();
