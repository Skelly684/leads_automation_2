
# Robust .env loader: load the .env next to this file even if uvicorn is started elsewhere
import os
from pathlib import Path
from dotenv import load_dotenv, find_dotenv

DOTENV_PATH = os.getenv("DOTENV_PATH") or (
    Path(__file__).with_name(".env") if Path(__file__).with_name(".env").exists()
    else find_dotenv(usecwd=True) or ".env"
)
load_dotenv(DOTENV_PATH)
print(f"[ENV] Loaded: {DOTENV_PATH}")
def _env(name: str, default: str = "") -> str:
    # Strip whitespace and hidden CR that break URLs/keys
    return (os.getenv(name, default) or "").strip().replace("\r", "")

from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, RedirectResponse, HTMLResponse
import requests
import json
from supabase import create_client, Client
from datetime import datetime, timedelta, timezone
import pytz
import phonenumbers
from phonenumbers import timezone as ph_timezone
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.schedulers.base import SchedulerAlreadyRunningError
import re
import smtplib
from email.message import EmailMessage
import os
from typing import Optional, Tuple, Dict, List
from uuid import uuid4
import base64
from urllib.parse import urlencode
import sys
from fastapi import Query, BackgroundTasks, HTTPException
import asyncio, os
from uuid import uuid4
# --- Credits (shared-by-domain) ---
from credits import (
    PRICE_CENTS_PER_MINUTE,
    MIN_RESERVE_CENTS,
    email_domain_of,
    domain_balance,
    domain_add_credits,
    domain_spend_credits,
    ensure_credit_before_call,
    bill_call_completion,
)

# ---- Google libs ----
try:
    from google.oauth2.credentials import Credentials as GCredentials
    from google_auth_oauthlib.flow import Flow as GFlow
    from googleapiclient.discovery import build as g_build
    from google.auth.transport.requests import Request as GAuthRequest
    _GOOGLE_LIBS_AVAILABLE = True
except Exception:
    print("[Google] WARNING: missing google libs", file=sys.stderr)
    _GOOGLE_LIBS_AVAILABLE = False

# ==== Config ====
VOICE_PROVIDER_NAME = os.getenv("VOICE_PROVIDER_NAME", "voice")
VAPI_API_KEY = os.getenv("VAPI_API_KEY", "")
VAPI_ASSISTANT_ID = os.getenv("VAPI_ASSISTANT_ID", "")
VAPI_PHONE_NUMBER_ID = os.getenv("VAPI_PHONE_NUMBER_ID", "")

SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "")
DEFAULT_USER_ID = os.getenv("DEFAULT_USER_ID")

CALL_WINDOW_START = 9
CALL_WINDOW_END   = 18
MAX_RETRIES = 3
RETRY_MINUTES = 30

SMTP_HOST = "smtp.gmail.com"
SMTP_PORT = 587
EMAIL_FROM = os.getenv("EMAIL_FROM", "")
EMAIL_FROM_NAME = os.getenv("EMAIL_FROM_NAME", "")
EMAIL_APP_PASSWORD = os.getenv("EMAIL_APP_PASSWORD", "")
MAX_EMAILS_PER_DAY = int(os.getenv("MAX_EMAILS_PER_DAY", "150"))
# Kill switches / safety flags
EMAIL_SENDING_ENABLED = _env("EMAIL_SENDING_ENABLED", "true").lower() == "true"
EMAIL_SEQUENCE_SCHEDULER_ENABLED = _env("EMAIL_SEQUENCE_SCHEDULER_ENABLED", "true").lower() == "true"
ALLOW_SMTP_FALLBACK   = _env("ALLOW_SMTP_FALLBACK",  "false").lower() == "true"
SKIP_SUPABASE_PROBE   = _env("SKIP_SUPABASE_PROBE", "false").lower() == "true"
# --- Process role (web or worker) ---
PROCESS_ROLE = _env("PROCESS_ROLE", "web").lower()
print(f"[ENV] PROCESS_ROLE={PROCESS_ROLE}")
print(f"[ENV] EMAIL_SEQUENCE_SCHEDULER_ENABLED(raw)={os.getenv('EMAIL_SEQUENCE_SCHEDULER_ENABLED')}")
print(f"[ENV] EMAIL_SEQUENCE_SCHEDULER_ENABLED(parsed)={EMAIL_SEQUENCE_SCHEDULER_ENABLED}")
# --- Apify (NL actor) ---
APIFY_TOKEN = os.getenv("APIFY_TOKEN", "").strip()
APIFY_ACTOR_ID = "VYRyEF4ygTTkaIghe"  # pipelinelabs~lead-scraper-apollo-zoominfo-lusha
if not APIFY_TOKEN:
    print("!!! WARNING: APIFY_TOKEN not set. NL scrape endpoint will fail.")

TERMINAL_STATUSES = {"completed", "no-answer", "busy", "failed", "canceled"}

GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
OAUTH_EXTERNAL_BASE_URL = os.getenv("OAUTH_EXTERNAL_BASE_URL", "").rstrip("/")
GOOGLE_REDIRECT_PATH = "/auth/google/callback"
GOOGLE_REDIRECT_URI = os.getenv(
    "GOOGLE_REDIRECT_URI",
    f"{OAUTH_EXTERNAL_BASE_URL}{GOOGLE_REDIRECT_PATH}" if OAUTH_EXTERNAL_BASE_URL else ""
).strip()

GOOGLE_SCOPES = [
    "https://www.googleapis.com/auth/calendar.readonly",
    "https://www.googleapis.com/auth/calendar.events",
    "https://www.googleapis.com/auth/gmail.send",
    "https://www.googleapis.com/auth/gmail.readonly",  # NEW: to read inbound messages
]

GOOGLE_TOKENS_TABLE = os.getenv("GOOGLE_TOKENS_TABLE", "google_oauth_tokens")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"],
)

def _get_request_user_id(req: Request) -> Optional[str]:
    hdr = req.headers.get("X-User-Id")
    if hdr: return hdr.strip()
    uid = req.query_params.get("user_id")
    if uid: return uid.strip()
    return None

@app.get("/api/health")
def health():
    return JSONResponse({"ok": True, "time": datetime.utcnow().isoformat()}, headers={"Cache-Control": "no-store"})

# ---------------------------------------------------
# Supabase diagnostics (at startup)
# ---------------------------------------------------
def _decode_jwt_role(jwt: str) -> str:
    try:
        parts = jwt.split(".")
        if len(parts) != 3:
            return "invalid_jwt"
        payload_b64 = parts[1] + "==="
        payload = json.loads(base64.urlsafe_b64decode(payload_b64.encode()).decode("utf-8"))
        return str(payload.get("role", "unknown"))
    except Exception:
        return "unknown"

def _short(s: str, n: int = 6) -> str:
    if not s:
        return ""
    return f"{s[:n]}…{s[-n:]}"

def _assert_supabase_ok():
    role = _decode_jwt_role(SUPABASE_KEY or "")
    print(f"[Supabase] URL={SUPABASE_URL} | KEY role={role} | key={_short(SUPABASE_KEY, 8)}")
    if role != "service_role":
        print("!!! WARNING: Supabase key is not service_role. Calls to PostgREST may be unauthorized.")
    try:
        supabase.table("leads").select("id").limit(1).execute()
        print("[Supabase] Probe OK")
    except Exception as e:
        print("!!! FATAL: Supabase probe failed. Check SUPABASE_URL / SUPABASE_KEY.")
        print("Error:", e)
        raise

# ==============================================
# Google Sheet CSV downloader (public/exportable sheets)
# ==============================================
def _extract_sheet_id(sheet_url: str) -> str | None:
    # expects https://docs.google.com/spreadsheets/d/<ID>/edit...
    import re
    m = re.search(r"/spreadsheets/d/([a-zA-Z0-9-_]+)", sheet_url)
    return m.group(1) if m else None

def _download_csv_from_sheet(sheet_url: str, gid_candidates=(0, 1837663021)) -> tuple[bytes, int] | None:
    """
    Returns (csv_bytes, gid) when accessible, otherwise None.
    NOTE: This only works if the sheet is shared "Anyone with link -> Viewer" AND allows download/print/copy,
    or if it's 'Published to the web' as CSV. Otherwise Google returns HTML.
    """
    sid = _extract_sheet_id(sheet_url)
    if not sid:
        return None
    for gid in gid_candidates:
        export = f"https://docs.google.com/spreadsheets/d/{sid}/export?format=csv&gid={gid}"
        r = requests.get(export, timeout=60)
        head = (r.content or b"")[:40].lower()
        # crude HTML sniff; CSV shouldn't begin with html
        if b"<!doctype html" in head or b"<html" in head:
            continue
        if r.status_code == 200 and r.content:
            return (r.content, gid)
    return None

# ===================================================
# Campaign rules (merge overrides with sane defaults)
# ===================================================
_CAMPAIGN_RULES_AVAILABLE = True

def get_campaign_rules(campaign_id: Optional[str]) -> Dict:
    """
    Returns a flat dict with keys used by call/email code:
      send_email, send_calls, call_window_start, call_window_end,
      max_attempts, retry_minutes, email (nested dict: send_initial)
    Accepts either our flat shape OR a nested {use_email,use_calls, call:{...}} from DB.
    """
    rules = {
        "send_email": True,
        "send_calls": True,
        "call_window_start": CALL_WINDOW_START,
        "call_window_end": CALL_WINDOW_END,
        "max_attempts": MAX_RETRIES,
        "retry_minutes": RETRY_MINUTES,
        "email": {"send_initial": True},
    }
    global _CAMPAIGN_RULES_AVAILABLE
    if not campaign_id or not _CAMPAIGN_RULES_AVAILABLE:
        return rules

    try:
        r = supabase.table("campaigns").select("delivery_rules").eq("id", campaign_id).single().execute()
        data = getattr(r, "data", None)
        if not data:
            return rules
        dr = (data.get("delivery_rules") or {}) if isinstance(data.get("delivery_rules"), dict) else {}

        # bool toggles (accept multiple spellings)
        if "send_email" in dr: rules["send_email"] = bool(dr["send_email"])
        if "send_calls" in dr: rules["send_calls"] = bool(dr["send_calls"])
        if "use_email" in dr:  rules["send_email"] = bool(dr["use_email"])
        if "use_calls" in dr:  rules["send_calls"] = bool(dr["use_calls"])

        # nested call dict (map to flat)
        call_dr = dr.get("call") or {}
        if isinstance(call_dr, dict):
            if "window_start" in call_dr: rules["call_window_start"] = int(call_dr["window_start"])
            if "window_end"   in call_dr: rules["call_window_end"]   = int(call_dr["window_end"])
            if "max_attempts" in call_dr: rules["max_attempts"]      = int(call_dr["max_attempts"])
            if "retry_minutes" in call_dr: rules["retry_minutes"]    = int(call_dr["retry_minutes"])

        # nested email dict
        email_dr = dr.get("email") or {}
        if isinstance(email_dr, dict):
            if "send_initial" in email_dr:
                rules["email"]["send_initial"] = bool(email_dr["send_initial"])
    except Exception:
        _CAMPAIGN_RULES_AVAILABLE = False
        print("Campaign rules lookup disabled (missing column/table). Using defaults.")

    return rules

# ===================================================
# Helpers: phone/timezone (for calls)
# ===================================================
def get_valid_phone(lead):
    """
    Try multiple likely places/aliases for a phone. Accepts raw strings or the
    structured array we get from Apollo. Returns a string like '+14155551212' or None.
    """
    # direct fields (different UIs/scrapers use different names)
    for k in ["cleanedPhoneNumber", "phone", "phone_number", "mobile", "mobile_number", "cell", "work_phone", "telephone", "tel", "primary_phone", "contact_number"]:
        v = lead.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    # nested 'company' payload sometimes contains phone
    comp = lead.get("company")
    if isinstance(comp, dict):
        for k in ["phone", "phone_number", "main_phone", "switchboard"]:
            v = comp.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    # Apollo-style list of numbers
    nums = lead.get("contact_phone_numbers")
    if isinstance(nums, str):
        try:
            nums = json.loads(nums)
        except Exception:
            nums = None
    if nums and isinstance(nums, list) and len(nums) > 0:
        first = nums[0]
        if isinstance(first, dict):
            # prefer sanitized if present
            return (first.get("sanitizedNumber") or first.get("rawNumber") or "").strip() or None
        if isinstance(first, str) and first.strip():
            return first.strip()

    return None

def get_local_tz_for_phone(phone) -> Optional[pytz.BaseTzInfo]:
    try:
        number = phonenumbers.parse(str(phone), None)
        tz_names = ph_timezone.time_zones_for_number(number)
        if not tz_names:
            return None
        return pytz.timezone(tz_names[0])
    except Exception:
        return None

def in_call_window_now(phone, start_hour: int, end_hour: int) -> bool:
    tz = get_local_tz_for_phone(phone)
    if not tz:
        return False
    now_local = datetime.now(tz)
    return start_hour <= now_local.hour < end_hour

def next_window_start(phone, start_hour: int, end_hour: int) -> Optional[datetime]:
    tz = get_local_tz_for_phone(phone)
    if not tz:
        return None
    now_local = datetime.now(tz)
    start_today = now_local.replace(hour=start_hour, minute=0, second=0, microsecond=0)
    end_today   = now_local.replace(hour=end_hour,   minute=0, second=0, microsecond=0)
    if now_local < start_today:
        next_local = start_today
    elif now_local >= end_today:
        next_local = (start_today + timedelta(days=1))
    else:
        next_local = now_local
    return next_local.astimezone(pytz.UTC)

def get_campaign_email_steps(campaign_id: Optional[str]) -> List[dict]:
    """Return active steps sorted by step_number for the campaign."""
    if not campaign_id:
        return []
    try:
        res = (supabase.table("campaign_email_steps")
               .select("*")
               .eq("campaign_id", campaign_id)
               .eq("is_active", True)
               .order("step_number", desc=False)
               .limit(5)
               .execute())
        return getattr(res, "data", []) or []
    except Exception as e:
        print("[EMAIL SEQ] fetch steps failed:", e)
        return []

def get_campaign_caller_config(campaign_id: Optional[str]) -> Dict:
    """
    Load the per-campaign caller configuration stored at campaigns.delivery_rules.caller.
    Returns a dict with safe defaults if absent.
    """
    defaults = {
        "opening_script": "",
        "goal": "qualify",
        "tone": "professional",
        "disclose_ai": False,
        "max_duration_sec": 180,
        "qualify_questions": [],
        "objections": [],
        "booking_link": None,
        "transfer_number": None,
        "voicemail_script": None,
        "not_interested_policy": "none",
        "disclaimer": None,
    }
    if not campaign_id:
        return defaults
    try:
        r = supabase.table("campaigns").select("delivery_rules").eq("id", campaign_id).single().execute()
        row = getattr(r, "data", None) or {}
        dr = (row.get("delivery_rules") or {}) if isinstance(row.get("delivery_rules"), dict) else {}
        caller = dr.get("caller") or {}
        if isinstance(caller, dict):
            return {**defaults, **caller}
    except Exception as e:
        print("[CallerConfig] load failed:", e)
    return defaults

def build_vapi_instructions_from_config(cfg: Dict) -> str:
    """
    Returns a compact system prompt for the Vapi assistant that:
    - uses only campaign-provided content,
    - never relies on a hard-coded script,
    - always extracts lead name + company and returns them in the call summary.
    """
    opening = (cfg.get("opening_script") or "").strip()
    tone = (cfg.get("tone") or "professional").replace("_", " ")
    goal = (cfg.get("goal") or "qualify").lower()
    disclose = bool(cfg.get("disclose_ai"))
    max_sec = int(cfg.get("max_duration_sec") or 180)
    disclaimer = (cfg.get("disclaimer") or "").strip()

    qlist = [q.strip() for q in (cfg.get("qualify_questions") or []) if q and str(q).strip()]
    obj_pairs = [o for o in (cfg.get("objections") or []) if isinstance(o, dict)]

    # Build objection lines
    obj_text = ""
    if obj_pairs:
        lines = []
        for o in obj_pairs:
            ob = (o.get("objection") or "").strip()
            rp = (o.get("response") or "").strip()
            if ob and rp:
                lines.append(f"- If lead says \"{ob}\": respond with \"{rp}\".")
        obj_text = "\n".join(lines)

    questions = ""
    if qlist:
        questions = "\n".join([f"- {q}" for q in qlist])

    booking_line = ""
    if cfg.get("booking_link"):
        booking_line = f"If lead is positive and time allows, offer to book at this link: {cfg['booking_link']}."

    transfer_line = ""
    if cfg.get("transfer_number"):
        transfer_line = f"If lead clearly requests to talk now, attempt live transfer to {cfg['transfer_number']}."

    voicemail_line = ""
    if cfg.get("voicemail_script"):
        voicemail_line = f"If you reach voicemail, leave this message verbatim (≤ 20s): {cfg['voicemail_script']}"

    notint = (cfg.get("not_interested_policy") or "none").lower()
    notint_line = ""
    if notint == "mark_do_not_contact":
        notint_line = "If the lead is not interested, politely close the call and note DO_NOT_CONTACT in the summary."
    elif notint == "send_followup_email":
        notint_line = "If the lead is not interested, politely close the call and note FOLLOW_UP_EMAIL in the summary."

    disclose_line = "If asked, you may state you're an AI assistant." if disclose else \
                    "Do not volunteer that you are AI; if asked directly, acknowledge briefly."

    return f"""
You are a concise, {tone} outbound caller for multiple different companies.
Obey the campaign config below. Never invent details beyond it.

OPENING (say naturally, adjust names): {opening}

GOAL: {goal}. Keep the entire call ≤ {max_sec} seconds.

TONE: {tone}. {disclose_line}

QUALIFY (ask only if needed, keep it brief):
{questions if questions else "- Ask one clarifying question if required; otherwise proceed."}

OBJECTIONS:
{obj_text if obj_text else "- Handle politely; offer to email a one-pager or book later."}

OPERATIONS:
- Never ask for, confirm, or repeat the lead's full name, company, or job title — even if not provided.
- You already have all the context you need; speak directly to "you" instead of using names.
- If the lead says they are available "now" or wants to discuss in detail immediately, DO NOT go into any specifics.
  Instead say: "Perfect — I’ll have one of our senior agents give you a call shortly to go over everything in detail."
- Never discuss investment details, property data, or pricing on this call.
- Never read or summarise the call out loud over the call.
- {booking_line}
- {transfer_line}
- {voicemail_line}
- {notint_line}

COMPLIANCE:
{disclaimer if disclaimer else "Be polite and compliant with local calling norms."}

SUMMARY FORMAT (must be included in provider summary metadata):
name=<Full Name>; company=<Company>; intent=<positive|neutral|negative>; action=<booked|transferred|followup|dnc|none>; notes=<one sentence>.
""".strip()

# ===================================================
# Helpers: Supabase writes (calls)
# ===================================================
def log_call_to_supabase(lead_id, call_status, notes=""):
    if not lead_id:
        print(f"[WARN] Skipping call log (missing lead_id). status={call_status} notes={notes[:120]}")
        return
    try:
        supabase.table("call_logs").insert({
            "lead_id": lead_id,
            "call_status": call_status,
            "notes": (notes or "")[:1000],
        }).execute()
    except Exception as e:
        print("Call log insert failed:", e)

def log_call_enqueued_structured(lead_id: str, attempt_number: int, external_call_id: Optional[str]):
    try:
        supabase.table("call_logs").insert({
            "lead_id": lead_id,
            "call_status": "queued",
            "provider": VOICE_PROVIDER_NAME,
            "external_call_id": external_call_id,
            "provider_call_id": external_call_id,
            "attempt_number": attempt_number,
            "started_at": datetime.utcnow().isoformat()
        }).execute()
    except Exception as e:
        print("Structured enqueue log failed:", e)

def update_structured_call_log(lead_id: str, external_call_id: Optional[str], patch: Dict):
    try:
        if external_call_id:
            supabase.table("call_logs").update(patch).eq("external_call_id", external_call_id).execute()
            return
        rows = (supabase.table("call_logs").select("id")
                .eq("lead_id", lead_id).eq("call_status", "queued")
                .order("created_at", desc=True).limit(1).execute())
        data = getattr(rows, "data", []) or []
        if data:
            rid = data[0]["id"]
            supabase.table("call_logs").update(patch).eq("id", rid).execute()
    except Exception as e:
        print("Structured call_log update failed:", e)

def update_lead(lead_id, patch: dict):
    if not lead_id:
        print("[WARN] update_lead called with empty lead_id. Patch ignored.")
        return
    patch = {**patch, "updated_at": datetime.utcnow().isoformat()}
    try:
        supabase.table("leads").update(patch).eq("id", lead_id).execute()
    except Exception as e:
        print("Lead update failed:", e)

def stop_sequence_for_lead(lead_id: str, reason: str = "reply"):
    """Stop any pending follow-up emails for this lead."""
    if not lead_id:
        return
    try:
        # Flip the stop flag and clear any future scheduling fields
        update_lead(lead_id, {
            "email_sequence_stopped": True,
            "next_email_at": None,
            "last_email_status": "reply" if reason == "reply" else "stopped",
            "last_email_reply_at": datetime.utcnow().isoformat() if reason == "reply" else None,
        })
        # Optional visibility entry
        try:
            supabase.table("email_logs").insert({
                "lead_id": lead_id,
                "to_email": "",
                "status": "sent",
                "provider": "system",
                "subject": "[Sequence Stopped]",
                "body": f"Sequence stopped due to {reason}",
                "notes": "auto-cancel on reply"
            }).execute()
        except Exception:
            pass
    except Exception as e:
        print("[SEQ] stop_sequence_for_lead failed:", e)

def schedule_next_call(lead_id, when_utc: datetime):
    update_lead(lead_id, {"next_call_at": when_utc.isoformat()})

def inc_attempts_and_reschedule(lead, max_attempts: int, after_minutes=30):
    attempts = int(lead.get("call_attempts") or 0) + 1
    if attempts >= max_attempts:
        update_lead(lead.get("id"), {
            "call_attempts": attempts,
            "last_call_status": "max-retries",
            "next_call_at": None,
        })
        log_call_to_supabase(lead.get("id"), "max-retries", f"Reached {max_attempts} attempts")
        return
    next_time = datetime.utcnow() + timedelta(minutes=after_minutes)
    update_lead(lead.get("id"), {
        "call_attempts": attempts,
        "last_call_status": "no-answer",
        "next_call_at": next_time.isoformat(),
    })
    log_call_to_supabase(lead.get("id"), "scheduled-retry", f"Retry at {next_time.isoformat()} UTC")

# ===================================================
# Provider call (generic wrapper; response parsed best-effort)
# ===================================================
def make_vapi_call(phone, lead):
    """
    Start a Vapi phone call and inject the campaign script at call time.
    """
    url = "https://api.vapi.ai/call/phone"

    campaign_id = lead.get("campaign_id")

    # Load per-campaign caller config (your helper returns safe defaults)
    try:
        caller_cfg = get_campaign_caller_config(campaign_id)
    except Exception:
        caller_cfg = {}

    # Choose assistant: campaign-specific id if present, else env default
    assistant_id = (caller_cfg.get("vapi_assistant_id") or VAPI_ASSISTANT_ID) or ""
    if not assistant_id:
        print("[VAPI][ERROR] No assistantId set (env VAPI_ASSISTANT_ID or delivery_rules.caller.vapi_assistant_id).")
    if not VAPI_PHONE_NUMBER_ID:
        print("[VAPI][ERROR] No phoneNumberId set (env VAPI_PHONE_NUMBER_ID).")

    print(f"[VAPI] Using assistantId={assistant_id} campaign_id={campaign_id}")

    # Build the instructions from campaign config
    instructions = build_vapi_instructions_from_config(caller_cfg) or ""

    # >>> ADD/REPLACE THIS BLOCK <<<
    assistant_overrides = {
        "model": {
            # Tell Vapi which LLM this override targets
            "provider": os.getenv("VAPI_MODEL_PROVIDER", "openai"),
            "model":    os.getenv("VAPI_MODEL_NAME", "gpt-4o-mini"),
            # Inject your campaign prompt as a system message
            "messages": [
                {"role": "system", "content": instructions}
            ],
        },
        "variableValues": {
            "lead_id": lead.get("id"),
            "lead_name": (lead.get("first_name") or lead.get("name") or ""),
            "company": (lead.get("company_name") or lead.get("company") or ""),
            "job_title": (lead.get("job_title") or ""),
            "campaign_id": campaign_id,
        }
    }

    payload = {
        "assistantId": assistant_id,
        "phoneNumberId": VAPI_PHONE_NUMBER_ID,
        "customer": {"number": phone},
        "assistantOverrides": assistant_overrides,    # <-- inject campaign script here
        "metadata": {                                 # keep for your webhook + logs
            "lead_id": lead.get("id"),
            "campaign_id": campaign_id,
            "leadId": lead.get("id"),       # camelCase mirrors (if needed in Vapi templates)
            "campaignId": campaign_id,
            "lead_name": (lead.get("first_name") or lead.get("name") or ""),
            "leadName": (lead.get("first_name") or lead.get("name") or ""),
            "company": (lead.get("company_name") or lead.get("company") or ""),
            "companyName": (lead.get("company_name") or lead.get("company") or ""),
            "job_title": (lead.get("job_title") or ""),
            "jobTitle": (lead.get("job_title") or ""),
        },
    }

    headers = {"Authorization": f"Bearer {VAPI_API_KEY}", "Content-Type": "application/json"}
    resp = requests.post(url, json=payload, headers=headers)

    who = (lead.get("first_name") or lead.get("name") or "Lead")
    print(f"[VAPI] POST {url} -> {resp.status_code} for {phone} ({who})")
    if resp.status_code >= 400:
        print("[VAPI][ERROR] Response text:", (resp.text or "")[:2000])

    external_call_id = None
    try:
        j = resp.json()
        external_call_id = j.get("id") or (j.get("call") or {}).get("id")
    except Exception:
        pass

    return resp.status_code, (resp.text or "")

def call_lead_if_possible(lead):
    lead_id = lead.get("id")
    campaign_id = lead.get("campaign_id")
    rules = get_campaign_rules(campaign_id)

        # ---- CREDIT GATE (shared by email domain) ----
    if not ensure_credit_before_call(
        supabase=supabase,
        lead=lead,
        min_reserve_cents=MIN_RESERVE_CENTS,
        log_call_cb=log_call_to_supabase,
        update_lead_cb=update_lead
    ):
        return
    # ----------------------------------------------

    if not rules.get("send_calls", True):
        print(f"[CALL] Skipped (calls disabled by campaign). lead_id={lead_id}")
        log_call_to_supabase(lead_id, "skipped", "Calls disabled by campaign rules")
        return

    phone = get_valid_phone(lead)
    if not phone:
        print(f"[CALL] No phone for lead_id={lead_id}")
        log_call_to_supabase(lead_id, "no-phone", "No valid phone on lead")
        return

    if not in_call_window_now(phone, rules["call_window_start"], rules["call_window_end"]):
        nxt = next_window_start(phone, rules["call_window_start"], rules["call_window_end"])
        if nxt:
            schedule_next_call(lead_id, nxt)
            print(f"[CALL] Out of window; scheduled next={nxt.isoformat()} lead_id={lead_id}")
            log_call_to_supabase(lead_id, "scheduled", f"Out of window. Next: {nxt.isoformat()} UTC")
        else:
            print(f"[CALL] No timezone derived for {phone}; lead_id={lead_id}")
            log_call_to_supabase(lead_id, "no-tz", "Could not determine timezone")
        return

    attempt_num = int(lead.get("call_attempts") or 0) + 1
    status_code, resp_text = make_vapi_call(phone, lead)

    external_call_id = None
    try:
        j = json.loads(resp_text)
        external_call_id = j.get("id") or (j.get("call") or {}).get("id")
    except Exception:
        pass

    log_call_enqueued_structured(lead_id, attempt_num, external_call_id)
    log_call_to_supabase(lead_id, "queued" if status_code in (200, 201, 202) else f"http-{status_code}")

    update_lead(lead_id, {
        "status": "sent_for_contact",
        "last_call_status": "queued",
        "sent_for_contact_at": datetime.utcnow().isoformat(),
        "next_call_at": None
    })

# ===================================================
# Email helpers (SMTP + Gmail API + templates)
# ===================================================
EMAIL_REGEX = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def get_lead_email(lead) -> Optional[str]:
    return lead.get("email_address") or lead.get("email")

def can_send_more_today() -> bool:
    try:
        start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0).isoformat()
        q = (supabase.table("email_logs")
             .select("id", count="exact")
             .gte("created_at", start)
             .eq("status", "sent")
             .in_("provider", ["gmail_api", "smtp"]))
        res = q.execute()
        sent = getattr(res, "count", None) or 0
        return sent < MAX_EMAILS_PER_DAY
    except Exception as e:
        print("Email throttle check failed:", e)
        return True  # fail-open

def fetch_email_template(template_id: Optional[str], campaign_id: Optional[str] = None) -> Tuple[str, str]:
    """
    Resolution order:
    1) Campaign-configured subject/body (campaigns table)
    2) Explicit template_id from email_templates
    3) Latest active template from email_templates
    4) Hardcoded default
    """
    # 1) Try campaign-configured fields first
    try:
        if campaign_id:
            cr = supabase.table("campaigns").select("*").eq("id", campaign_id).single().execute()
            c = getattr(cr, "data", None) or {}
            # Try a few likely shapes/keys to be robust with UI storage
            subject = (
                c.get("subject_line")
                or (c.get("email_config") or {}).get("subject_line")
                or (c.get("messaging") or {}).get("subject_line")
                or ""
            )
            body = (
                c.get("email_body")
                or (c.get("email_config") or {}).get("email_body")
                or (c.get("messaging") or {}).get("email_body")
                or ""
            )
            if (subject or body):  # if either is present, use both (empty -> "")
                return subject or "", body or ""
    except Exception as e:
        print("[Template] Campaign lookup failed:", e)

    # 2) If a specific template_id was supplied, use it
    try:
        if template_id:
            r = supabase.table("email_templates").select("*").eq("id", template_id).single().execute()
            t = getattr(r, "data", None)
            if t:
                return (t.get("subject") or ""), (t.get("body") or "")
    except Exception as e:
        print("[Template] template_id fetch failed:", e)

    # 3) Otherwise use the latest active template
    try:
        r = (supabase.table("email_templates")
             .select("*")
             .eq("is_active", True)
             .order("updated_at", desc=True)
             .limit(1)
             .execute())
        rows = getattr(r, "data", []) or []
        if rows:
            t = rows[0]
            return (t.get("subject") or ""), (t.get("body") or "")
    except Exception as e:
        print("[Template] latest active fetch failed:", e)

    # 4) Last-resort default
    subject = "{first_name}, quick intro from PSN"
    body = """Hi {first_name},

I'm Scott from Premier Sports Network (PSN). We help leaders at {company} with curated introductions, private events, and bespoke support across sport & business.

If you're open, I'd love to share a 60-second overview tailored to your role.

Best,
Scott
Premier Sports Network
"""
    return subject, body

def render_template(tpl: str, lead: dict) -> str:
    values = {
        "first_name": (lead.get("first_name") or lead.get("name") or "there").strip(),
        "last_name": (lead.get("last_name") or "").strip(),
        "company": (lead.get("company_name") or lead.get("company") or "your organisation").strip(),
        "job_title": (lead.get("job_title") or "").strip(),
        "email": (lead.get("email_address") or lead.get("email") or "").strip(),
        "city": (lead.get("city_name") or "").strip(),
        "state": (lead.get("state_name") or "").strip(),
        "country": (lead.get("country_name") or "").strip(),
    }
    out = tpl
    for k, v in values.items():
        out = out.replace("{" + k + "}", v)
    return out

# ==============================================
# Outbox enqueue (single source of truth)
# ==============================================
def _enqueue_outbox(
    lead: dict,
    campaign_id: str,
    step_number: int,
    template_id: Optional[str],
    send_after_dt: Optional[datetime] = None,
) -> dict:
    """
    Create (or no-op via unique idem_key) an email_outbox row for this logical step.
    idem_key shape: "<lead_id>:step:<campaign_id>:<step_number>"
    """
    lead_id  = lead.get("id")
    to_email = get_lead_email(lead) or ""
    if not to_email or not EMAIL_REGEX.match(to_email):
        print(f"[OUTBOX] skip (invalid email) lead_id={lead_id}")
        return {"queued": False, "reason": "invalid_email"}

    idem_key = f"{lead_id}:step:{campaign_id}:{step_number}"

    # Render immutable snapshot now
    subj_tpl, body_tpl = fetch_email_template(template_id, campaign_id=campaign_id)
    subject = render_template(subj_tpl, lead)
    body    = render_template(body_tpl, lead)

    send_after = (send_after_dt or datetime.utcnow()).isoformat()

    try:
        res = (supabase.table("email_outbox")
               .upsert({
                   "idem_key":    idem_key,
                   "lead_id":     lead_id,
                   "campaign_id": campaign_id,
                   "step_number": step_number,
                   "template_id": template_id,
                   "to_email":    to_email,
                   "subject":     subject,
                   "body":        body,
                   "send_after":  send_after,
                   "provider":    "gmail_api",
                   "status":      "queued",
               }, on_conflict="idem_key")
               .execute())
        print(f"[OUTBOX] queued idem_key={idem_key} send_after={send_after}")
        return {"queued": True, "idem_key": idem_key}
    except Exception as e:
        msg = str(e)
        if "duplicate key value violates unique constraint" in msg or "uq_email_outbox_idem_key" in msg:
            print(f"[OUTBOX] already queued idem_key={idem_key}")
            return {"queued": True, "idem_key": idem_key, "note": "already_queued"}
        print("[OUTBOX] upsert error:", e)
        return {"queued": False, "reason": "upsert_error", "error": msg}

def _split_email_address(addr: str) -> Tuple[str, str]:
    try:
        local, domain = addr.split("@", 1)
        return local, domain
    except Exception:
        return "", ""

def send_email_via_gmail(to_email: str, subject: str, body: str, reply_to: Optional[str] = None):
    msg = EmailMessage()
    msg["From"] = f"{EMAIL_FROM_NAME} <{EMAIL_FROM}>"
    msg["To"] = to_email
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.ehlo()
        server.starttls()
        server.login(EMAIL_FROM, EMAIL_APP_PASSWORD)
        server.send_message(msg)

def _gmail_service(creds: "GCredentials"):
    return g_build("gmail", "v1", credentials=creds, cache_discovery=False)

def _gmail_users_service(creds: "GCredentials"):
    return g_build("gmail", "v1", credentials=creds, cache_discovery=False)

def _gmail_list_messages(svc, q: str, label_ids=None, max_results=50):
    kwargs = {"userId": "me", "q": q, "maxResults": max_results}
    if label_ids:
        kwargs["labelIds"] = label_ids
    return (svc.users().messages().list(**kwargs).execute() or {}).get("messages", []) or []

def _gmail_get_message(svc, msg_id: str):
    return svc.users().messages().get(userId="me", id=msg_id, format="metadata", metadataHeaders=[
        "To", "Delivered-To", "From", "Subject", "Date"
    ]).execute()

def _hdr(headers, name):
    for h in headers or []:
        if h.get("name","").lower() == name.lower():
            return h.get("value","")
    return ""

def _seen_gmail_message(msg_id: str) -> bool:
    try:
        r = (supabase.table("email_logs")
             .select("id").eq("idem_key", f"gmail:{msg_id}").limit(1).execute())
        return bool((getattr(r, "data", []) or []))
    except Exception:
        return False

def _build_raw_email(from_addr: str, to_addr: str, subject: str, body: str, reply_to: Optional[str] = None) -> str:
    msg = EmailMessage()
    msg["From"] = from_addr
    msg["To"] = to_addr
    msg["Subject"] = subject
    if reply_to:
        msg["Reply-To"] = reply_to
    msg.set_content(body)
    raw_bytes = base64.urlsafe_b64encode(msg.as_bytes())
    return raw_bytes.decode("utf-8")

def send_email_via_gmail_api(user_id: str, to_email: str, subject: str, body: str, reply_to: Optional[str] = None):
    """
    Sends email as the connected Google account for this user_id.
    Falls back to SMTP at a higher level if not connected.
    """
    creds = _get_authed_creds(user_id)  # raises 401 if not connected
    svc = _gmail_service(creds)
    from_addr = EMAIL_FROM  # label; Gmail will set actual From to user's account
    raw = _build_raw_email(from_addr, to_email, subject, body, reply_to)
    svc.users().messages().send(userId="me", body={"raw": raw}).execute()

def log_email_to_supabase(
    lead_id: Optional[str],
    to_email: str,
    status: str,
    error_msg: str = "",
    subject: str = "",
    body: str = "",
    provider: str = "gmail",
    idem_key: Optional[str] = None,
    notes: str = "",
):
    """
    Persist an email attempt. Our table only allows status in ('sent','failed').
    We normalize everything else to one of those and store details in error/notes.
    """
    normalized = "sent" if str(status).lower() == "sent" else "failed"
    try:
        payload = {
            "lead_id": lead_id,
            "to_email": to_email,
            "status": normalized,
            "provider": provider,
            "subject": subject or "",
            "body": body or "",
            "error": (error_msg or "")[:1000],
            "notes": (notes or "")[:1000],
        }
        if idem_key:
            payload["idem_key"] = idem_key
        supabase.table("email_logs").insert(payload).execute()
    except Exception as e:
        print("Email log insert failed:", e)

def send_email_if_possible(
    lead: dict,
    template_id: Optional[str] = None,
    user_id: Optional[str] = None,
    idem_key: Optional[str] = None
) -> dict:
    """
    Returns: {"sent": bool, "skipped": bool, "reason": str, "provider": str|None}
    NOTE: This function is now the single source of truth for when we stamp `emailed_at`.
          Only when `sent == True` do we update the lead with an emailed_at timestamp.
    """
    if not EMAIL_SENDING_ENABLED:
        print("[EMAIL] SENDING DISABLED via env flag")
        return {"sent": False, "skipped": True, "reason": "SENDING_DISABLED", "provider": None}

    campaign_id = lead.get("campaign_id")
    rules = get_campaign_rules(campaign_id)
    if not rules.get("send_email", True):
        print(f"[EMAIL] Skipped (emails disabled by campaign). lead_id={lead.get('id')}")
        log_email_to_supabase(
            lead.get("id"),
            lead.get("email_address") or lead.get("email") or "",
            "failed",
            "Emails disabled by campaign rules",
            subject="",
            body="",
            provider="gmail",
            idem_key=idem_key,
            notes="skipped_by_rules",
        )
        return {"sent": False, "skipped": True, "reason": "DISABLED_BY_RULES", "provider": None}

    to_email = get_lead_email(lead)
    if not to_email or not EMAIL_REGEX.match(to_email):
        print(f"[EMAIL] No/invalid email for lead_id={lead.get('id')}")
        log_email_to_supabase(
            lead.get("id"),
            to_email or "",
            "failed",
            "No/invalid email",
            subject="",
            body="",
            provider="gmail",
            idem_key=idem_key,
            notes="invalid_to",
        )
        return {"sent": False, "skipped": True, "reason": "INVALID_TO", "provider": None}

    # DB-based idempotency check: if this exact step was already sent, skip.
    try:
        if idem_key:
            chk = (supabase.table("email_logs")
                    .select("id")
                    .eq("idem_key", idem_key)
                    .limit(1)
                    .execute())
            if (getattr(chk, "data", []) or []):
                print(f"[EMAIL] Skip duplicate by idem_key={idem_key}")
                return {"sent": False, "skipped": True, "reason": "DUPLICATE", "provider": None}
    except Exception as e:
        print("[EMAIL] idem check failed (proceeding):", e)

    if not can_send_more_today():
        print("[EMAIL] Throttled by daily cap")
        log_email_to_supabase(
            lead.get("id"), to_email, "failed",
            "Daily soft cap reached",
            subject="",
            body="",
            provider="gmail",
            idem_key=idem_key,
            notes="throttled",
        )
        return {"sent": False, "skipped": True, "reason": "THROTTLED", "provider": None}

    campaign_id = lead.get("campaign_id")
    subj_tpl, body_tpl = fetch_email_template(template_id, campaign_id=campaign_id)
    subject = render_template(subj_tpl, lead)
    body    = render_template(body_tpl, lead)
# ---- pre-send reservation to prevent double sends (with lock token) ----
    lock_token = str(uuid4())
    if idem_key:
        try:
            # Try to create (or update) the reservation and store OUR lock_token
            supabase.table("email_logs").upsert({
                "lead_id": lead.get("id"),
                "to_email": to_email,
                "status": "sending",            # reservation
                "provider": "lock",
                "subject": subject,
                "body": "",
                "error": "LOCK",
                "notes": "pre-send reservation",
                "idem_key": idem_key,
                "lock_token": lock_token,       # <-- NEW: who owns this reservation
            }, on_conflict="idem_key").execute()

            # Read back the row; only proceed if WE own the reservation
            row = (supabase.table("email_logs")
                .select("lock_token,status")
                .eq("idem_key", idem_key)
                .single()
                .execute()).data or {}

            if not row or row.get("lock_token") != lock_token:
                print(f"[EMAIL] Another worker owns reservation for {idem_key}; skipping.")
                return {"sent": False, "skipped": True, "reason": "DUPLICATE_RESERVED", "provider": None}
        except Exception as e:
            print(f"[EMAIL] Reservation step failed: {e}")
            return {"sent": False, "skipped": True, "reason": "DUPLICATE_RESERVED", "provider": None}
# ------------------------------------------------------

    # Plus addressing to track replies
    local, domain = _split_email_address(EMAIL_FROM)
    reply_to_tagged = f"{local}+{lead.get('id')}@{domain}" if local and domain and lead.get("id") else None

# Try Gmail API first when we have a user_id
    if user_id:
        try:
            # --- EXTRA GUARD: if this step is already marked sent, skip entirely ---
            if idem_key:
                already = (
                    supabase.table("email_logs")
                    .select("id")
                    .eq("idem_key", idem_key)
                    .eq("status", "sent")
                    .limit(1)
                    .execute()
                )
                if (getattr(already, "data", None) or []):
                    print(f"[EMAIL] Skipping Gmail send; idem_key already sent: {idem_key}")
                    return {"sent": False, "skipped": True, "reason": "ALREADY_SENT", "provider": None}

            # Proceed with actual send
            send_email_via_gmail_api(user_id, to_email, subject, body, reply_to=reply_to_tagged)

            if idem_key:
                # Finalize only if WE own the reservation
                upd = (
                    supabase.table("email_logs").update({
                        "status": "sent",
                        "provider": "gmail_api",
                        "subject": subject,
                        "body": body,
                        "error": "",
                        "notes": "ok"
                    })
                    .eq("idem_key", idem_key)
                    .eq("lock_token", lock_token)   # <-- ownership check
                    .execute()
                )
                # If 0 rows updated, another worker finalized first—treat as duplicate and bail
                if not (getattr(upd, "data", None) or []):
                    print(f"[EMAIL] Lost finalize race for {idem_key}; another worker owns it.")
                    return {"sent": False, "skipped": True, "reason": "FINALIZE_LOST", "provider": None}
            else:
                # No idem_key path (unlikely for scheduled steps)
                log_email_to_supabase(
                    lead.get("id"), to_email, "sent", "",
                    subject=subject, body=body,
                    provider="gmail_api", idem_key=None, notes="ok"
                )

            # Only stamp after successful finalize
            update_lead(lead.get("id"), {"emailed_at": datetime.utcnow().isoformat(), "last_email_status": "sent"})
            print(f"[EMAIL] Sent to {to_email} via gmail_api (Reply-To: {reply_to_tagged})")
            return {"sent": True, "skipped": False, "reason": "", "provider": "gmail_api"}

        except Exception as gmail_err:
            print("[EMAIL] Gmail path failed or not connected:", gmail_err)

            if idem_key:
                # Mark failed only if WE own the reservation
                upd = (
                    supabase.table("email_logs").update({
                        "status": "failed",
                        "provider": "gmail_api",
                        "subject": subject,
                        "body": body,
                        "error": "GMAIL_SEND_FAILED_OR_NOT_CONNECTED",
                        "notes": "no_fallback"
                    })
                    .eq("idem_key", idem_key)
                    .eq("lock_token", lock_token)   # <-- ownership check
                    .execute()
                )
                if not (getattr(upd, "data", None) or []):
                    print(f"[EMAIL] Lost finalize race (failed) for {idem_key}; another worker owns it.")
                    return {"sent": False, "skipped": True, "reason": "FINALIZE_LOST", "provider": None}
            else:
                log_email_to_supabase(
                    lead.get("id"), to_email, "failed",
                    "GMAIL_SEND_FAILED_OR_NOT_CONNECTED",
                    subject=subject, body=body,
                    provider="gmail_api", idem_key=None, notes="no_fallback"
                )

            update_lead(lead.get("id"), {"last_email_status": "failed"})
            return {"sent": False, "skipped": True, "reason": "GMAIL_FAILED", "provider": None}
# ===================================================
# Background scheduler (calls)
# ===================================================
scheduler = BackgroundScheduler(timezone="UTC")

def poll_due_calls():
    try:
        now_iso = datetime.utcnow().isoformat()
        resp = (supabase.table("leads").select("*")
                .or_("status.eq.accepted,status.eq.sent_for_contact")
                .lte("next_call_at", now_iso).execute())
        leads = resp.data or []
        if not leads:
            return
        print(f"[Scheduler] Due leads: {len(leads)}")
        for lead in leads:
            update_lead(lead.get("id"), {"next_call_at": None})
            call_lead_if_possible(lead)
    except Exception as e:
        print("[Scheduler] Error:", e)

# ===================================================
# Email Sequence Scheduler (follow-up steps)
# ===================================================
def _due_email_steps(now_utc_iso: str, limit: int = 100):
    """
    Returns a list of (lead, step) that are due to send now.
    IMPORTANT: The scheduler ONLY handles FOLLOW-UPS (step >= 2).
               Initial email (step 0/1) is sent immediately at accept time.
    """
    steps_resp = supabase.table("campaign_email_steps").select(
        "*, campaign:campaigns(delivery_rules)"
    ).eq("is_active", True).limit(limit).execute()
    steps = steps_resp.data or []

    due = []
    for st in steps:
        # Campaign-level email kill switch
        dr = (st.get("campaign") or {}).get("delivery_rules") or {}
        if not dr.get("use_email", True):
            continue

        # Scheduler handles FOLLOW-UPS only
        step_no_raw = st.get("step_number")
        try:
            step_no = int(step_no_raw) if step_no_raw is not None else 0
        except Exception:
            step_no = 0
        if step_no in (0, 1):
            # Skip initial step(s); accept endpoint sends those
            continue

        send_at    = st.get("send_at")
        offset_min = st.get("send_offset_minutes")

        # Pull candidate leads for this campaign that are still eligible
        leads_q = (
            supabase.table("leads")
            .select("*")
            .eq("campaign_id", st["campaign_id"])
            .neq("last_email_status", "reply")       # skip anyone who replied
            .neq("email_sequence_stopped", True)     # skip sequences we stopped
            .limit(200)
        )
        leads = leads_q.execute().data or []

        for ld in leads:
            # Case A: absolute time
            if send_at and send_at <= now_utc_iso:
                due.append((ld, st))
                continue

            # Case B: offset from the initial email (requires emailed_at to exist)
            if offset_min is not None:
                emailed_at = ld.get("emailed_at")
                if not emailed_at:
                    continue
                try:
                    base = datetime.fromisoformat(emailed_at.replace("Z", "+00:00"))
                    now  = datetime.fromisoformat(now_utc_iso.replace("Z", "+00:00"))
                    if base + timedelta(minutes=int(offset_min)) <= now:
                        due.append((ld, st))
                except Exception:
                    continue

    return due

def poll_due_email_steps():
    """Check for scheduled email follow-ups that are now due."""
    # Hard stop: if email sending is off, don't even scan/query or print noise
    if not EMAIL_SENDING_ENABLED:
        return

    try:
        now_iso = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
        items = _due_email_steps(now_iso, limit=200)
        if not items:
            return
        print(f"[EmailSeq] Due items: {len(items)}")

        for lead, step in items:
            # Skip if already replied
            if (lead.get("last_email_status") or "").lower() == "reply":
                continue
            # Hard skip: scheduler only sends FOLLOW-UPS (step >= 2).
            try:
                step_no = int(step.get("step_number") or 0)
            except Exception:
                step_no = 0
            if step_no < 2:
                continue

            step_id = step.get("id") or f"{step.get('campaign_id')}:{step_no}"
            idem = f"{lead.get('id')}:step:{step_id}"

            tpl_id = step.get("template_id")
            user_id = lead.get("user_id") or DEFAULT_USER_ID  # kept if your worker wants it later

            # Enqueue to the Outbox; the Outbox worker will actually send.
            try:
                _enqueue_outbox(
                    lead=lead,
                    campaign_id=lead.get("campaign_id"),
                    step_number=step_no,
                    template_id=tpl_id,
                    send_after_dt=None  # it's due now
                )
            except Exception as e:
                print("[EmailSeq] enqueue to outbox failed:", e)

    except Exception as e:
        print("[EmailSeq] Error:", e)

def norm_ws(s): 
    return re.sub(r"\s+", " ", s.strip())

def pick_one_with_priority(candidates, priority_order):
    if not candidates: 
        return None
    for p in priority_order:
        if p in candidates:
            return p
    return candidates[0]

def tokenize_words(s):
    # Lowercase tokens with word boundaries; keeps only alphabetic words of length >= 2
    return re.findall(r"\b[a-z][a-z]+\b", s.lower())

def find_titles(s):
    low = s.lower()
    hits = []
    for t in TITLES:
        if t.lower() in low:
            hits.append(t)
    # brute force role words as titles
    for kw in ["ceo","president","partner","founder","owner"]:
        if re.search(rf"\b{kw}\b", low):
            if kw == "partner":
                hits.append("Partner")
            elif kw != "ceo":
                hits.append(kw.title())
            else:
                hits.append("CEO")
    return sorted(set(hits))

def find_seniority(s):
    low = s.lower()
    hits = set()
    for pat, value in SENIORITY_MAP.items():
        if re.search(pat, low):
            hits.add(value)
    # title-derived hints
    if re.search(r"\bceo|cto|cfo|coo\b", low): hits.add("C-Suite")
    if re.search(r"\bvp\b|\bvice president\b", low): hits.add("VP")
    if re.search(r"\bhead\b", low): hits.add("Head")
    if re.search(r"\bdirector\b", low): hits.add("Director")
    if re.search(r"\bmanager\b", low): hits.add("Manager")
    if re.search(r"\bpartner\b", low): hits.add("Partner")
    if re.search(r"\bfounder\b", low): hits.add("Founder")
    return sorted(hits)

def find_functions(s):
    toks = set(tokenize_words(s))
    hits = set()
    for kw, fn in FUNCTIONAL_BY_KEYWORD.items():
        if kw in toks:
            hits.add(fn)
    return sorted(hits)

def find_industries_and_keywords(s):
    low = s.lower()
    toks = set(tokenize_words(s))  # avoids matching 'ai' from 'emails'
    inds, company_keywords = set(), set()
    # phrase-first matches (multiword)
    for phrase, mapped in INDUSTRY_BY_KEYWORD.items():
        if " " in phrase:
            if phrase in low:
                inds.add(mapped)
                company_keywords.add(phrase)
    # single-token matches
    for phrase, mapped in INDUSTRY_BY_KEYWORD.items():
        if " " not in phrase and phrase in toks:
            inds.add(mapped)
            company_keywords.add(phrase)
    # quoted phrases as keywords
    for q in re.findall(r'"([^"]+)"', s):
        if 1 <= len(q.split()) <= 5:
            company_keywords.add(q.lower())
    # explicit phrase support
    if "wealth management" in low:
        company_keywords.add("wealth management")
    return sorted(inds), sorted(company_keywords)

def find_geo(s):
    low = s.lower()
    countries, states, cities = [], [], []

    # Countries (aliases)
    for alias, proper in COUNTRY_ALIASES.items():
        if re.search(rf"\b{re.escape(alias)}\b", low):
            countries.append(proper)

    # Cities (stop before conjunctions or punctuation)
    STOP = r"[,.]|(?=\s+(with|and|or|for|of|that|who|which|emails?|phones?)\b)"
    for m in re.finditer(r"(?:\bin\b|\bat\b|\bbased in\b)\s+([a-zA-Z][a-zA-Z\s\-]{1,40})", low):
        chunk = norm_ws(m.group(1))
        cut = re.split(STOP, chunk)[0].strip()
        token = norm_ws(cut).title()
        if token and token.lower() not in COUNTRY_ALIASES and token.lower() not in ("usa","united states"):
            cities.append(token)

    # State extraction (extend as needed)
    for st in ["california","texas","new york","florida","illinois","washington","massachusetts","georgia","ohio","pennsylvania","arizona","colorado"]:
        if re.search(rf"\b{st}\b", low):
            states.append(st.title())

    return sorted(set(countries)), sorted(set(states)), sorted(set(cities))

def find_size(s):
    m = SIZE_PAT.search(s)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        lo, hi = min(a, b), max(a, b)
        return [f"{lo} - {hi}"]  # spaces around dash as per actor example
    if "startup" in s.lower():
        return ["1 - 200"]
    return []

def detect_flags(s):
    low = s.lower()
    has_email = any(w in low for w in ["with email","with emails","has email","emails"])
    has_phone = any(w in low for w in ["with phone","with phones","has phone","phones"])
    verified = "verified email" in low or "verified emails" in low or "only verified" in low
    return bool(has_email), bool(has_phone), bool(verified)

def find_company_domains(s):
    # capture simple domain tokens like example.com
    domains = re.findall(r"\b([a-z0-9][a-z0-9\-]+\.[a-z]{2,})\b", s.lower())
    bad = {"usa.com","email.com","gmail.com","yahoo.com"}
    return sorted({d for d in domains if d not in bad})

def nl_to_actor_input(prompt: str, total_results: int):
    s = prompt.strip()

    titles        = find_titles(s)
    seniority_l   = find_seniority(s)
    functional_l  = find_functions(s)
    industries_l, company_keywords = find_industries_and_keywords(s)
    countries, states, cities = find_geo(s)
    sizes        = find_size(s)
    has_email, has_phone, verified_only = detect_flags(s)
    company_domains = find_company_domains(s)

    def cap1(arr):
        if not arr: return arr
        return [arr[0]]

    seniority_one = pick_one_with_priority(seniority_l, SENIORITY_PRIORITY)
    seniority_arr = [seniority_one] if seniority_one else []

    functional_arr = cap1(functional_l)
    industry_arr   = cap1(industries_l)

    payload = {
        "totalResults": total_results,
        "personTitle": titles or None,
        "seniority": (seniority_arr or None),
        "functional": (functional_arr or None),
        "companyIndustry": (industry_arr or None),
        "companyKeyword": (company_keywords or None),
        "companyEmployeeSize": (sizes or None),
        "personCountry": (countries or None),
        "personState": (states or None),
        "personCity": (cities or None),
        "companyCountry": (countries or None),
        "companyState": (states or None),
        "companyCity": (cities or None),
        "companyDomain": (company_domains or None),
        "hasEmail": has_email,
        "hasPhone": has_phone,
        "contactEmailStatus": (["Verified"] if verified_only else None),
    }
    # remove empties
    return {k: v for k, v in payload.items() if v not in (None, [], "")}

# REST: scrape (NL → Apify actor via working translator)
# ===================================================
@app.post("/api/scrape-leads")
@app.post("/api/scrape-leads-nl")  # alias for backward compatibility
async def scrape_leads_nl(request: Request):
    """
    Natural-language scraping via Apify actor (VYRyEF4ygTTkaIghe).
    Body:
      { "prompt": "Finance CEO based in USA with emails", "count": 1000 }
    Returns:
      The actor dataset items (list).
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "JSON body required"}, status_code=400)

    prompt = (body.get("prompt") or body.get("q") or "").strip()
    count  = int(body.get("count") or 1000)
    if not prompt:
        return JSONResponse({"ok": False, "error": "Missing 'prompt'"}, status_code=400)

    try:
        items = run_apify(prompt, count)  # uses the working translator in scrape_runner.py
        return items
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Apify error: {e}"}, status_code=502)

@app.post("/api/accepted-leads")
async def accept_and_call_leads(request: Request, background_tasks: BackgroundTasks):
    """
    Accepts:
      - a single lead object
      - a list of leads
      - or an object: { "leads": [...], "emailTemplateId": "uuid-optional", "campaignId": "..." }
    Stamps campaign_id onto every lead and runs call/email flows.
    """
    # Read JSON first so we can accept user_id from the body too
    body = await request.json()

    # Accept user_id from header, query, OR body (user_id / userId)
    user_id = (
        _get_request_user_id(request)
        or (body.get("user_id") if isinstance(body, dict) else None)
        or (body.get("userId") if isinstance(body, dict) else None)
    )
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"}, status_code=400)

    try:
        # One-time raw log to prove what the UI actually sent
        print(f"[ACCEPT][RAW BODY] {json.dumps(body)[:2000]}")
    except Exception:
        print("[ACCEPT][RAW BODY] (non-serializable)")

    email_template_id = None
    leads = []
    body_type = "unknown"

    # ---- Resolve container and optional template ----
    if isinstance(body, dict) and "leads" in body:
        leads = body.get("leads") or []
        email_template_id = body.get("emailTemplateId")
        body_type = "dict"
    elif isinstance(body, list):
        leads = body
        body_type = "list"
    elif isinstance(body, dict):
        leads = [body]
        body_type = "single-dict"
    else:
        return JSONResponse({"ok": False, "error": "Invalid body"}, status_code=400)

    # ---- Resolve top-level campaign (optional – per-lead overrides still supported) ----
    req_campaign_id = (
        (body.get("campaignId") if isinstance(body, dict) else None)
        or (body.get("campaign_id") if isinstance(body, dict) else None)
        or ""
    )
    req_campaign_id = (req_campaign_id or "").strip() or None

    # If no top-level id, allow per-lead BUT require at least one present to avoid silent reuse
    if not req_campaign_id:
        has_any = any(
            (isinstance(l, dict) and ((l.get("campaignId") or l.get("campaign_id") or "").strip()))
            for l in leads
        )
        if not has_any:
            return JSONResponse({"ok": False, "error": "Missing campaignId (top-level or per-lead)"}, status_code=400)

    print(f"[ACCEPT] Received {len(leads)} lead(s). BodyType={body_type}")

    results = []
    for lead in leads:
        if not isinstance(lead, dict):
            continue

        # ----- Canonicalize & stamp campaign onto the lead BEFORE any DB writes -----
        per_lead_campaign = (lead.get("campaignId") or lead.get("campaign_id") or "").strip() or None
        resolved_campaign = per_lead_campaign or req_campaign_id
        lead["campaign_id"] = resolved_campaign  # <- canonical target field

        if not lead["campaign_id"]:
            # Shouldn't happen due to guard above, but double-check
            print("[ACCEPT][WARN] Lead missing campaign_id after resolution; skipping.")
            continue

        # Generate id if needed and bind to user
        if not lead.get("id"):
            lead["id"] = str(uuid4())
            print(f"[ACCEPT] Generated id for lead: {lead['id']}")
        lead["user_id"] = user_id

        # ---- EMAIL NORMALIZATION & VALIDATION (REQUIRED for upsert) ----
        raw_email = (
            (lead.get("email_address") or lead.get("email") or lead.get("Email") or "").strip()
        )
        if raw_email:
            lead["email_address"] = raw_email.lower()
        else:
            print(f"[ACCEPT][EMAIL][ERROR] Lead missing email/email_address; skipping. id={lead.get('id')} name={lead.get('first_name')}")
            continue

        # Optional (reject clearly invalid emails early)
        if not EMAIL_REGEX.match(lead["email_address"]):
            print(f"[ACCEPT][EMAIL][ERROR] Invalid email='{lead['email_address']}' ; skipping lead id={lead.get('id')}")
            continue

        # ---- PHONE NORMALIZATION (kept from your code) ----
        for src in [
            "phone_number", "mobile", "mobile_number", "cell", "work_phone",
            "telephone", "tel", "primary_phone", "contact_number"
        ]:
            v = lead.get(src)
            if isinstance(v, str) and v.strip() and not lead.get("phone"):
                lead["phone"] = v.strip()
        if not lead.get("contact_phone_numbers") and isinstance(lead.get("phone"), str) and lead["phone"].strip():
            lead["contact_phone_numbers"] = [{"rawNumber": lead["phone"].strip()}]
        print("[ACCEPT][PHONE DEBUG]", json.dumps({
            "phone": lead.get("phone"),
            "phone_number": lead.get("phone_number"),
            "mobile": lead.get("mobile"),
            "contact_phone_numbers": lead.get("contact_phone_numbers"),
            "company.phone": (lead.get("company") or {}).get("phone") if isinstance(lead.get("company"), dict) else None
        }, indent=2))

        # Parse a few stringified fields safely
        if isinstance(lead.get("contact_phone_numbers"), str):
            try:
                lead["contact_phone_numbers"] = json.loads(lead["contact_phone_numbers"])
            except Exception:
                lead["contact_phone_numbers"] = []
        if isinstance(lead.get("company"), str):
            try:
                lead["company"] = json.loads(lead["company"])
            except Exception:
                pass

        # Status defaults
        lead["status"] = "accepted"
        lead["accepted_at"] = datetime.utcnow().isoformat()
        lead.setdefault("call_attempts", 0)
        lead.setdefault("last_call_status", None)
        lead.setdefault("next_call_at", None)

        # ---- Upsert (unique on user_id + email_address) ----
        try:
            res = supabase.table("leads").upsert(lead, on_conflict="user_id,email_address").execute()
            saved_rows = getattr(res, "data", []) or []

            if saved_rows:
                saved = saved_rows[0]
                lead["id"] = saved.get("id", lead["id"])
                # if DB returned a different campaign (e.g., existing row), force it to the *new* one
                if (saved.get("campaign_id") or "") != lead["campaign_id"]:
                    update_lead(lead["id"], {"campaign_id": lead["campaign_id"]})
                results.append({**saved, "campaign_id": lead["campaign_id"]})
            else:
                # some PostgREST versions return no data on upsert; fetch explicitly
                fetch = (supabase.table("leads")
                         .select("*")
                         .eq("user_id", user_id)
                         .eq("email_address", lead.get("email_address") or lead.get("email"))
                         .single().execute())
                saved = getattr(fetch, "data", None) or {}
                if saved:
                    lead["id"] = saved.get("id", lead["id"])
                    if (saved.get("campaign_id") or "") != lead["campaign_id"]:
                        update_lead(lead["id"], {"campaign_id": lead["campaign_id"]})
                    results.append({**saved, "campaign_id": lead["campaign_id"]})

            print(f"[ACCEPT] Saved/updated lead: {lead.get('first_name','')} {lead.get('last_name','')} (id: {lead.get('id')})")
            print(f"[ACCEPT] campaign_in_body={per_lead_campaign or req_campaign_id} final_campaign={lead.get('campaign_id')}")
            print(f"[ACCEPT][TRACE] id={lead.get('id')} email={lead.get('email_address')} campaign={lead.get('campaign_id')}")
            # hydrate phone back from DB if we still don't have one (unchanged from your code)
            try:
                db_row_res = (supabase.table("leads")
                              .select("phone,contact_phone_numbers,company")
                              .eq("id", lead["id"]).single().execute())
                db_lead = getattr(db_row_res, "data", None) or {}
                if not lead.get("phone") and isinstance(db_lead.get("phone"), str) and db_lead["phone"].strip():
                    lead["phone"] = db_lead["phone"].strip()
                if not lead.get("contact_phone_numbers") and isinstance(db_lead.get("contact_phone_numbers"), list):
                    lead["contact_phone_numbers"] = db_lead["contact_phone_numbers"]
                if not lead.get("phone") and isinstance(db_lead.get("company"), dict):
                    cph = db_lead["company"].get("phone")
                    if isinstance(cph, str) and cph.strip():
                        lead["phone"] = cph.strip()
                if isinstance(lead.get("phone"), str) and lead["phone"].strip() and not lead.get("contact_phone_numbers"):
                    lead["contact_phone_numbers"] = [{"rawNumber": lead["phone"].strip()}]
                print("[ACCEPT][POST-UPSERT PHONE]", json.dumps({
                    "phone": lead.get("phone"),
                    "contact_phone_numbers": lead.get("contact_phone_numbers"),
                }, indent=2))
            except Exception as e:
                print("[ACCEPT] DB hydrate failed (continuing):", e)

        except Exception as e:
            msg = str(e)
            # Duplicate -> reuse existing row and UPDATE campaign to the new one
            if "23505" in msg or "duplicate key value violates unique constraint" in msg:
                try:
                    existing = (supabase.table("leads")
                                .select("*")
                                .eq("user_id", user_id)
                                .eq("email_address", lead.get("email_address") or lead.get("email"))
                                .single().execute()).data
                    if existing:
                        lead["id"] = existing["id"]
                        # refresh sparse fields
                        patch = {}
                        for k in ["first_name", "last_name", "company_name", "job_title", "location",
                                  "city_name", "state_name", "country_name", "phone", "contact_phone_numbers"]:
                            v = lead.get(k)
                            if v and not existing.get(k):
                                patch[k] = v
                        # always move to the new campaign if different
                        if (existing.get("campaign_id") or "") != lead["campaign_id"]:
                            patch["campaign_id"] = lead["campaign_id"]
                        if patch:
                            update_lead(lead["id"], patch)
                        results.append({**existing, **patch})
                        print(f"[ACCEPT] Reused existing lead (user/email unique). id={lead['id']} -> campaign={lead['campaign_id']}")
                    else:
                        print("[ACCEPT] Duplicate raised but fetch failed; skipping this lead.")
                        continue
                except Exception as e2:
                    print(f"[ACCEPT] Failed to fetch existing lead after 23505: {e2}")
                    continue
            else:
                print(f"[ACCEPT] Supabase upsert failed for lead {lead.get('id')}: {e}")
                continue

        # === Delivery rules & actions ===
        rules = get_campaign_rules(lead.get("campaign_id"))

        if rules.get("send_calls", True):
            phone = get_valid_phone(lead)
            if phone and in_call_window_now(phone, rules["call_window_start"], rules["call_window_end"]):
                print(f"[ACCEPT] Calling now: {phone} lead_id={lead.get('id')}")
                background_tasks.add_task(call_lead_if_possible, lead)
            else:
                nxt = next_window_start(phone, rules["call_window_start"], rules["call_window_end"]) if phone else None
                if nxt:
                    schedule_next_call(lead.get("id"), nxt)
                    log_call_to_supabase(lead.get("id"), "scheduled", f"Out of window at accept. Next: {nxt.isoformat()} UTC")
                    print(f"[ACCEPT] Out of window; scheduled for {nxt.isoformat()} lead_id={lead.get('id')}")
                else:
                    log_call_to_supabase(lead.get("id"), "no-tz", "No timezone at accept")
                    print(f"[ACCEPT] Could not determine timezone for phone={phone} lead_id={lead.get('id')}")

        # Initial email is handled by Lovable when INITIAL_EMAIL_SENDER=lovable
        if rules.get("send_email", True):
            initial_sender = os.getenv("INITIAL_EMAIL_SENDER", "render").strip().lower()
            if initial_sender == "render":
                print(f"[ACCEPT] Queueing INITIAL email for lead_id={lead.get('id')} (sender=render)")
                initial_idem = f"{lead.get('id')}:step:initial"
                tpl_id = email_template_id
                background_tasks.add_task(send_email_if_possible, lead, tpl_id, user_id, initial_idem)
            else:
                print(f"[ACCEPT] Skipping INITIAL email (sender={initial_sender}); Lovable will send.")

    return {"status": "saved_and_scheduled", "num_leads": len(results), "received": len(leads)}

# ---------------------------------------------------
# Test endpoints (handy for Outreach Centre buttons)
# ---------------------------------------------------
@app.post("/api/test-email")
async def test_email(request: Request):
    caller_user_id = _get_request_user_id(request)

    body = await request.json()
    to = (body.get("to") or "").strip()
    tpl = body.get("emailTemplateId")

    if not to or not EMAIL_REGEX.match(to):
        return JSONResponse({"ok": False, "error": "Invalid 'to' email"}, status_code=400)

    fake_lead = {
        "id": None,
        "first_name": "Scott",
        "last_name": "",
        "company_name": "Premier Sports Network",
        "email_address": to,
        "job_title": "",
        "city_name": "",
        "state_name": "",
        "country_name": "",
    }

    subj_tpl, body_tpl = fetch_email_template(tpl)
    subject = render_template(subj_tpl, fake_lead)
    body_txt = render_template(body_tpl, fake_lead)

    try:
        if caller_user_id:
            # Send via Gmail API
            send_email_via_gmail_api(caller_user_id, to, subject, body_txt)
            # LOG AS 'sent' with subject/body, provider='gmail_api'
            log_email_to_supabase(
                lead_id=None,
                to_email=to,
                status="sent",
                error_msg="",
                subject=subject,
                body=body_txt,
                provider="gmail_api",
            )
            return {"ok": True, "sent_to": to, "via": "gmail_api"}
        else:
            # Force SMTP path if no user_id for Gmail
            raise Exception("No user_id for Gmail send")
    except Exception as e:
        print("[TEST EMAIL] Gmail failed or not connected, trying SMTP:", e)

        if not caller_user_id and not ALLOW_SMTP_FALLBACK:
            return JSONResponse({"ok": False, "error": "SMTP_FALLBACK_DISABLED"}, status_code=400)

        try:
            # Send via SMTP fallback
            send_email_via_gmail(to, subject, body_txt)
            # LOG AS 'sent' with subject/body, provider='smtp'
            log_email_to_supabase(
                lead_id=None,
                to_email=to,
                status="sent",
                error_msg="",
                subject=subject,
                body=body_txt,
                provider="smtp",
            )
            return {"ok": True, "sent_to": to, "via": "smtp"}
        except Exception as e2:
            # LOG AS 'failed' with subject/body and error text
            log_email_to_supabase(
                lead_id=None,
                to_email=to,
                status="failed",
                error_msg=str(e2),
                subject=subject,
                body=body_txt,
                provider="smtp",
            )
            return JSONResponse({"ok": False, "error": str(e2)}, status_code=500)

@app.post("/api/test-call")
async def test_call(request: Request):
    body = await request.json()
    number = (body.get("number") or "").strip()
    if not number:
        return JSONResponse({"ok": False, "error": "Missing 'number'"}, status_code=400)

    campaign_id = body.get("campaign_id")  # <- define it safely

    lead = {
        "id": "test-lead-id",
        "first_name": body.get("lead_name") or "Test",
        "last_name": "",
        "company_name": "PSN",
        "job_title": "Test",
        "campaign_id": campaign_id,
    }
    code, text = make_vapi_call(number, lead)
    return {"ok": code in (200, 201, 202), "status": code, "response": text}

from fastapi import Body

@app.post("/vapi/campaign-instructions")
def vapi_campaign_instructions_post(payload: dict = Body(...)):
    # Accept both snake_case and camelCase just in case
    campaign_id = (payload.get("campaign_id") or payload.get("campaignId") or "").strip()
    lead_id = (payload.get("lead_id") or payload.get("leadId") or "").strip() or None
    cfg = get_campaign_caller_config(campaign_id)
    prompt = build_vapi_instructions_from_config(cfg) or ""
    return {"instructions": prompt}

@app.get("/vapi/campaign-instructions")
def vapi_campaign_instructions_get(campaign_id: str, lead_id: Optional[str] = None):
    cfg = get_campaign_caller_config(campaign_id)
    prompt = build_vapi_instructions_from_config(cfg) or ""
    return {"instructions": prompt}

@app.get("/api/dev/gmail-scan")
def dev_gmail_scan(request: Request, days: int = 7):
    """
    Returns the messages the poller would scan for this user, including the parsed lead_id.
    Use:
      curl "http://localhost:8000/api/dev/gmail-scan?days=7" -H "X-User-Id: <AUTH_USER_ID>"
    """
    user_id = _get_request_user_id(request)
    if not user_id:
        return JSONResponse({"ok": False, "error": "Missing user_id"}, status_code=400)

    try:
        creds = _get_authed_creds(user_id)
        svc = _gmail_users_service(creds)
    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Auth error: {e}"}, status_code=401)

    # Build the same query the poller uses
    email_local, email_domain = _split_email_address(EMAIL_FROM)
    reply_domain = (os.getenv("REPLY_TO_DOMAIN") or email_domain)

    # Primary query: capture +tag replies (no quotes so * expands)
    q = f'to:{email_local}+*@{reply_domain} newer_than:{days}d -in:sent -in:chat'
    out = {"ok": True, "user_id": user_id, "query": q, "items": []}

    try:
        # Primary search
        items = _gmail_list_messages(svc, q=q, label_ids=["INBOX"], max_results=50) or []

        # Fallback: some clients strip +tag; look for replies to the bare address
        if not items:
            q2 = f'to:{email_local}@{reply_domain} newer_than:{days}d -in:sent -in:chat'
            out["query_fallback"] = q2
            items = _gmail_list_messages(svc, q=q2, label_ids=["INBOX"], max_results=50) or []

    except Exception as e:
        return JSONResponse({"ok": False, "error": f"Gmail list error: {e}"}, status_code=500)

    out["items"] = items
    return out

@app.post("/api/dev/poll-gmail")
def dev_poll_gmail(request: Request):
    """
    Manually trigger the Gmail reply poller.
    - If you pass X-User-Id header (or ?user_id=...), polls only that user.
    - Otherwise, polls all connected users.
    """
    uid = _get_request_user_id(request)
    if uid:
        poll_gmail_replies_for_user(uid)
        return {"ok": True, "polled": [uid]}
    else:
        poll_all_gmail_replies()
        return {"ok": True, "polled": "all_connected"}

# ===================================================
# Activity feed endpoints (for the dashboard)
# ===================================================
def _parse_since(since_str: Optional[str]) -> Optional[str]:
    if not since_str:
        return None
    try:
        if since_str.endswith("Z"):
            dt = datetime.fromisoformat(since_str.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(since_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = dt.astimezone(timezone.utc)
        return dt.isoformat()
    except Exception:
        return None

@app.get("/api/lead-activity/{lead_id}")
def get_lead_activity(lead_id: str, since: Optional[str] = None):
    try:
        lead_res = supabase.table("leads").select("*").eq("id", lead_id).single().execute()
        lead = getattr(lead_res, "data", None)
    except Exception as e:
        lead = None
        print("Lead fetch failed:", e)

    since_iso = _parse_since(since)

    try:
        q = supabase.table("call_logs").select("*").eq("lead_id", lead_id)
        if since_iso:
            q = q.gte("created_at", since_iso)
        calls_res = q.order("created_at", desc=True).limit(100).execute()
        calls = getattr(calls_res, "data", []) or []
    except Exception as e:
        calls = []
        print("Call logs fetch failed:", e)

    try:
        q = supabase.table("email_logs").select("*").eq("lead_id", lead_id)
        if since_iso:
            q = q.gte("created_at", since_iso)
        emails_res = q.order("created_at", desc=True).limit(100).execute()
        emails = getattr(emails_res, "data", []) or []
    except Exception as e:
        emails = []
        print("Email logs fetch failed:", e)

    return {"lead": lead, "calls": calls, "emails": emails}

@app.get("/api/leads/{lead_id}")
def get_lead(lead_id: str):
    try:
        res = supabase.table("leads").select("*").eq("id", lead_id).single().execute()
        return getattr(res, "data", None) or {}
    except Exception as e:
        print("Lead fetch failed:", e)
        return JSONResponse({"ok": False, "error": "Lead not found"}, status_code=404)


# ===================================================
# Inbound Email Webhook (generic) — logs replies & marks contacted
# ===================================================
PLUS_TAG_RE = re.compile(r"\+([0-9a-fA-F-]{8,})@")

def _extract_emails(value: Optional[str]) -> List[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(v) for v in value]
    return [v.strip() for v in str(value).split(",") if v.strip()]

def parse_lead_id_from_addresses(addresses: List[str]) -> Optional[str]:
    for addr in addresses:
        m = PLUS_TAG_RE.search(addr)
        if m:
            return m.group(1)
    return None

def poll_gmail_replies_for_user(user_id: str, to_domain_override: Optional[str] = None):
    """
    Pulls recent inbound replies addressed to our plus-address alias.
    Creates email_logs rows with status='reply' and updates the lead to 'replied'.
    """
    try:
        creds = _get_authed_creds(user_id)
        svc = _gmail_users_service(creds)
    except Exception as e:
        print("[Gmail Poller] Skipping; cannot auth for user:", e)
        return

    # Figure out the reply domain (same as EMAIL_FROM unless overridden)
    email_local, email_domain = _split_email_address(EMAIL_FROM)
    reply_domain = (to_domain_override or os.getenv("REPLY_TO_DOMAIN") or email_domain)

    # Search last 7 days; exclude our own Sent/Chat.
    # IMPORTANT: no quotes around the address — quotes break wildcard expansion.
    primary_query = f'to:{email_local}+*@{reply_domain} newer_than:7d -in:sent -in:chat'
    print(f"[Gmail Poller] user={user_id} query={primary_query} label=INBOX domain={reply_domain}")

    try:
        msgs = _gmail_list_messages(svc, q=primary_query, label_ids=["INBOX"], max_results=100)
    except Exception as e:
        print("[Gmail Poller] list error:", e)
        return

    # Fallback: if no +tag replies found, also check plain address (some clients strip the +tag).
    if not msgs:
        fallback_query = f'to:{email_local}@{reply_domain} newer_than:7d -in:sent -in:chat'
        print(f"[Gmail Poller] fallback query={fallback_query}")
        try:
            msgs = _gmail_list_messages(svc, q=fallback_query, label_ids=["INBOX"], max_results=100)
        except Exception as e:
            print("[Gmail Poller] fallback list error:", e)
            return

    if not msgs:
        return

    for m in msgs:
        mid = m.get("id")
        if not mid:
            print("[Gmail Poller] skip message with no id")
            continue

        # Skip if we've already processed this Gmail message id
        if _seen_gmail_message(mid):
            print(f"[Gmail Poller] skip already-seen mid={mid}")
            continue

        try:
            full = _gmail_get_message(svc, mid)
            headers = (full or {}).get("payload", {}).get("headers", []) or []
            to_hdr   = _hdr(headers, "Delivered-To") or _hdr(headers, "To")
            from_hdr = _hdr(headers, "From")
            subject  = _hdr(headers, "Subject")
            snippet  = (full or {}).get("snippet", "") or ""
            print(f"[Gmail Poller] mid={mid} to='{to_hdr}' from='{from_hdr}' subj='{subject}'")
        except Exception as e:
            print("[Gmail Poller] get error:", e)
            continue

        # Parse lead_id from plus-addressing (scott+<LEAD_ID>@domain)
        lead_id = parse_lead_id_from_addresses(_extract_emails(to_hdr))
        print(f"[Gmail Poller] mid={mid} parsed lead_id={lead_id}")
        if not lead_id:
            continue

        # Try to insert the reply log
        try:
            supabase.table("email_logs").insert({
                "lead_id": lead_id,
                "to_email": to_hdr,
                "status": "reply",
                "provider": "gmail_inbox",
                "error": "",
                "notes": f"from={from_hdr}; subject={subject}; snippet={snippet[:500]}",
                "idem_key": f"gmail:{mid}",
                "subject": subject or "",
                "body": snippet[:500] if snippet else "",
            }).execute()
            print(f"[Gmail Poller] inserted reply mid={mid} lead_id={lead_id}")
        except Exception as e:
            print("[Gmail Poller] email_logs insert failed:", e)

        # Update lead status → replied + snapshot and stop follow-ups
        try:
            update_lead(lead_id, {
                "status": "replied",
                "last_email_status": "reply",
                "last_reply_at": datetime.utcnow().isoformat(),
                "last_reply_from": from_hdr,
                "last_reply_subject": subject or "",
                "last_reply_snippet": (snippet or "")[:500],
            })
            stop_sequence_for_lead(lead_id, reason="reply")
            print(f"[Gmail Poller] marked replied & stopped sequence lead_id={lead_id}")
        except Exception as e:
            print("[Gmail Poller] lead update/stop failed:", e)

def poll_all_gmail_replies():
    """
    Iterates all connected Google users and polls their inboxes for replies.
    """
    user_ids = _list_google_connected_user_ids()
    if not user_ids:
        print("[Gmail Poller] No connected users to poll")
        return
    for uid in user_ids:
        try:
            poll_gmail_replies_for_user(uid)
        except Exception as e:
            print(f"[Gmail Poller] error polling user {uid}:", e)

@app.post("/webhooks/inbound-email")
async def inbound_email(request: Request):
    try:
        payload = await request.json()
    except Exception:
        return JSONResponse({"ok": False, "error": "non-JSON payload"}, status_code=400)

    tos = _extract_emails(payload.get("to") or payload.get("recipients") or payload.get("envelopeTo"))
    frm = (payload.get("from") or payload.get("sender") or "").strip()
    subject = (payload.get("subject") or "").strip()
    text = (payload.get("text") or payload.get("body-plain") or payload.get("textBody") or "")
    html = (payload.get("html") or payload.get("body-html") or payload.get("htmlBody") or "")

    lead_id = parse_lead_id_from_addresses(tos)
    if not lead_id:
        vars_obj = payload.get("user-variables") or payload.get("custom_variables") or {}
        lead_id = vars_obj.get("lead_id")

    if not lead_id:
        print("[Inbound Email] No lead_id found on", tos)
        return JSONResponse({"ok": True, "note": "no lead_id tag found"}, status_code=200)

    snippet = (text or html or "")[:500]

    # 1) Log the reply in email_logs (as you already do)
    try:
        supabase.table("email_logs").insert({
            "lead_id": lead_id,
            "to_email": ",".join(tos),
            "status": "reply",
            "provider": "inbound",
            "error": "",
            "subject": subject or "",
            "body": snippet or "",
            "notes": f"from={frm}"
         }).execute()
    except Exception as e:
        print("Email reply log insert failed:", e)

    # 2) Stamp a tiny snapshot onto the lead + flip status to 'replied' + stop sequence
    try:
        update_lead(lead_id, {
            "status": "replied",
            "last_email_status": "reply",
            "last_reply_at": datetime.utcnow().isoformat(),
            "last_reply_from": frm,
            "last_reply_subject": subject or "",
            "last_reply_snippet": snippet or "",
        })
        stop_sequence_for_lead(lead_id, reason="reply")
    except Exception as e:
        print("Lead status/snapshot update (reply) failed:", e)

    return {"ok": True, "lead_id": lead_id}

# ---------- DEV: inspect last inbox messages (headers) ----------
@app.get("/api/dev/gmail-last")
def gmail_last(request: Request, max_results: int = 10, q: str = "newer_than:3d"):
    """
    Dump headers of recent messages so we can see what 'To' / 'Delivered-To' look like.
    curl "http://localhost:8000/api/dev/gmail-last?max_results=5&q=newer_than:2d%20-to:me" -H "X-User-Id: <YOUR_ID>"
    """
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")

    try:
        creds = _get_authed_creds(user_id)
        svc = _gmail_users_service(creds)
        msgs = _gmail_list_messages(svc, q=q, label_ids=None, max_results=max(1, min(50, max_results)))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Gmail list error: {e}")

    out = []
    for m in msgs:
        try:
            full = svc.users().messages().get(
                userId="me",
                id=m["id"],
                format="metadata",
                metadataHeaders=["To", "Delivered-To", "From", "Subject", "Date", "Cc", "Bcc", "Return-Path", "X-Original-To", "Envelope-To", "Received"]
            ).execute()
            headers = (full or {}).get("payload", {}).get("headers", []) or []
            def H(n): 
                return next((h.get("value","") for h in headers if h.get("name","").lower()==n.lower()), "")
            out.append({
                "id": m["id"],
                "threadId": full.get("threadId"),
                "From": H("From"),
                "To": H("To"),
                "Delivered-To": H("Delivered-To"),
                "Cc": H("Cc"),
                "Bcc": H("Bcc"),
                "Return-Path": H("Return-Path"),
                "X-Original-To": H("X-Original-To"),
                "Envelope-To": H("Envelope-To"),
                "Subject": H("Subject"),
                "Date": H("Date"),
                "snippet": (full.get("snippet") or "")[:160],
            })
        except Exception as e:
            out.append({"id": m.get("id"), "error": str(e)})
    return {"items": out}

# ===================================================
# Webhook (provider-agnostic updates to call_logs)
# ===================================================
def _extract_status(evt: dict) -> Optional[str]:
    """
    Return a normalized status string if present (accepts mid-call variants),
    otherwise None. Normalizes common provider variants to our canonical names.
    """
    # look in several likely places
    raw = (
        (evt.get("status") or evt.get("callStatus") or "").strip()
        or ((evt.get("message") or {}).get("status") or "").strip()
        or ((evt.get("call") or {}).get("status") or "").strip()
        or (((evt.get("message") or {}).get("call") or {}).get("status") or "").strip()
    )

    s = str(raw).lower().replace("_", "-")

    # normalize common variants
    alias = {
        "noanswer": "no-answer",
        "no-answer": "no-answer",
        "no_answer": "no-answer",
        "ended": "completed",
        "complete": "completed",
        "completed": "completed",
        "busy": "busy",
        "failed": "failed",
        "canceled": "canceled",
        "cancelled": "canceled",
        "in-progress": "in-progress",
        "ringing": "ringing",
        "queued": "queued",
        "starting": "starting",
    }
    return alias.get(s) or None

def _extract_ids(evt: dict) -> Tuple[Optional[str], Optional[str]]:
    """
    Return (external_call_id, lead_id) from a Vapi webhook-style event.
    Looks in multiple likely locations for robustness.
    """
    msg = evt.get("message") or {}
    call = evt.get("call") or msg.get("call") or {}
    external_call_id = call.get("id")

    # lead_id is usually in call.metadata, but fall back to top-level metadata
    metadata = call.get("metadata") or evt.get("metadata") or msg.get("metadata") or {}
    lead_id = metadata.get("lead_id")

    # Some providers/versions pass variables via assistantOverrides.variableValues
    if not lead_id:
        aov = evt.get("assistantOverrides") or msg.get("assistantOverrides") or {}
        vars_ = aov.get("variableValues") or {}
        lead_id = vars_.get("lead_id")

    return external_call_id, lead_id

def _maybe_schedule_followup_from_event(user_id: str, evt: dict, lead: Optional[dict]):
    try:
        meta = (evt.get("call") or {}).get("metadata") or evt.get("metadata") or {}
        start_iso = meta.get("followup_start")
        end_iso = meta.get("followup_end")
        summary = meta.get("summary") or "Follow-up call"
        attendees = meta.get("attendees") or []
        if not start_iso or not end_iso:
            return
        payload = {
            "summary": summary,
            "description": f"Lead: {lead.get('first_name','')} {lead.get('last_name','')} | Company: {lead.get('company_name','')}" if lead else summary,
            "start": {"dateTime": start_iso},
            "end": {"dateTime": end_iso},
            "attendees": [{"email": a} for a in attendees if isinstance(a, str) and a],
        }
        created = _calendar_create_event(user_id, payload)
        if created and created.get("htmlLink"):
            print("[Calendar] Follow-up booked:", created["htmlLink"])
    except Exception as e:
        print("[Calendar] follow-up auto-book error:", e)

# ===================================================
# Outbox worker: the ONLY place emails are actually sent
# ===================================================
def _process_email_outbox_tick(batch_size: int = 25):
    now_iso = datetime.utcnow().isoformat()
    my_lock = str(uuid4())

    # 1) Load candidates due now
    try:
        cand = (supabase.table("email_outbox")
                .select("*")
                .lte("send_after", now_iso)
                .eq("status", "queued")
                .order("send_after", desc=False)
                .limit(batch_size)
                .execute()).data or []
    except Exception as e:
        print("[OUTBOX] select candidates failed:", e)
        return

    if not cand:
        return

    for row in cand:
        rid = row.get("id")
        if not rid:
            continue

        # 2) Try to claim the row atomically
        try:
            upd = (supabase.table("email_outbox")
                   .update({"status": "sending", "lock_token": my_lock, "attempts": (int(row.get("attempts") or 0) + 1)})
                   .eq("id", rid)
                   .eq("status", "queued")
                   .execute())
            owned = (getattr(upd, "data", None) or [])
            if not owned:
                # Someone else claimed it
                continue
        except Exception as e:
            print(f"[OUTBOX] claim failed id={rid}:", e)
            continue

        # 3) Load lead & user_id needed to send
        try:
            lead_id = row.get("lead_id")
            lead_res = supabase.table("leads").select("*").eq("id", lead_id).single().execute()
            lead = getattr(lead_res, "data", None) or {}
            user_id = lead.get("user_id") or DEFAULT_USER_ID
        except Exception as e:
            print(f"[OUTBOX] fetch lead failed id={rid}:", e)
            # return row to queue with backoff
            try:
                supabase.table("email_outbox").update({
                    "status": "queued",
                    "send_after": (datetime.utcnow() + timedelta(minutes=5)).isoformat(),
                    "last_error": f"lead_fetch:{e}",
                }).eq("id", rid).eq("lock_token", my_lock).execute()
            except Exception:
                pass
            continue

        # 4) Send (use the subject/body snapshot from outbox row)
        try:
            # plus-addressing tag for replies
            local, domain = _split_email_address(EMAIL_FROM)
            reply_to = f"{local}+{lead.get('id')}@{domain}" if local and domain and lead.get("id") else None

            send_email_via_gmail_api(
                user_id=user_id,
                to_email=row.get("to_email") or "",
                subject=row.get("subject") or "",
                body=row.get("body") or "",
                reply_to=reply_to
            )

            # Success → mark sent, mirror to email_logs
            supabase.table("email_outbox").update({
                "status": "sent",
                "provider": "gmail_api",
                "last_error": "",
            }).eq("id", rid).eq("lock_token", my_lock).execute()

            # mirror log for analytics/history
            try:
                log_email_to_supabase(
                    lead_id=lead.get("id"),
                    to_email=row.get("to_email") or "",
                    status="sent",
                    error_msg="",
                    subject=row.get("subject") or "",
                    body=row.get("body") or "",
                    provider="gmail_api",
                    idem_key=row.get("idem_key"),
                    notes="outbox"
                )
            except Exception:
                pass

            # Only stamp emailed_at for the FIRST email (step 0/1). Follow-ups don’t re-stamp.
            try:
                if int(row.get("step_number") or 0) in (0, 1):
                    update_lead(lead.get("id"), {"emailed_at": datetime.utcnow().isoformat(), "last_email_status": "sent"})
                else:
                    update_lead(lead.get("id"), {"last_email_status": "sent"})
            except Exception:
                pass

            print(f"[OUTBOX] sent idem_key={row.get('idem_key')} to={row.get('to_email')}")
        except Exception as e:
            # Failure → backoff and requeue
            print(f"[OUTBOX] send failed idem_key={row.get('idem_key')} err={e}")
            try:
                supabase.table("email_outbox").update({
                    "status": "queued",
                    "send_after": (datetime.utcnow() + timedelta(minutes=10)).isoformat(),
                    "last_error": str(e)[:500],
                }).eq("id", rid).eq("lock_token", my_lock).execute()
            except Exception:
                pass

def _schedule_jobs():
    # Calls poller (ok on both roles if you want)
    scheduler.add_job(
        poll_due_calls,
        trigger="interval", minutes=1,
        id="calls-poller",
        replace_existing=True,
        coalesce=True,
        max_instances=1,
    )

    # Only schedule follow-up email steps on the worker AND when enabled
    if PROCESS_ROLE == "worker" and EMAIL_SEQUENCE_SCHEDULER_ENABLED:
        scheduler.add_job(
            poll_due_email_steps,
            trigger="interval", minutes=5,
            id="email-steps-poller",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        print("[Scheduler] Email steps poller scheduled (every 5m) on worker")
    else:
        why = []
        if PROCESS_ROLE != "worker":
            why.append(f"PROCESS_ROLE={PROCESS_ROLE}")
        if not EMAIL_SEQUENCE_SCHEDULER_ENABLED:
            why.append("EMAIL_SEQUENCE_SCHEDULER_ENABLED=false")
        print("[Scheduler] Email steps poller NOT scheduled:", "; ".join(why) or "unknown reason")

    # Outbox sender — only on worker
    if PROCESS_ROLE == "worker":
        scheduler.add_job(
            _process_email_outbox_tick,
            trigger="interval", seconds=30,
            id="email-outbox-sender",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        print("[Scheduler] Outbox sender scheduled (every 30s)")

    # Gmail reply poller (leave as-is)
    if _GOOGLE_LIBS_AVAILABLE:
        scheduler.add_job(
            poll_all_gmail_replies,
            trigger="interval", minutes=2,
            id="gmail-replies-poller",
            replace_existing=True,
            coalesce=True,
            max_instances=1,
        )
        print("[Scheduler] Gmail replies poller scheduled (every 2m)")
    else:
        print("[Scheduler] Gmail libs missing; reply poller not scheduled")

@app.on_event("startup")
def on_startup():
    if SKIP_SUPABASE_PROBE:
        print("[Supabase] Probe skipped by SKIP_SUPABASE_PROBE=true")
    else:
        _assert_supabase_ok()

    # Always log Vapi env presence (booleans only; no secrets)
    print(f"[VAPI][ENV] assistantId set? {bool(VAPI_ASSISTANT_ID)} | phoneNumberId set? {bool(VAPI_PHONE_NUMBER_ID)} | apiKey set? {bool(VAPI_API_KEY)}")

    try:
        if not getattr(scheduler, "running", False):
            scheduler.start()
            print("[Scheduler] Started")
        _schedule_jobs()
    except SchedulerAlreadyRunningError:
        print("[Scheduler] Already running; refreshing jobs")
        _schedule_jobs()

@app.post("/vapi/webhook")
async def vapi_webhook(request: Request):
    try:
        evt = await request.json()
        print(f"[VAPI][WEBHOOK] status={(_extract_status(evt) or 'n/a')} lead_id={((_extract_ids(evt) or (None,None))[1])} campaign={((evt.get('call') or {}).get('metadata',{}) or {}).get('campaign_id')}")
    except Exception:
        return JSONResponse({"ok": True}, status_code=200)

    # Extract identifiers and a (possibly non-terminal) status
    external_call_id, lead_id = _extract_ids(evt)
    status = _extract_status(evt)  # may be a mid-call status like 'ringing' / 'in-progress'
    summary = evt.get("summary") or (evt.get("message") or {}).get("summary") or ""

    # Lightweight observability row so activity appears on the dashboard immediately
    if lead_id:
        try:
            supabase.table("call_logs").insert({
                "lead_id": lead_id,
                "call_status": (status or "event"),
                "provider": VOICE_PROVIDER_NAME,
                "external_call_id": external_call_id,
                "notes": (summary or "")[:500],
            }).execute()
        except Exception as e:
            print("Call log (event) insert failed:", e)

    # If we don't have a recognizable status yet, just ACK
    if not status:
        return JSONResponse({"ok": True}, status_code=200)

    # From this point, only do the heavy updates for terminal statuses
    if status not in TERMINAL_STATUSES:
        return JSONResponse({"ok": True}, status_code=200)

    # ----- existing terminal processing (kept as-is) -----
    if not lead_id:
        return JSONResponse({"ok": True}, status_code=200)

    # Try to parse name/company out of the provider's summary (if present)
    try:
        parts = [p.strip() for p in summary.split(";") if "=" in p]
        kv = {}
        for p in parts:
            k, v = p.split("=", 1)
            kv[k.strip().lower()] = v.strip()
        name_from_call = kv.get("name")
        company_from_call = kv.get("company")
    except Exception:
        name_from_call = company_from_call = None

    call_obj = (evt.get("call") or (evt.get("message") or {}).get("call") or {}) or {}
    meta = call_obj.get("metadata") or {}

    started_at = call_obj.get("startedAt") or call_obj.get("startTime") or meta.get("started_at")
    ended_at   = call_obj.get("endedAt")   or call_obj.get("endTime")   or meta.get("ended_at")
    duration_seconds = (call_obj.get("durationSeconds") or call_obj.get("duration") or meta.get("duration_seconds"))
    recording_url = (call_obj.get("recordingUrl") or meta.get("recording_url") or meta.get("recordingUrl"))

    patch = {
        "call_status": status,
        "provider": VOICE_PROVIDER_NAME,
    }
    if started_at:        patch["started_at"] = started_at
    if ended_at:          patch["ended_at"] = ended_at
    if duration_seconds:  patch["duration_seconds"] = duration_seconds
    if recording_url:     patch["recording_url"] = recording_url

    update_structured_call_log(lead_id, external_call_id, patch)
    log_call_to_supabase(lead_id, status, (summary or f"external_call_id={external_call_id}" or "").strip())
    print(f"[Webhook] {status} for lead {lead_id}")

    if status == "completed":
        lead_patch = {
            "status": "contacted",
            "last_call_status": status,
            "next_call_at": None,
        }
        if name_from_call:
            lead_patch["name"] = name_from_call
            lead_patch["first_name"] = name_from_call.split()[0].strip()
        if company_from_call:
            lead_patch["company_name"] = company_from_call
        update_lead(lead_id, lead_patch)
                # BILL the call based on durationSeconds (rounded up to minutes)
        try:
            dur = 0
            try:
                dur = int(duration_seconds) if duration_seconds is not None else 0
            except Exception:
                dur = 0
            bill_call_completion(
                supabase=supabase,
                lead_id=lead_id,
                external_call_id=external_call_id,
                duration_seconds=dur
            )
        except Exception as bill_e:
            print("[CREDITS] billing error:", bill_e)

    elif status in ("failed",):
        update_lead(lead_id, {"last_call_status": status, "next_call_at": None})

    elif status in ("no-answer", "busy"):
        lead_resp = supabase.table("leads").select("*").eq("id", lead_id).single().execute()
        lead = getattr(lead_resp, "data", None)
        if lead:
            rules = get_campaign_rules(lead.get("campaign_id"))
            inc_attempts_and_reschedule(
                lead,
                max_attempts=rules["max_attempts"],
                after_minutes=rules["retry_minutes"]
            )

    return JSONResponse({"ok": True}, status_code=200)

# ===================================================
# GOOGLE OAUTH 2.0 + CALENDAR
# ===================================================
def _ensure_google_ready():
    if not _GOOGLE_LIBS_AVAILABLE:
        raise HTTPException(
            status_code=500,
            detail="Google libs not installed. Run: pip install google-auth-oauthlib google-api-python-client"
        )
    if not GOOGLE_CLIENT_ID or not GOOGLE_CLIENT_SECRET:
        raise HTTPException(
            status_code=500,
            detail="Missing GOOGLE_CLIENT_ID / GOOGLE_CLIENT_SECRET env vars."
        )
    if not GOOGLE_REDIRECT_URI:
        raise HTTPException(
            status_code=500,
            detail="Missing GOOGLE_REDIRECT_URI (or OAUTH_EXTERNAL_BASE_URL)"
        )

def _flow_for_oauth(state: str):
    client_config = {
        "web": {
            "client_id": GOOGLE_CLIENT_ID,
            "auth_uri": "https://accounts.google.com/o/oauth2/v2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
            "client_secret": GOOGLE_CLIENT_SECRET,
            "redirect_uris": [GOOGLE_REDIRECT_URI],
        }
    }
    flow = GFlow.from_client_config(client_config, scopes=GOOGLE_SCOPES, state=state, redirect_uri=GOOGLE_REDIRECT_URI)
    return flow

def _upsert_google_tokens(user_id: str, creds: "GCredentials"):
    try:
        payload = {
            "user_id": user_id,
            "access_token": creds.token,
            "refresh_token": getattr(creds, "refresh_token", None),
            "token_uri": creds.token_uri,
            "client_id": creds.client_id,
            "client_secret": creds.client_secret,
            "scopes": list(creds.scopes or []) if getattr(creds, "scopes", None) else GOOGLE_SCOPES,
            "expiry": creds.expiry.isoformat() if getattr(creds, "expiry", None) else None
        }
        supabase.table(GOOGLE_TOKENS_TABLE).upsert(payload, on_conflict="user_id").execute()
    except Exception as e:
        print("[Google] Token upsert failed:", e)
        raise HTTPException(status_code=500, detail="Unable to store Google tokens.")

def _load_google_tokens(user_id: str) -> Optional[dict]:
    try:
        res = supabase.table(GOOGLE_TOKENS_TABLE).select("*").eq("user_id", user_id).single().execute()
        return getattr(res, "data", None)
    except Exception as e:
        print("[Google] Load tokens failed:", e)
        return None

def _list_google_connected_user_ids() -> List[str]:
    """
    Returns user_ids that have Google tokens saved.
    Falls back to [DEFAULT_USER_ID] if token table access fails.
    """
    try:
        rows = supabase.table(GOOGLE_TOKENS_TABLE).select("user_id").limit(1000).execute()
        data = getattr(rows, "data", []) or []
        seen = set()
        out: List[str] = []
        for r in data:
            uid = r.get("user_id")
            if uid and uid not in seen:
                out.append(uid)
                seen.add(uid)
        return out if out else ([DEFAULT_USER_ID] if DEFAULT_USER_ID else [])
    except Exception as e:
        print("[Gmail Poller] list tokens failed:", e)
        return [DEFAULT_USER_ID] if DEFAULT_USER_ID else []

def _creds_from_row(row: dict) -> Optional["GCredentials"]:
    if not row:
        return None
    try:
        creds = GCredentials(
            token=row.get("access_token"),
            refresh_token=row.get("refresh_token"),
            token_uri=row.get("token_uri") or "https://oauth2.googleapis.com/token",
            client_id=row.get("client_id") or GOOGLE_CLIENT_ID,
            client_secret=row.get("client_secret") or GOOGLE_CLIENT_SECRET,
            scopes=row.get("scopes") or GOOGLE_SCOPES,
        )
        exp = row.get("expiry") or row.get("expires_at")
        if exp:
            try:
                dt = datetime.fromisoformat(str(exp).replace("Z", "+00:00"))
                creds.expiry = dt.astimezone(timezone.utc).replace(tzinfo=None)
            except Exception:
                creds.expiry = None
        return creds
    except Exception as e:
        print("[Google] creds_from_row error:", e)
        return None

def _refresh_if_needed(creds: "GCredentials") -> "GCredentials":
    try:
        exp = getattr(creds, "expiry", None)
        needs_refresh = False
        if exp:
            now = datetime.utcnow()
            needs_refresh = (exp <= now + timedelta(seconds=60))
        if not needs_refresh:
            try:
                needs_refresh = bool(getattr(creds, "expired", False))
            except Exception:
                pass
        if needs_refresh and getattr(creds, "refresh_token", None):
            creds.refresh(GAuthRequest())
        return creds
    except Exception as e:
        print("[Google] Token refresh failed:", e)
        raise HTTPException(status_code=401, detail="Google token expired and refresh failed.")

def _calendar_service(creds: "GCredentials"):
    return g_build("calendar", "v3", credentials=creds, cache_discovery=False)

@app.get("/auth/google/start")
def google_auth_start(request: Request):
    _ensure_google_ready()
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id (X-User-Id header or ?user_id=).")
    state = request.query_params.get("state") or f"uid:{user_id}"
    flow = _flow_for_oauth(state)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    return {"auth_url": auth_url}

@app.get("/api/google/oauth/start")
def api_google_oauth_start(request: Request):
    _ensure_google_ready()
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id (X-User-Id header or ?user_id=).")
    state = request.query_params.get("state") or f"uid:{user_id}"
    flow = _flow_for_oauth(state)
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent"
    )
    return RedirectResponse(url=auth_url, status_code=307)

@app.get("/auth/google/callback")
def google_auth_callback(request: Request):
    _ensure_google_ready()
    params = dict(request.query_params)
    error = params.get("error")
    if error:
        return JSONResponse({"ok": False, "error": error}, status_code=400)

    code = params.get("code")
    state = params.get("state") or ""
    user_id = state[4:] if state.startswith("uid:") else _get_request_user_id(request)
    if not user_id:
        return JSONResponse({"ok": False, "error": "No user_id to store tokens"}, status_code=400)

    flow = _flow_for_oauth(state or f"uid:{user_id}")
    flow.fetch_token(code=code)
    creds = flow.credentials
    _upsert_google_tokens(user_id, creds)

    html = "<script>window.close();</script><p>Google connected. You may close this tab.</p>"
    return HTMLResponse(content=html)

def _get_authed_creds(user_id: str) -> "GCredentials":
    _ensure_google_ready()
    row = _load_google_tokens(user_id)
    if not row:
        raise HTTPException(status_code=401, detail="Google not connected for this user.")
    creds = _creds_from_row(row)
    if not creds:
        raise HTTPException(status_code=401, detail="Invalid stored Google credentials.")
    creds = _refresh_if_needed(creds)
    try:
        _upsert_google_tokens(user_id, creds)
    except Exception:
        pass
    return creds

def _calendar_create_event(user_id: str, event_body: dict) -> Optional[dict]:
    creds = _get_authed_creds(user_id)
    svc = _calendar_service(creds)
    try:
        created = svc.events().insert(calendarId="primary", body=event_body, sendUpdates="all").execute()
        return created
    except Exception as e:
        print("[Google] create_event error:", e)
        raise HTTPException(status_code=500, detail="Google Calendar create event failed.")

@app.get("/api/calendar/list")
def calendar_list(request: Request):
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")
    creds = _get_authed_creds(user_id)
    svc = _calendar_service(creds)
    now = datetime.utcnow().isoformat() + "Z"
    try:
        events = (svc.events()
                  .list(calendarId="primary", timeMin=now, maxResults=10,
                        singleEvents=True, orderBy="startTime").execute())
        return {"ok": True, "events": events.get("items", [])}
    except Exception as e:
        print("[Google] list error:", e)
        raise HTTPException(status_code=500, detail="Unable to list events")

@app.post("/api/calendar/quick-add")
async def calendar_quick_add(request: Request):
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")
    body = await request.json()
    if "start" not in body or "end" not in body:
        raise HTTPException(status_code=400, detail="Missing 'start'/'end'")
    created = _calendar_create_event(user_id, body)
    return {"ok": True, "event": created}

# =========================
# >>> Lovable alias routes <<<
# =========================
@app.get("/oauth/google/start")
def oauth_google_start_alias(request: Request):
    return google_auth_start(request)

@app.get("/oauth/callback")
def oauth_callback_alias(request: Request):
    return google_auth_callback(request)

@app.get("/oauth/status")
def oauth_status(request: Request):
    """
    Lightweight check used by the UI:
    If we have any token row for this user, consider them 'connected'.
    (Deep verification happens when we actually call Google.)
    """
    user_id = _get_request_user_id(request)
    if not user_id:
        return {"connected": False}

    row = _load_google_tokens(user_id)
    if row and (row.get("access_token") or row.get("refresh_token")):
        return {"connected": True}
    return {"connected": False}

@app.get("/api/dev/google-scopes")
def dev_google_scopes(request: Request):
    """
    Returns the stored scopes for this user's Google token row so we can verify
    gmail.readonly is actually present.
    """
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id (X-User-Id or ?user_id=)")
    row = _load_google_tokens(user_id)
    if not row:
        return {"ok": False, "error": "No token row"}
    return {
        "ok": True,
        "user_id": user_id,
        "scopes_in_row": row.get("scopes"),
        "has_gmail_readonly": ("https://www.googleapis.com/auth/gmail.readonly" in (row.get("scopes") or [])),
        "expiry": row.get("expiry"),
    }

@app.post("/oauth/google/disconnect")
def oauth_disconnect(request: Request):
    user_id = _get_request_user_id(request)
    if user_id:
        try:
            supabase.table(GOOGLE_TOKENS_TABLE).delete().eq("user_id", user_id).execute()
        except Exception as e:
            print("Disconnect failed:", e)
    return {"ok": True}

@app.get("/calendar/events")
def list_events_alias(
    request: Request,
    timeMin: Optional[str] = None,
    timeMax: Optional[str] = None,
    maxResults: int = 50,
):
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")

    creds = _get_authed_creds(user_id)
    svc = _calendar_service(creds)

    if not timeMin:
        timeMin = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    if not timeMax:
        timeMax = (datetime.utcnow() + timedelta(days=7)).replace(microsecond=0).isoformat() + "Z"

    try:
        r = (svc.events()
             .list(calendarId="primary", singleEvents=True, orderBy="startTime",
                   timeMin=timeMin, timeMax=timeMax,
                   maxResults=max(1, min(maxResults, 250))).execute())
        return {"ok": True, "items": r.get("items", [])}
    except Exception as e:
        print("[Google] list events error:", e)
        raise HTTPException(status_code=500, detail="Unable to list events")

@app.get("/api/calendar/events")
def list_events_api_alias(
    request: Request,
    timeMin: Optional[str] = None,
    timeMax: Optional[str] = None,
    maxResults: int = 50,
):
    return list_events_alias(request, timeMin=timeMin, timeMax=timeMax, maxResults=maxResults)

@app.post("/api/calendar/events")
async def create_event_api_alias(request: Request):
    return await create_event_alias(request)

@app.post("/calendar/events")
async def create_event_alias(request: Request):
    user_id = _get_request_user_id(request)
    if not user_id:
        raise HTTPException(status_code=400, detail="Missing user_id")
    body = await request.json()
    start = body.get("start")
    end = body.get("end")
    if isinstance(start, dict): start = start.get("dateTime")
    if isinstance(end, dict):   end = end.get("dateTime")
    if not start or not end:
        raise HTTPException(status_code=400, detail="Missing start/end")
    event = {
        "summary": body.get("summary") or "Follow-up call",
        "description": body.get("description") or "",
        "start": {"dateTime": start},
        "end": {"dateTime": end},
    }
    if body.get("attendees"):
        event["attendees"] = [{"email": e} if isinstance(e, str) else e for e in body["attendees"]]
    created = _calendar_create_event(user_id, event)
    return {"ok": True, "event": created}

# ===================================================
# ADMIN-ONLY USER MANAGEMENT (self-contained block)
# ===================================================
ADMIN_USER_IDS = {u.strip() for u in os.getenv("ADMIN_USER_IDS", "").split(",") if u.strip()}

def user_is_admin(user_id: Optional[str]) -> bool:
    if not user_id:
        return False
    try:
        res = supabase.table("profiles").select("is_admin").eq("id", user_id).single().execute()
        row = getattr(res, "data", None)
        if isinstance(row, dict):
            return bool(row.get("is_admin"))
    except Exception as e:
        print("[ADMIN] profiles lookup failed or missing; using env fallback:", e)
    return user_id in ADMIN_USER_IDS

def _require_admin(request: Request) -> str:
    uid = _get_request_user_id(request)
    if not uid or not user_is_admin(uid):
        raise HTTPException(status_code=403, detail="Admin only")
    return uid

def _upsert_profile_flag(user_id: str, is_admin: bool):
    try:
        supabase.table("profiles").upsert({"id": user_id, "is_admin": is_admin}).execute()
    except Exception as e:
        print("[ADMIN] upsert profiles failed (ensure 'profiles' table exists):", e)

def _auth_admin_headers():
    if not SUPABASE_KEY or "service" not in (_decode_jwt_role(SUPABASE_KEY) or ""):
        print("!!! WARNING: SUPABASE_KEY is not service_role; admin endpoints may fail.")
    return {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
    }

@app.post("/api/admin/users")
async def admin_create_user(request: Request):
    _require_admin(request)

    body = await request.json()
    email = (body.get("email") or "").strip().lower()
    password = (body.get("password") or "").strip()
    name = (body.get("name") or "").strip()
    is_admin_flag = bool(body.get("is_admin", False))

    if not email or not password:
        raise HTTPException(status_code=400, detail="email and password are required")

    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    payload = {
        "email": email,
        "password": password,
        "email_confirm": True,
        "user_metadata": {"name": name} if name else {},
    }

    try:
        r = requests.post(url, headers=_auth_admin_headers(), json=payload, timeout=20)
        if r.status_code >= 300:
            try:
                err = r.json()
            except Exception:
                err = {"message": r.text}
            raise HTTPException(status_code=r.status_code, detail=f"Auth create failed: {err}")
        user = r.json()
        auth_id = user.get("id")

        if auth_id is not None:
            _upsert_profile_flag(auth_id, is_admin_flag)

        return {"ok": True, "user": user}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Create user error: {e}")

@app.get("/api/admin/users")
def admin_list_users(request: Request, limit: int = 50, page: int = 1, search: Optional[str] = None):
    _require_admin(request)

    url = f"{SUPABASE_URL}/auth/v1/admin/users"
    params = {"per_page": max(1, min(200, int(limit))), "page": max(1, int(page))}
    if search:
        params["email"] = search

    try:
        r = requests.get(url, headers=_auth_admin_headers(), params=params, timeout=20)
        if r.status_code >= 300:
            try:
                err = r.json()
            except Exception:
                err = {"message": r.text}
            raise HTTPException(status_code=r.status_code, detail=f"Auth list failed: {err}")
        return {"ok": True, "items": r.json()}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"List users error: {e}")

@app.post("/api/admin/users/{auth_user_id}/set-admin")
async def admin_set_admin_flag(request: Request, auth_user_id: str):
    _require_admin(request)
    try:
        body = await request.json()
    except Exception:
        body = {}
    is_admin_flag = bool(body.get("is_admin", True))

    try:
        _upsert_profile_flag(auth_user_id, is_admin_flag)
        return {"ok": True, "user_id": auth_user_id, "is_admin": is_admin_flag}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Set admin error: {e}")

@app.get("/api/credits")
def get_credits(request: Request):
    uid = _get_request_user_id(request)
    domain = email_domain_of(supabase, uid)
    if not domain:
        return {"ok": True, "domain": None, "balance_cents": 0}
    return {"ok": True, "domain": domain, "balance_cents": domain_balance(supabase, domain)}

@app.post("/api/credits/topup")
async def post_topup(request: Request):
    """
    Call this after a successful payment.
    Body: { "amount_cents": 5000 }  # $50.00
    """
    uid = _get_request_user_id(request)
    if not uid:
        raise HTTPException(status_code=400, detail="Missing user_id")
    domain = email_domain_of(supabase, uid)
    if not domain:
        raise HTTPException(status_code=400, detail="Cannot resolve email domain for user")

    body = await request.json()
    amount_cents_raw = body.get("amount_cents")
    try:
        amount_cents = int(amount_cents_raw)
    except Exception:
        amount_cents = 0
    if amount_cents <= 0:
        raise HTTPException(status_code=400, detail="amount_cents must be > 0")

    new_balance = domain_add_credits(supabase, domain, amount_cents, reason="topup", meta={"user_id": uid})
    return {"ok": True, "domain": domain, "balance_cents": new_balance}
