# credits.py
import os
import math
import json
from typing import Optional, Dict, Any

# ---- Compatibility knobs -----------------------------------------------------
# Old code passed "cents" thresholds; we derive required credits from that if present.
PRICE_CENTS_PER_MINUTE = int(os.getenv("PRICE_CENTS_PER_MINUTE", "30"))  # legacy; only used for compatibility math
MIN_REQUIRED_CREDITS   = int(os.getenv("MIN_REQUIRED_CREDITS", "1"))     # default: must have >=1 credit to start

# Back-compat for main.py: it imports MIN_RESERVE_CENTS.
# If MIN_RESERVE_CENTS env is set, we honor it; otherwise derive from credits model:
# required_credits * price_cents_per_minute  (1 credit * 30c = 30 cents)
MIN_RESERVE_CENTS = int(os.getenv("MIN_RESERVE_CENTS", str(MIN_REQUIRED_CREDITS * PRICE_CENTS_PER_MINUTE)))

# -----------------------------------------------------------------------------
# Email domain resolver (unchanged)
# -----------------------------------------------------------------------------
def email_domain_of(supabase, user_id: Optional[str]) -> Optional[str]:
    """Resolve a user's email domain for shared balance."""
    if not user_id:
        return None
    try:
        # Try profiles table first
        r = supabase.table("profiles").select("email").eq("id", user_id).single().execute()
        email = (getattr(r, "data", None) or {}).get("email")
        if not email:
            # Optional fallback if you mirror auth.users
            try:
                r2 = supabase.table("auth_users").select("email").eq("id", user_id).single().execute()
                email = (getattr(r2, "data", None) or {}).get("email")
            except Exception:
                email = None
        if not email or "@" not in email:
            return None
        return email.split("@", 1)[1].lower().strip()
    except Exception:
        return None

# -----------------------------------------------------------------------------
# Domain credits (INTEGER credits, not cents)
# -----------------------------------------------------------------------------
def domain_balance_credits(supabase, domain: str) -> int:
    """
    Reads INTEGER credits from domain_credits.balance_credits.
    """
    try:
        r = (
            supabase.table("domain_credits")
            .select("balance_credits")
            .eq("domain", domain)
            .single()
            .execute()
        )
        row = getattr(r, "data", None) or {}
        bal = int(row.get("balance_credits") or 0)
        print(f"[CREDITS][BALANCE] domain={domain} balance_credits={bal}")
        return bal
    except Exception:
        return 0

# Back-compat alias so existing main.py calls keep working.
def domain_balance(supabase, domain: str) -> int:
    # return balance in CREDITS to keep /api/credits simple
    return domain_balance_credits(supabase, domain)

def domain_add_credits(
    supabase,
    domain: str,
    amount_credits: int,
    reason: str = "topup",
    meta: Dict[str, Any] | None = None
) -> int:
    """
    Adds credits to domain_credits.balance_credits (upsert).
    Prefer a dedicated RPC if you have one; this keeps simple upsert semantics.
    """
    try:
        # Ensure row exists
        supabase.table("domain_credits").upsert(
            {"domain": domain, "balance_credits": 0}
        ).execute()

        cur = (
            supabase.table("domain_credits")
            .select("balance_credits")
            .eq("domain", domain)
            .single()
            .execute()
            .data
            or {}
        )
        new_bal = int(cur.get("balance_credits") or 0) + int(amount_credits)
        supabase.table("domain_credits").update(
            {"balance_credits": new_bal}
        ).eq("domain", domain).execute()

        # Optional: record a ledger row if you maintain one
        try:
            supabase.table("credits_ledger").insert({
                "domain": domain,
                "delta_credits": int(amount_credits),
                "reason": reason,
                "meta": meta or {}
            }).execute()
        except Exception:
            pass

        print(f"[CREDITS][TOPUP] domain={domain} +{int(amount_credits)} -> {new_bal}")
        return new_bal
    except Exception:
        return domain_balance_credits(supabase, domain)

def domain_spend_credits(
    supabase,
    domain: str,
    amount_credits: int,
    reason: str = "call_charge",
    meta: Dict[str, Any] | None = None
) -> int:
    """
    Deduct credits using the spend_credits RPC.
    Contract:
      - RPC: spend_credits(p_domain text, p_amount_credits int, p_reason text, p_meta jsonb)
      - Should return {"ok": true, "new_balance": <int>} (if implemented that way),
        otherwise we re-query the balance after the call.
    """
    payload = {
        "p_domain": domain,
        "p_amount_credits": int(amount_credits),
        "p_reason": reason,
        "p_meta": meta or {},
    }
    print(f"[CREDITS][SPEND] domain={domain} amount_credits={int(amount_credits)} payload={json.dumps(payload)}")
    res = supabase.rpc("spend_credits", payload).execute()

    # If your RPC returns the new balance in res.data, use it; otherwise re-query.
    new_bal = None
    try:
        if getattr(res, "data", None) and isinstance(res.data, dict):
            if "new_balance" in res.data:
                new_bal = int(res.data["new_balance"])
    except Exception:
        new_bal = None

    if new_bal is None:
        new_bal = domain_balance_credits(supabase, domain)

    print(f"[CREDITS][AFTER SPEND] domain={domain} balance_credits={new_bal}")
    return new_bal

# -----------------------------------------------------------------------------
# Call gating & billing (credits-based)
# -----------------------------------------------------------------------------
def _required_credits_from_legacy(min_reserve_cents: int | None) -> int:
    """
    Convert legacy 'min_reserve_cents' into required credits, keeping at least 1.
    If min_reserve_cents is 30 and price is 30c/min, this yields 1 credit.
    """
    if not min_reserve_cents or min_reserve_cents <= 0:
        return max(1, MIN_REQUIRED_CREDITS)
    per_min = max(1, PRICE_CENTS_PER_MINUTE)
    return max(MIN_REQUIRED_CREDITS, math.ceil(min_reserve_cents / per_min))

def ensure_credit_before_call(
    supabase,
    lead: Dict[str, Any],
    min_reserve_cents: int,   # kept for backward-compat; we translate it to credits
    log_call_cb,              # function(lead_id, status, notes)
    update_lead_cb            # function(lead_id, patch_dict)
) -> bool:
    """
    Return True if there is enough shared domain credit to start a call, else log+mark and return False.
    Now credits-based: require at least 1 credit (or legacy-derived equivalent).
    """
    user_id = lead.get("user_id")
    domain = email_domain_of(supabase, user_id)
    if not domain:
        # If we can't resolve a domain, allow the call (or flip to block if you prefer stricter)
        return True

    required_credits = _required_credits_from_legacy(min_reserve_cents)
    bal = domain_balance_credits(supabase, domain)

    if bal < required_credits:
        lead_id = lead.get("id")
        log_call_cb(lead_id, "blocked", f"insufficient_funds domain={domain} bal_credits={bal} required={required_credits}")
        update_lead_cb(lead_id, {"last_call_status": "blocked_insufficient_credits"})
        print(f"[CREDITS] Blocked call (insufficient) domain={domain} balance={bal} required={required_credits}")
        return False

    print(f"[CREDITS] Allowed call start domain={domain} balance={bal} required={required_credits}")
    return True

def bill_call_completion(
    supabase,
    lead_id: str,
    external_call_id: Optional[str],
    duration_seconds: int,
    price_cents_per_minute: int = PRICE_CENTS_PER_MINUTE  # kept for signature compatibility (unused for billing)
) -> None:
    """
    Charges the shared domain of the lead's owner for a completed call.
    Now bills in CREDITS: ceil(duration_seconds / 60), min 1 credit.
    """
    # Find the lead's user_id (owner) to resolve domain
    user_id = None
    try:
        lres = supabase.table("leads").select("user_id").eq("id", lead_id).single().execute()
        user_id = (getattr(lres, "data", None) or {}).get("user_id")
    except Exception:
        pass

    domain = email_domain_of(supabase, user_id)
    if not domain:
        return

    dur = int(duration_seconds or 0)
    billed_minutes = max(1, math.ceil(dur / 60))  # 1 credit = 1 minute (round up)
    new_balance = domain_spend_credits(
        supabase,
        domain=domain,
        amount_credits=billed_minutes,
        reason="call_charge",
        meta={
            "lead_id": lead_id,
            "external_call_id": external_call_id,
            "duration_sec": dur,
            "billed_minutes": billed_minutes
        }
    )

    # Optional: usage/audit row. Keep your existing table if you have one.
    try:
        supabase.table("call_usage").insert({
            "external_call_id": external_call_id,
            "lead_id": lead_id,
            "user_id": user_id,
            "domain": domain,
            "duration_sec": dur,
            "billed_minutes": billed_minutes,
            "charged_credits": billed_minutes,
            "status": "completed",
        }).execute()
    except Exception:
        pass

    print(f"[CREDITS] Charged {billed_minutes} credit(s) ({billed_minutes}m) domain={domain} new_balance={new_balance}")
