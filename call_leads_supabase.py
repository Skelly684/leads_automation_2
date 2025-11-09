# credits.py
import os
from typing import Optional, Dict, Any

# Pricing (env overrides supported)
PRICE_CENTS_PER_MINUTE = int(os.getenv("PRICE_CENTS_PER_MINUTE", "30"))  # $0.30/min
MIN_RESERVE_CENTS = int(os.getenv("MIN_RESERVE_CENTS", "30"))            # require ≥ 1 min to start a call

def email_domain_of(supabase, user_id: Optional[str]) -> Optional[str]:
    """Resolve a user's email domain for shared balance."""
    if not user_id:
        return None
    try:
        # Try profiles table first (adjust if you store elsewhere)
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

def domain_balance(supabase, domain: str) -> int:
    try:
        r = supabase.table("org_credits").select("balance_cents").eq("domain", domain).single().execute()
        row = getattr(r, "data", None)
        return int(row.get("balance_cents")) if row else 0
    except Exception:
        return 0

def domain_add_credits(supabase, domain: str, amount_cents: int, reason: str = "topup", meta: Dict[str, Any] | None = None) -> int:
    res = supabase.rpc("add_credits", {
        "p_domain": domain,
        "p_amount_cents": amount_cents,
        "p_reason": reason,
        "p_meta": meta or {}
    }).execute()
    data = getattr(res, "data", []) or [{"new_balance": 0}]
    return int(data[0]["new_balance"])

def domain_spend_credits(supabase, domain: str, amount_cents: int, reason: str = "call_charge", meta: Dict[str, Any] | None = None) -> int:
    res = supabase.rpc("spend_credits", {
        "p_domain": domain,
        "p_amount_cents": amount_cents,
        "p_reason": reason,
        "p_meta": meta or {}
    }).execute()
    data = getattr(res, "data", []) or [{"new_balance": 0}]
    return int(data[0]["new_balance"])

def ensure_credit_before_call(
    supabase,
    lead: Dict[str, Any],
    min_reserve_cents: int,
    log_call_cb,     # function(lead_id, status, notes)
    update_lead_cb   # function(lead_id, patch_dict)
) -> bool:
    """
    Return True if there is enough shared domain credit to start a call, else log+mark and return False.
    """
    user_id = lead.get("user_id")
    domain = email_domain_of(supabase, user_id)
    if not domain:
        # If we can't resolve a domain, allow the call (or flip this to block)
        return True
    bal = domain_balance(supabase, domain)
    if bal < min_reserve_cents:
        lead_id = lead.get("id")
        log_call_cb(lead_id, "blocked", f"insufficient_funds domain={domain} bal_cents={bal}")
        update_lead_cb(lead_id, {"last_call_status": "blocked_insufficient_credits"})
        print(f"[CREDITS] Blocked call (insufficient) domain={domain} balance={bal}")
        return False
    return True

def bill_call_completion(
    supabase,
    lead_id: str,
    external_call_id: Optional[str],
    duration_seconds: int,
    price_cents_per_minute: int = PRICE_CENTS_PER_MINUTE
) -> None:
    """
    Charges the shared domain of the lead's owner for a completed call.
    Rounds duration up to the next minute (min 1 minute).
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
    billed_minutes = max(1, (dur + 59) // 60)
    cost_cents = billed_minutes * int(price_cents_per_minute)

    if cost_cents <= 0:
        return

    new_balance = domain_spend_credits(
        supabase,
        domain=domain,
        amount_cents=cost_cents,
        reason="call_charge",
        meta={
            "lead_id": lead_id,
            "external_call_id": external_call_id,
            "duration_sec": dur,
            "billed_minutes": billed_minutes
        }
    )

    # Optional usage row
    try:
        supabase.table("call_usage").insert({
            "external_call_id": external_call_id,
            "lead_id": lead_id,
            "user_id": user_id,
            "domain": domain,
            "duration_sec": dur,
            "billed_minutes": billed_minutes,
            "cost_cents": cost_cents,
            "status": "completed",
        }).execute()
    except Exception:
        pass

    print(f"[CREDITS] Charged {cost_cents}c ({billed_minutes}m) domain={domain} new_balance={new_balance}")
