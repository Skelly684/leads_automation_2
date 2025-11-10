# stripe_credits.py
import os
import json
import math
import stripe
from typing import Optional, Dict, Any

from fastapi import APIRouter, HTTPException, Request, Body
from supabase import create_client

# ---------- Environment ----------
stripe.api_key = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
STRIPE_CURRENCY = os.getenv("STRIPE_CURRENCY", "usd")
PRICE_CENTS_PER_CREDIT = int(os.getenv("PRICE_CENTS_PER_CREDIT", "30"))

# Supabase (Lovable Cloud) – use service role for writes/billing
SUPABASE_URL = os.getenv("SUPABASE_URL", "")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY", os.getenv("SUPABASE_KEY", ""))  # fallback
supabase_sr = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

router = APIRouter(tags=["credits"])

# ---------- Helpers ----------
def _compute_credits(amount_cents: int, price_cents_per_credit: int) -> int:
    if price_cents_per_credit <= 0:
        return 0
    return amount_cents // price_cents_per_credit

def _credit_domain(domain: str, credits: int, meta: Dict[str, Any]) -> None:
    """
    Prefer RPC add_credits(domain, credits, reason, meta).
    Fallback to direct update + optional ledger if RPC missing.
    """
    domain = (domain or "").strip().lower()
    if not domain or credits <= 0:
        return

    # Try RPC first
    try:
        res = supabase_sr.rpc("add_credits", {
            "p_domain": domain,
            "p_amount_credits": int(credits),
            "p_reason": "topup",
            "p_meta": meta or {},
        }).execute()
        print(f"[CREDITS][TOPUP] domain={domain} +{credits} RPC -> {getattr(res, 'data', None)}")
        return
    except Exception as e:
        print("[CREDITS][TOPUP] RPC failed; using fallback:", e)

    # Fallback path
    try:
        # Ensure row exists
        supabase_sr.table("domain_credits").upsert({"domain": domain, "balance_credits": 0}).execute()

        # Get current
        cur = supabase_sr.table("domain_credits").select("balance_credits").eq("domain", domain).single().execute()
        row = getattr(cur, "data", None) or {}
        new_bal = int(row.get("balance_credits") or 0) + int(credits)

        # Update
        supabase_sr.table("domain_credits").update({"balance_credits": new_bal}).eq("domain", domain).execute()

        # Optional ledger
        try:
            supabase_sr.table("credits_ledger").insert({
                "domain": domain,
                "delta_credits": int(credits),
                "reason": "topup",
                "meta": meta or {},
            }).execute()
        except Exception:
            pass

        print(f"[CREDITS][TOPUP][FALLBACK] domain={domain} +{credits} -> {new_bal}")
    except Exception as e2:
        print("[CREDITS][TOPUP][ERROR]", e2)

# ---------- Routes ----------
@router.post("/api/credits/checkout")
def create_checkout_session(payload: dict = Body(...)):
    """
    Body:
      {
        "amount": 1500,            # cents (required)
        "domain": "leadm8.io",     # who to credit (required)
        "returnTo": "https://app.yoursite.com/settings/billing"  # optional
      }
    Returns: {"url": "<stripe checkout url>"}
    """
    try:
        amount = int(payload.get("amount") or 0)
    except Exception:
        amount = 0

    domain = (payload.get("domain") or "").strip().lower()
    if amount <= 0 or not domain:
        raise HTTPException(status_code=400, detail="amount (cents) > 0 and domain are required")

    price_cents_per_credit = int(os.getenv("PRICE_CENTS_PER_CREDIT", str(PRICE_CENTS_PER_CREDIT)))
    est_credits = _compute_credits(amount, price_cents_per_credit)

    success_url = (payload.get("returnTo")
                   or "https://app.yoursite.com/settings/billing?status=success")
    cancel_url = "https://app.yoursite.com/settings/billing?status=cancel"

    try:
        session = stripe.checkout.Session.create(
            mode="payment",
            payment_method_types=["card"],
            line_items=[{
                "price_data": {
                    "currency": STRIPE_CURRENCY,
                    "product_data": {"name": "AI Caller Credits"},
                    "unit_amount": amount,  # total amount in cents (dynamic)
                },
                "quantity": 1,
            }],
            metadata={
                "domain": domain,
                "amount_cents": str(amount),
                "price_cents_per_credit": str(price_cents_per_credit),
                "est_credits": str(est_credits),
                "reason": "topup",
            },
            success_url=success_url,
            cancel_url=cancel_url,
        )
        return {"url": session.url}
    except Exception as e:
        print("[STRIPE][CHECKOUT][ERROR]", e)
        raise HTTPException(status_code=500, detail="Failed to create checkout session")

@router.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe webhook receiver.
    Handles: checkout.session.completed → add credits to domain.
    """
    payload = await request.body()
    sig = request.headers.get("stripe-signature")
    if not STRIPE_WEBHOOK_SECRET:
        print("[STRIPE][WEBHOOK] Missing STRIPE_WEBHOOK_SECRET")
        raise HTTPException(status_code=500, detail="Webhook not configured")

    try:
        event = stripe.Webhook.construct_event(
            payload=payload, sig_header=sig, secret=STRIPE_WEBHOOK_SECRET
        )
    except Exception as e:
        print("[STRIPE][WEBHOOK] verify fail:", e)
        raise HTTPException(status_code=400, detail=f"Webhook error: {e}")

    etype = event.get("type")
    if etype == "checkout.session.completed":
        session = event["data"]["object"]
        meta = session.get("metadata") or {}
        domain = (meta.get("domain") or "").strip().lower()

        try:
            amount_cents = int(meta.get("amount_cents") or 0)
        except Exception:
            amount_cents = 0
        try:
            price_cents_per_credit = int(meta.get("price_cents_per_credit") or PRICE_CENTS_PER_CREDIT)
        except Exception:
            price_cents_per_credit = PRICE_CENTS_PER_CREDIT

        credits = _compute_credits(amount_cents, price_cents_per_credit)
        print(f"[STRIPE][WEBHOOK] session={session.get('id')} domain={domain} amount_cents={amount_cents} -> credits={credits}")

        if domain and credits > 0:
            _credit_domain(domain, credits, {
                "stripe_session": session.get("id"),
                "amount_cents": amount_cents
            })

    # You can optionally handle payment_intent.succeeded as a backup
    return {"ok": True}
