import phonenumbers
from phonenumbers import timezone as ph_timezone
from datetime import datetime
import pytz
import requests
import time
from supabase import create_client, Client

# ---- Credentials/Settings ----
VAPI_API_KEY = 'd7b7ebcb-156a-44b7-9763-ea90defd5c48'
VAPI_ASSISTANT_ID = '90dcabfe-0201-4a0e-9325-af9ae40c9352'
VAPI_PHONE_NUMBER_ID = '88839f31-726a-4e63-8f44-61a30ec8f63d'
SUPABASE_URL = 'https://qjmzyoxzxjkvrvohksjh.supabase.co'
SUPABASE_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6InFqbXp5b3h6eGprdnJ2b2hrc2poIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjE1Nzg0MDMsImV4cCI6MjA3NzE1NDQwM30.YwPa9P4rwB9Z3jYcaLVLsM3eepvoTM3wHvlxSJr476U'  # <<<<<<<< FILL THIS IN <<<<<<<<

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
CALL_WINDOW_START = 9
CALL_WINDOW_END = 18

def get_local_hour(phone):
    try:
        number = phonenumbers.parse(str(phone), None)
        timezones = ph_timezone.time_zones_for_number(number)
        if not timezones:
            return None
        tz = pytz.timezone(timezones[0])
        now_local = datetime.now(tz)
        return now_local.hour
    except Exception as e:
        print(f"Timezone error for {phone}: {e}")
        return None

def is_valid_phone(phone):
    try:
        number = phonenumbers.parse(str(phone), None)
        return phonenumbers.is_possible_number(number) and phonenumbers.is_valid_number(number)
    except:
        return False

def make_vapi_call(phone, lead):
    url = "https://api.vapi.ai/v1/call/phone"
    payload = {
        "assistant": VAPI_ASSISTANT_ID,
        "phone": {
            "number": phone,
            "phoneNumberId": VAPI_PHONE_NUMBER_ID
        },
        "metadata": {
            "lead_name": lead.get("first_name") or lead.get("name") or "",
            "job_title": lead.get("job_title") or "",
            "company": lead.get("company_name") or lead.get("company") or "",
        }
    }
    headers = {
        "Authorization": f"Bearer {VAPI_API_KEY}",
        "Content-Type": "application/json"
    }
    resp = requests.post(url, json=payload, headers=headers)
    print(f"Call to {phone} ({payload['metadata']['lead_name']}): {resp.status_code}, {resp.text}")
    return resp.status_code, resp.text

def fetch_accepted_leads():
    resp = supabase.table('leads').select("*").eq('status', 'accepted').execute()
    return resp.data

def update_lead_status(lead_id, status, contact_time=None):
    update = {'status': status}
    if contact_time:
        update['sent_for_contact_at'] = contact_time
    supabase.table('leads').update(update).eq('id', lead_id).execute()

def log_call(lead_id, call_status, notes=""):
    # Call duration and notes are optional for now
    supabase.table('call_logs').insert({
        'lead_id': lead_id,
        'call_status': call_status,
        'notes': notes,
        # 'call_duration': call_duration  # You can add this if available
    }).execute()

def call_all_leads():
    leads = fetch_accepted_leads()
    for lead in leads:
        phone = (
            lead.get("phone") or ""
        )
        if not is_valid_phone(phone):
            print(f"Skipping invalid phone: {phone}")
            continue

        local_hour = get_local_hour(phone)
        if local_hour is None or not (CALL_WINDOW_START <= local_hour < CALL_WINDOW_END):
            print(f"Skipping {phone}: Not in allowed call window ({local_hour})")
            continue

        status_code, msg = make_vapi_call(phone, lead)
        now = datetime.utcnow().isoformat()
        # Log call attempt in call_logs table
        log_call(lead['id'], f"{status_code}", msg)
        # Update lead as contacted
        update_lead_status(lead['id'], 'contacted', contact_time=now)
        time.sleep(1)  # Respect rate limits

if __name__ == "__main__":
    call_all_leads()
