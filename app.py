import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, request, jsonify
from supabase import create_client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

WASENDER_BASE_URL = (
    os.environ.get("WASENDER_BASE_URL", "https://www.wasenderapi.com") or ""
).rstrip("/")
WASENDER_SEND_PATH = (os.environ.get("WASENDER_SEND_PATH") or "/api/sendMessage").lstrip(
    "/"
)
WASENDER_AUTH_MODE = (os.environ.get("WASENDER_AUTH_MODE") or "token").lower()
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY") or ""
WASENDER_TOKEN = os.environ.get("WASENDER_TOKEN") or ""
WASENDER_DEVICE_ID = os.environ.get("WASENDER_DEVICE_ID") or ""

OWNER_WHATSAPP_NUMBER = "9647722602749"
NOTIFICATION_INTERVAL_SECONDS = 60  # TEST MODE: notification check every 1 minute
NOTIFICATION_LOOKAHEAD_DAYS = 7
STATUS_LABELS = {"overdue": "Overdue", "expiring": "Expiring Soon"}


def fetch_rentals_from_supabase():
    now = datetime.now(timezone.utc)
    upcoming_limit = now + timedelta(days=NOTIFICATION_LOOKAHEAD_DAYS)
    print(
        "[RentalNotifier] Fetching rentals. "
        f"Now={now.isoformat()} UpcomingLimit={upcoming_limit.isoformat()}"
    )
    try:
        overdue_resp = (
            supabase.table("rentals")
            .select("*")
            .lt("end_datetime", now.isoformat())
            .eq("paid", False)
            .execute()
        )
        expiring_resp = (
            supabase.table("rentals")
            .select("*")
            .gte("end_datetime", now.isoformat())
            .lte("end_datetime", upcoming_limit.isoformat())
            .eq("paid", False)
            .execute()
        )
    except Exception as exc:
        print(f"[RentalNotifier] Error fetching rentals from Supabase: {exc}")
        return {"overdue": [], "expiring": []}

    overdue = overdue_resp.data or []
    expiring = expiring_resp.data or []

    print(
        "[RentalNotifier] Rentals fetched "
        f"(overdue={len(overdue)} expiring={len(expiring)})"
    )
    return {"overdue": overdue, "expiring": expiring}


def notification_already_sent(rental_id, notification_type, reference_time=None):
    if rental_id is None:
        return True
    if reference_time is None:
        reference_time = datetime.now(timezone.utc)
    day_start = reference_time.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    day_end = day_start + timedelta(days=1)

    try:
        response = (
            supabase.table("notifications_log")
            .select("sent_at")
            .eq("rental_id", rental_id)
            .eq("notification_type", notification_type)
            .gte("sent_at", day_start.isoformat())
            .lt("sent_at", day_end.isoformat())
            .execute()
        )
    except Exception as exc:
        print(
            "[RentalNotifier] Failed to check notifications_log "
            f"for rental {rental_id}: {exc}"
        )
        return False

    exists = bool(response.data)
    if exists:
        print(
            f"[RentalNotifier] Notification already sent today "
            f"for rental {rental_id} ({notification_type})"
        )
    return exists


def log_notification(rental_id, notification_type):
    try:
        supabase.table("notifications_log").insert(
            {
                "rental_id": rental_id,
                "notification_type": notification_type,
                "sent_at": datetime.now(timezone.utc).isoformat(),
            }
        ).execute()
        print(
            f"[RentalNotifier] Logged notification for rental {rental_id} "
            f"({notification_type})"
        )
    except Exception as exc:
        print(
            "[RentalNotifier] Failed to log notification "
            f"for rental {rental_id}: {exc}"
        )


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, str):
        normalized = value.replace("Z", "+00:00")
    else:
        normalized = value
    try:
        return datetime.fromisoformat(normalized)
    except ValueError:
        print(f"[RentalNotifier] Failed to parse datetime: {value}")
        return None


def _format_client_identity(rental):
    return (
        rental.get("client_name")
        or rental.get("client_phone")
        or (f"Client #{rental.get('client_id')}" if rental.get("client_id") else "Client")
    )


def _build_message(rental, status_label, end_dt):
    client_identity = _format_client_identity(rental)
    property_name = rental.get("property_name") or "Rental property"
    formatted_end = end_dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    status_text = STATUS_LABELS.get(status_label.lower(), status_label.title())
    return (
        f"{status_text} rental alert:\n"
        f"Client: {client_identity}\n"
        f"Property: {property_name}\n"
        f"Ends: {formatted_end}"
    )


def _send_whatsapp_message(message_body):
    if not WASENDER_BASE_URL:
        print("[RentalNotifier] Wasender base URL missing; cannot send message")
        return False

    url = f"{WASENDER_BASE_URL}/{WASENDER_SEND_PATH}"
    headers = {}
    payload = {}

    if WASENDER_AUTH_MODE == "bearer":
        if not WASENDER_API_KEY:
            print("[RentalNotifier] Wasender API key missing; cannot send message")
            return False
        headers["Authorization"] = f"Bearer {WASENDER_API_KEY}"
        payload = {"to": OWNER_WHATSAPP_NUMBER, "text": message_body}
    else:
        token = WASENDER_TOKEN or WASENDER_API_KEY
        if not token:
            print("[RentalNotifier] Wasender token missing; cannot send message")
            return False
        payload = {
            "token": token,
            "to": OWNER_WHATSAPP_NUMBER,
            "message": message_body,
        }
        if WASENDER_DEVICE_ID:
            payload["device_id"] = WASENDER_DEVICE_ID

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=15)
    except requests.RequestException as exc:
        print(f"[RentalNotifier] Wasender request failed: {exc}")
        return False

    if response.ok:
        print("[RentalNotifier] WhatsApp notification sent successfully via Wasender")
        return True
    print(
        "[RentalNotifier] Wasender API responded with error "
        f"(status={response.status_code} body={response.text})"
    )
    return False


def _process_rental_list(rentals, notification_type):
    print(
        f"[RentalNotifier] Processing {len(rentals)} rentals "
        f"for notification_type={notification_type}"
    )
    sent_count = 0
    for rental in rentals:
        rental_id = rental.get("id")
        end_dt = _parse_datetime(rental.get("end_datetime"))
        if rental_id is None or not end_dt:
            print(
                f"[RentalNotifier] Skipping rental due to missing data "
                f"(id={rental_id} end_dt={end_dt})"
            )
            continue

        if notification_already_sent(rental_id, notification_type):
            continue

        message = _build_message(rental, notification_type, end_dt)
        if _send_whatsapp_message(message):
            sent_count += 1
            log_notification(rental_id, notification_type)
        else:
            print(
                f"[RentalNotifier] Failed to send WhatsApp message for rental {rental_id}"
            )

    print(
        f"[RentalNotifier] Finished processing {notification_type} rentals "
        f"(sent={sent_count})"
    )


def check_and_send_notifications():
    print(
        "[RentalNotifier] Starting notification cycle "
        f"at {datetime.now(timezone.utc).isoformat()}"
    )
    rentals = fetch_rentals_from_supabase()
    _process_rental_list(rentals.get("overdue", []), "overdue")
    _process_rental_list(rentals.get("expiring", []), "expiring")


def _notification_worker():
    while True:
        try:
            check_and_send_notifications()
        except Exception as exc:
            print(f"[RentalNotifier] Unexpected error during notification cycle: {exc}")
        time.sleep(NOTIFICATION_INTERVAL_SECONDS)


def start_rental_notification_scheduler():
    print(
        "[RentalNotifier] Scheduler starting "
        f"with interval {NOTIFICATION_INTERVAL_SECONDS} seconds"
    )
    worker = threading.Thread(
        target=_notification_worker, name="rental-notifier", daemon=True
    )
    worker.start()


start_rental_notification_scheduler()


@app.route("/wasender/webhook", methods=["POST"])
def wasender_webhook():
    print("[WasenderWebhook] ===== NEW REQUEST =====")

    payload = request.get_json(silent=True) or {}
    print("[WasenderWebhook] Raw payload:", payload)

    event = str(payload.get("event") or "")
    print("[WasenderWebhook] Event:", event)

    data = payload.get("data") or {}
    messages = data.get("messages") or {}
    if isinstance(messages, list):
        messages = messages[0] if messages else {}
    if not isinstance(messages, dict):
        messages = {}

    key_block = messages.get("key") or {}
    if isinstance(key_block, list):
        key_block = key_block[0] if key_block else {}
    if not isinstance(key_block, dict):
        key_block = {}

    remote_jid = key_block.get("remoteJid")
    sender_pn = messages.get("senderPn")
    participant = messages.get("participant")

    print("[WasenderWebhook] Extracted remoteJid:", remote_jid)
    print("[WasenderWebhook] Extracted senderPn:", sender_pn)
    print("[WasenderWebhook] Extracted participant:", participant)

    response_payload = {"received": True}
    normalized_phone = None

    if sender_pn:
        cleaned_sender = (
            str(sender_pn).replace("@s.whatsapp.net", "").replace("@c.us", "")
        )
        digits_sender = "".join(filter(str.isdigit, cleaned_sender))
        normalized_phone = digits_sender
        print("[WasenderWebhook] Phone extracted from senderPn:", normalized_phone)
        if not (
            normalized_phone.startswith("9647") and len(normalized_phone) == 13
        ):
            print("[WasenderWebhook] Invalid phone from senderPn:", normalized_phone)
            return jsonify({"status": "ignored"}), 200
    else:
        print("[WasenderWebhook] senderPn missing, using fallback extraction")
        phone_candidate = remote_jid or participant
        if not phone_candidate:
            print("[WasenderWebhook] NO PHONE FOUND - missing remoteJid/participant")
            return jsonify({"status": "ignored"}), 200

        phone_str = str(phone_candidate)
        print("[WasenderWebhook] Raw phone value:", phone_str)
        digits_only = "".join(filter(str.isdigit, phone_str))
        print("[WasenderWebhook] Digits-only phone:", digits_only)

        local_part = ""
        if digits_only.startswith("964"):
            local_part = digits_only[3:]
        else:
            digits_trimmed = digits_only.lstrip("0")
            local_part = digits_trimmed[-9:] if len(digits_trimmed) >= 9 else ""

        if len(local_part) == 9:
            normalized_phone = "964" + local_part
        else:
            normalized_phone = ""

        if len(normalized_phone) != 12 or not normalized_phone.startswith("964"):
            print(
                "[WasenderWebhook] Invalid phone after fallback normalization:",
                normalized_phone or digits_only,
            )
            return jsonify({"status": "ignored"}), 200

    print("[WasenderWebhook] Normalized phone:", normalized_phone)
    print("[WasenderWebhook] Data section:", data)
    name = data.get("pushName") or "Unknown"

    try:
        print("[WasenderWebhook] Checking client existence in Supabase")
        existing = (
            supabase.table("clients")
            .select("id")
            .eq("phone", normalized_phone)
            .execute()
        )
    except Exception as exc:
        print("[WasenderWebhook] Supabase query error:", exc)
        return jsonify(response_payload), 200

    if existing.data:
        print("[WasenderWebhook] Client already exists:", normalized_phone)
        return jsonify(response_payload), 200

    try:
        supabase.table("clients").insert({
            "phone": normalized_phone,
            "name": name
        }).execute()
        print("[WasenderWebhook] Client inserted:", normalized_phone)
    except Exception as exc:
        print("[WasenderWebhook] Client insert failed:", exc)

    return jsonify(response_payload), 200


@app.route("/", methods=["GET"])
def health_check():
    return "RealesrateCRM Webhook is running", 200


if __name__ == "__main__":
    app.run()
