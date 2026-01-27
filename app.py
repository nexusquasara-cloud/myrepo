import os
import threading
import time
from datetime import datetime, timedelta, timezone

import requests
from flask import Flask, request
from supabase import create_client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

ULTRAMSG_INSTANCE_ID = os.environ.get("ULTRAMSG_INSTANCE_ID")
ULTRAMSG_TOKEN = os.environ.get("ULTRAMSG_TOKEN")
ULTRAMSG_CHAT_URL = os.environ.get("ULTRAMSG_CHAT_URL")
if not ULTRAMSG_CHAT_URL and ULTRAMSG_INSTANCE_ID:
    ULTRAMSG_CHAT_URL = (
        f"https://api.ultramsg.com/{ULTRAMSG_INSTANCE_ID}/messages/chat"
    )

OWNER_WHATSAPP_NUMBER = "+15551234567"
NOTIFICATION_INTERVAL_SECONDS = 3600
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
    if not ULTRAMSG_CHAT_URL or not ULTRAMSG_TOKEN:
        print("[RentalNotifier] UltraMsg credentials missing; cannot send message")
        return False

    payload = {
        "token": ULTRAMSG_TOKEN,
        "to": OWNER_WHATSAPP_NUMBER,
        "body": message_body,
    }
    try:
        response = requests.post(ULTRAMSG_CHAT_URL, data=payload, timeout=15)
    except requests.RequestException as exc:
        print(f"[RentalNotifier] UltraMsg request failed: {exc}")
        return False

    if response.ok:
        print("[RentalNotifier] WhatsApp notification sent successfully")
        return True
    print(
        "[RentalNotifier] UltraMsg API responded with error "
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

@app.route("/", methods=["GET"])
def health_check():
    return "RealesrateCRM Webhook is running", 200


@app.route("/ultramsg/webhook", methods=["POST"])
def ultramsg_webhook():
    data = request.json or {}

    print("UltraMsg webhook received")
    print(f"Incoming payload: {data}")

    payload_data = data.get("data") or {}
    message_info = data.get("message") or {}
    sender_info = data.get("sender") or {}
    phone = (
        payload_data.get("from")
        or payload_data.get("chatId")
        or data.get("author")
        or message_info.get("from")
    )
    if isinstance(phone, str):
        phone = phone.replace("@c.us", "")
    print(f"Extracted phone: {phone}")

    name = (
        data.get("notifyName")
        or sender_info.get("name")
        or sender_info.get("pushname")
        or "Unknown"
    )

    if not phone:
        print("Phone number not found in payload")
        return "No phone", 200

    # تحقق هل العميل موجود
    existing = supabase.table("clients").select("id").eq("phone", phone).execute()

    if not existing.data:
        print("Inserting client into Supabase")
        try:
            supabase.table("clients").insert({
                "phone": phone,
                "name": name
            }).execute()
        except Exception as exc:
            print(f"Supabase insertion failed: {exc}")
            raise

    return "OK", 200


if __name__ == "__main__":
    app.run()
