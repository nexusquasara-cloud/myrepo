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

WASENDER_BASE_URL = os.environ.get("WASENDER_BASE_URL", "https://www.wasenderapi.com")
WASENDER_API_KEY = os.environ.get("WASENDER_API_KEY")

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


def _normalize_iraqi_number(phone):
    if not phone:
        return None
    digits = "".join(char for char in str(phone) if char.isdigit())
    if not digits:
        return None
    if digits.startswith("00"):
        digits = digits[2:]
    if digits.startswith("0"):
        digits = "964" + digits[1:]
    if not digits.startswith("964"):
        digits = "964" + digits
    return f"+{digits}"


def _extract_sender_details(payload):
    def _first_dict(value):
        if isinstance(value, dict):
            return value
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    return item
        return None

    search_spaces = []
    if isinstance(payload, dict):
        search_spaces.append(payload)
        for key in ("data", "message", "payload"):
            candidate = _first_dict(payload.get(key))
            if candidate:
                search_spaces.append(candidate)
        data_section = _first_dict(payload.get("data"))
        if data_section:
            for key in ("message", "payload"):
                candidate = _first_dict(data_section.get(key))
                if candidate:
                    search_spaces.append(candidate)
        for key in ("messages", "entries", "contacts"):
            candidate = _first_dict(payload.get(key))
            if candidate:
                search_spaces.append(candidate)

    phone = None
    name = None
    phone_keys = ("from", "sender", "phone", "client_phone", "number")
    name_keys = ("senderName", "name", "client_name", "contact_name")

    for obj in search_spaces:
        for key in phone_keys:
            value = obj.get(key)
            if value:
                phone = value
                break
        if phone:
            break

    for obj in search_spaces:
        for key in name_keys:
            value = obj.get(key)
            if value:
                name = value
                break
        if name:
            break

    return phone, name


def _send_whatsapp_message(message_body):
    if not WASENDER_API_KEY:
        print("[RentalNotifier] WasenderAPI key missing; cannot send message")
        return False

    normalized = _normalize_iraqi_number(OWNER_WHATSAPP_NUMBER)
    if not normalized:
        print("[RentalNotifier] Invalid WhatsApp number; cannot send message")
        return False

    payload = {
        "to": normalized,
        "text": message_body,
    }
    try:
        response = requests.post(
            f"{WASENDER_BASE_URL.rstrip('/')}/api/send-message",
            json=payload,
            headers={
                "Authorization": f"Bearer {WASENDER_API_KEY}",
                "Content-Type": "application/json",
            },
            timeout=15,
        )
    except requests.RequestException as exc:
        print(f"[RentalNotifier] WasenderAPI request failed: {exc}")
        return False

    if response.ok:
        print("[RentalNotifier] WhatsApp notification sent successfully")
        return True
    print(
        "[RentalNotifier] WasenderAPI responded with error "
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


@app.route("/wasender/webhook", methods=["POST"])
def wasender_webhook():
    payload = request.get_json(silent=True) or {}
    event_name = payload.get("event")
    data_section = payload.get("data") or {}

    if event_name == "contacts.upsert":
        print(f"[ContactsUpsert] Full payload: {payload}")
        if not isinstance(data_section, dict):
            print("[ContactsUpsert] Missing data section; skipping")
            return "OK", 200

        contact_id = data_section.get("id")
        if not contact_id:
            print("[ContactsUpsert] Missing contact id; skipping")
            return "OK", 200

        if isinstance(contact_id, str) and contact_id.endswith("@whatsapp.net"):
            contact_id = contact_id[: -len("@whatsapp.net")]

        digits_only = "".join(char for char in str(contact_id) if char.isdigit())
        normalized_phone = _normalize_iraqi_number(digits_only)
        print(f"[ContactsUpsert] Raw id: {contact_id}")
        print(f"[ContactsUpsert] Normalized phone: {normalized_phone}")
        if not normalized_phone:
            print("[ContactsUpsert] Failed to normalize phone; skipping")
            return "OK", 200

        print(f"[ContactsUpsert] Contact detected: {normalized_phone}")
        contact_name = data_section.get("name") or data_section.get("pushName") or "Unknown"

        try:
            existing = (
                supabase.table("clients")
                .select("id")
                .eq("phone", normalized_phone)
                .limit(1)
                .execute()
            )
        except Exception as exc:
            print(f"[ContactsUpsert] Failed to query clients table: {exc}")
            return "OK", 200

        try:
            if existing.data:
                client_id = existing.data[0].get("id")
                supabase.table("clients").update(
                    {"name": contact_name}
                ).eq("id", client_id).execute()
                print(f"[ContactsUpsert] Client updated: {normalized_phone}")
            else:
                supabase.table("clients").insert(
                    {"phone": normalized_phone, "name": contact_name}
                ).execute()
                print(f"[ContactsUpsert] Client inserted: {normalized_phone}")
        except Exception as exc:
            print(f"[ContactsUpsert] Failed to upsert client: {exc}")

        return "OK", 200

    if event_name == "messages.upsert":
        print("[MessageUpsert] Full payload received")
        print(f"[MessageUpsert] Payload: {payload}")
        if not isinstance(data_section, dict):
            print("[MessageUpsert] Missing data section; skipping")
            return "OK", 200

        messages = data_section.get("messages") or []
        if isinstance(messages, dict):
            message = messages
        elif isinstance(messages, list) and messages:
            candidate = messages[0]
            message = candidate if isinstance(candidate, dict) else {}
        else:
            message = {}

        if not message:
            print("[MessageUpsert] No message payload found; skipping")
            return "OK", 200

        if message.get("fromMe"):
            print("[MessageUpsert] Outbound message detected; skipping client insert")
            return "OK", 200

        extraction_attempts = [
            ("data.messages[0].senderPn", message.get("senderPn")),
            (
                "data.messages[0].key.remoteJid",
                (
                    message.get("key", {}).get("remoteJid")
                    if isinstance(message.get("key"), dict)
                    else None
                ),
            ),
            ("data.from", data_section.get("from")),
            ("data.chatId", data_section.get("chatId")),
        ]

        sender_phone_raw = None
        source_used = None
        for label, value in extraction_attempts:
            print(f"[MessageUpsert] Attempting {label}: {value}")
            if value:
                sender_phone_raw = value
                source_used = label
                break

        if sender_phone_raw is None:
            print("[MessageUpsert] All extraction attempts failed; skipping")
            return "OK", 200

        if source_used == "data.messages[0].senderPn" and isinstance(
            sender_phone_raw, str
        ):
            sender_phone_raw = sender_phone_raw.replace("@s.whatsapp.net", "")

        digits_only = "".join(char for char in str(sender_phone_raw) if char.isdigit())
        normalized_phone = _normalize_iraqi_number(digits_only)
        print(
            f"[MessageUpsert] Source={source_used} raw={sender_phone_raw} "
            f"normalized={normalized_phone}"
        )
        if not normalized_phone:
            print("[MessageUpsert] Normalization failed; skipping")
            return "OK", 200

        try:
            existing = (
                supabase.table("clients")
                .select("id")
                .eq("phone", normalized_phone)
                .limit(1)
                .execute()
            )
        except Exception as exc:
            print(f"[MessageUpsert] Failed to query clients table: {exc}")
            return "OK", 200

        if existing.data:
            print("[MessageUpsert] Client already exists; no insert")
            return "OK", 200

        try:
            supabase.table("clients").insert(
                {"phone": normalized_phone, "name": "Unknown", "source": "first_message"}
            ).execute()
            print(f"[MessageUpsert] Client created on first message: {normalized_phone}")
        except Exception as exc:
            print(f"[MessageUpsert] Failed to insert client: {exc}")

        return "OK", 200

    return "OK", 200


if __name__ == "__main__":
    app.run()
