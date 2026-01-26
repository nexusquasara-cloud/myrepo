from flask import Flask, request
import os
from supabase import create_client

app = Flask(__name__)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")

supabase = create_client(SUPABASE_URL, SUPABASE_ANON_KEY)

@app.route("/", methods=["GET"])
def health_check():
    return "RealesrateCRM Webhook is running", 200


@app.route("/ultramsg/webhook", methods=["POST"])
def ultramsg_webhook():
    data = request.json or {}

    print("UltraMsg webhook received")
    print(f"Incoming payload: {data}")

    sender_info = data.get("sender") or {}
    phone = (
        data.get("from")
        or data.get("phone")
        or sender_info.get("phone")
    )
    print(f"Extracted phone: {phone}")

    name = (
        data.get("notifyName")
        or sender_info.get("name")
        or sender_info.get("pushname")
        or "Unknown"
    )

    if not phone:
        return "No phone", 400

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
