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

    phone = data.get("from")
    name = data.get("notifyName") or "WhatsApp User"

    if not phone:
        return "No phone", 400

    # تحقق هل العميل موجود
    existing = supabase.table("clients").select("id").eq("phone", phone).execute()

    if not existing.data:
        supabase.table("clients").insert({
            "phone": phone,
            "name": name
        }).execute()

    return "OK", 200


if __name__ == "__main__":
    app.run()
