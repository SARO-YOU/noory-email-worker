import os
import time
import json
import psycopg2
import requests
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')
BREVO_API_KEY = os.environ.get('BREVO_API_KEY')
EMAIL_FROM = os.environ.get('EMAIL_FROM', 'shopnoorey@gmail.com')
FROM_NAME = os.environ.get('FROM_NAME', 'Noory Shop')
POLL_SECONDS = int(os.environ.get('POLL_SECONDS', 30))

print("🚀 Noory Shop Email Worker starting...")
print(f"   Polling every {POLL_SECONDS}s")
print(f"   Sending from: {EMAIL_FROM}")


def create_table():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS email_queue (
                id SERIAL PRIMARY KEY,
                to_email VARCHAR(255) NOT NULL,
                subject VARCHAR(500) NOT NULL,
                body_text TEXT,
                body_html TEXT,
                status VARCHAR(20) DEFAULT 'pending',
                attempts INTEGER DEFAULT 0,
                error TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                sent_at TIMESTAMP
            )
        """)
        conn.commit()
        cur.close()
        conn.close()
        print("✅ email_queue table ready")
    except Exception as e:
        print(f"Table creation error: {e}")


def send_via_brevo(to_email, subject, body_html, body_text):
    url = "https://api.brevo.com/v3/smtp/email"
    headers = {
        "accept": "application/json",
        "api-key": BREVO_API_KEY,
        "content-type": "application/json"
    }
    payload = {
        "sender": {"name": FROM_NAME, "email": EMAIL_FROM},
        "to": [{"email": to_email}],
        "subject": subject,
        "htmlContent": body_html if body_html else f"<p>{body_text}</p>",
        "textContent": body_text if body_text else ""
    }
    response = requests.post(url, headers=headers, json=payload)
    if response.status_code in (200, 201):
        return True
    else:
        raise Exception(f"Brevo error {response.status_code}: {response.text}")


def process_emails():
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()

        cur.execute("""
            SELECT id, to_email, subject, body_text, body_html
            FROM email_queue
            WHERE status = 'pending' AND attempts < 3
            ORDER BY created_at ASC
            LIMIT 10
        """)
        rows = cur.fetchall()

        if rows:
            print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Found {len(rows)} pending email(s)")

        for row in rows:
            email_id, to_email, subject, body_text, body_html = row
            try:
                send_via_brevo(to_email, subject, body_html, body_text)
                cur.execute("""
                    UPDATE email_queue
                    SET status='sent', sent_at=NOW()
                    WHERE id=%s
                """, (email_id,))
                conn.commit()
                print(f"  ✅ Sent to {to_email}: {subject}")
            except Exception as e:
                cur.execute("""
                    UPDATE email_queue
                    SET attempts=attempts+1, error=%s,
                        status=CASE WHEN attempts+1 >= 3 THEN 'failed' ELSE 'pending' END
                    WHERE id=%s
                """, (str(e), email_id))
                conn.commit()
                print(f"  ❌ Failed to {to_email}: {e}")

        cur.close()
        conn.close()

    except Exception as e:
        print(f"DB error: {e}")


create_table()

while True:
    process_emails()
    time.sleep(POLL_SECONDS)