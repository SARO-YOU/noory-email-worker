"""
Noory Shop - Email Worker
Runs on Railway.app (free tier — no SMTP firewall)
Polls the database every 30 seconds and sends pending emails via Gmail SMTP
"""

import os
import time
import smtplib
import psycopg2
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime

DATABASE_URL = os.environ.get('DATABASE_URL')
EMAIL_USER   = os.environ.get('EMAIL_USER', 'shopnoorey@gmail.com')
EMAIL_PASS   = os.environ.get('EMAIL_PASS', 'izwo nuqj ksxp bkty')
EMAIL_FROM   = os.environ.get('EMAIL_FROM', 'shopnoorey@gmail.com')
POLL_SECONDS = int(os.environ.get('POLL_SECONDS', 30))


def get_conn():
    return psycopg2.connect(DATABASE_URL)


def send_email(to_email, subject, body_text, body_html):
    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From']    = f'Noory Shop <{EMAIL_FROM}>'
    msg['To']      = to_email
    msg.attach(MIMEText(body_text, 'plain'))
    if body_html:
        msg.attach(MIMEText(body_html, 'html'))

    with smtplib.SMTP('smtp.gmail.com', 587) as server:
        server.ehlo()
        server.starttls()
        server.login(EMAIL_USER, EMAIL_PASS)
        server.sendmail(EMAIL_FROM, to_email, msg.as_string())


def process_pending_emails():
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()

        # Fetch up to 10 pending emails
        cur.execute("""
            SELECT id, to_email, subject, body_text, body_html
            FROM email_queue
            WHERE status = 'pending'
            ORDER BY created_at ASC
            LIMIT 10
        """)
        rows = cur.fetchall()

        if not rows:
            return

        print(f"[{datetime.utcnow().strftime('%H:%M:%S')}] Found {len(rows)} pending email(s)")

        for row in rows:
            email_id, to_email, subject, body_text, body_html = row
            try:
                send_email(to_email, subject, body_text or '', body_html)
                cur.execute("""
                    UPDATE email_queue
                    SET status = 'sent', sent_at = NOW(), error = NULL
                    WHERE id = %s
                """, (email_id,))
                conn.commit()
                print(f"  ✅ Sent to {to_email}: {subject}")
            except Exception as e:
                cur.execute("""
                    UPDATE email_queue
                    SET status = 'failed', error = %s, attempts = attempts + 1
                    WHERE id = %s
                """, (str(e)[:500], email_id))
                conn.commit()
                print(f"  ❌ Failed to {to_email}: {e}")

    except Exception as e:
        print(f"DB error: {e}")
    finally:
        if conn:
            conn.close()


def ensure_table_exists():
    """Create email_queue table if it doesn't exist yet"""
    conn = None
    try:
        conn = get_conn()
        cur  = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS email_queue (
                id         SERIAL PRIMARY KEY,
                to_email   TEXT NOT NULL,
                subject    TEXT NOT NULL,
                body_text  TEXT,
                body_html  TEXT,
                status     TEXT DEFAULT 'pending',
                attempts   INT  DEFAULT 0,
                error      TEXT,
                created_at TIMESTAMP DEFAULT NOW(),
                sent_at    TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ email_queue table ready")
    except Exception as e:
        print(f"Table creation error: {e}")
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    print("🚀 Noory Shop Email Worker starting...")
    print(f"   Polling every {POLL_SECONDS}s")
    print(f"   Sending from: {EMAIL_FROM}")

    ensure_table_exists()

    while True:
        try:
            process_pending_emails()
        except Exception as e:
            print(f"Worker error: {e}")
        time.sleep(POLL_SECONDS)
