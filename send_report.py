"""
Send renewal report via email.

Requires environment variables:
    SMTP_USER     — sender email address
    SMTP_PASSWORD — sender app password (Gmail app password, etc.)
"""

import os
import smtplib
import sys
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from config import EMAIL_RECIPIENTS, ENTITY_NAME


def send_report(subject, body_text):
    """Send a plain-text email report to configured recipients."""
    smtp_user = os.environ.get("SMTP_USER")
    smtp_pass = os.environ.get("SMTP_PASSWORD")

    if not smtp_user or not smtp_pass:
        print("WARNING: SMTP_USER / SMTP_PASSWORD not set — skipping email.")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(EMAIL_RECIPIENTS)

    # Plain text body
    msg.attach(MIMEText(body_text, "plain"))

    # Simple HTML version (preformatted to preserve table alignment)
    html_body = f"""\
<html>
<body>
<pre style="font-family: Consolas, monospace; font-size: 13px;">
{body_text}
</pre>
</body>
</html>"""
    msg.attach(MIMEText(html_body, "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, EMAIL_RECIPIENTS, msg.as_string())
        print(f"Email sent to: {', '.join(EMAIL_RECIPIENTS)}")
        return True
    except Exception as e:
        print(f"ERROR sending email: {e}")
        return False


if __name__ == "__main__":
    send_report(
        f"[{ENTITY_NAME}] Test Renewal Report",
        "This is a test email from the agent renewal system.",
    )
