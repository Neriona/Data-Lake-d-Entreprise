#!/usr/bin/env python3
"""
Alert notification system for Data Lake Enterprise
Supports: File-based alerts, Slack, Email
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

# Create alerts directory
ALERTS_DIR = "/opt/airflow/alerts"
Path(ALERTS_DIR).mkdir(parents=True, exist_ok=True)

def create_file_alert(alert_type, entity, zone, message, severity="WARNING"):
    """Create file-based alert"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    alert_data = {
        "timestamp": datetime.now().isoformat(),
        "type": alert_type,
        "entity": entity,
        "zone": zone,
        "severity": severity,
        "message": message,
    }
    
    filename = f"{ALERTS_DIR}/alert_{severity}_{entity}_{timestamp}.json"
    
    with open(filename, 'w') as f:
        json.dump(alert_data, f, indent=2)
    
    print(f"✓ Alert saved: {filename}")
    return filename

def send_slack_alert(webhook_url, alert_type, entity, zone, message, severity="WARNING"):
    """Send Slack notification (requires webhook URL)"""
    try:
        import requests
        
        color_map = {
            "CRITICAL": "#FF0000",
            "WARNING": "#FFA500",
            "INFO": "#0000FF"
        }
        
        payload = {
            "attachments": [
                {
                    "color": color_map.get(severity, "#808080"),
                    "title": f"🚨 Data Lake Alert - {severity}",
                    "fields": [
                        {"title": "Type", "value": alert_type, "short": True},
                        {"title": "Entity", "value": entity, "short": True},
                        {"title": "Zone", "value": zone, "short": True},
                        {"title": "Severity", "value": severity, "short": True},
                        {"title": "Message", "value": message, "short": False},
                        {"title": "Timestamp", "value": datetime.now().isoformat(), "short": False}
                    ]
                }
            ]
        }
        
        response = requests.post(webhook_url, json=payload)
        if response.status_code == 200:
            print(f"✓ Slack alert sent successfully")
        else:
            print(f"✗ Failed to send Slack alert: {response.text}")
    except Exception as e:
        print(f"⚠ Slack alert failed (webhook not configured): {str(e)}")

def send_email_alert(recipient, alert_type, entity, zone, message, severity="WARNING"):
    """Send email notification (requires SMTP configuration)"""
    try:
        import smtplib
        from email.mime.text import MIMEText
        from email.mime.multipart import MIMEMultipart
        
        # Get SMTP configuration from environment
        smtp_server = os.getenv("SMTP_SERVER", "localhost")
        smtp_port = int(os.getenv("SMTP_PORT", "587"))
        sender_email = os.getenv("SENDER_EMAIL", "datalake@company.com")
        sender_password = os.getenv("SENDER_PASSWORD", "")
        
        # Create email
        message_obj = MIMEMultipart("alternative")
        message_obj["Subject"] = f"[{severity}] Data Lake Alert - {entity}"
        message_obj["From"] = sender_email
        message_obj["To"] = recipient
        
        text = f"""
Data Lake Enterprise Alert

Alert Type: {alert_type}
Entity: {entity}
Zone: {zone}
Severity: {severity}
Timestamp: {datetime.now().isoformat()}

Message:
{message}

---
This is an automated alert from Data Lake Enterprise
        """
        
        html = f"""
<html>
  <body style="font-family: Arial, sans-serif;">
    <h2>🚨 Data Lake Enterprise Alert</h2>
    <table border="1" cellpadding="10" style="border-collapse: collapse;">
      <tr><td><b>Alert Type</b></td><td>{alert_type}</td></tr>
      <tr><td><b>Entity</b></td><td>{entity}</td></tr>
      <tr><td><b>Zone</b></td><td>{zone}</td></tr>
      <tr><td><b>Severity</b></td><td style="color: {'red' if severity == 'CRITICAL' else 'orange'}"><b>{severity}</b></td></tr>
      <tr><td><b>Timestamp</b></td><td>{datetime.now().isoformat()}</td></tr>
    </table>
    <h3>Message:</h3>
    <p>{message}</p>
  </body>
</html>
        """
        
        part1 = MIMEText(text, "plain")
        part2 = MIMEText(html, "html")
        message_obj.attach(part1)
        message_obj.attach(part2)
        
        # Send email (only if SMTP is configured)
        if sender_password:
            server = smtplib.SMTP(smtp_server, smtp_port)
            server.starttls()
            server.login(sender_email, sender_password)
            server.sendmail(sender_email, recipient, message_obj.as_string())
            server.quit()
            print(f"✓ Email alert sent to {recipient}")
        else:
            print(f"⚠ Email alert not sent (SMTP not configured)")
            
    except Exception as e:
        print(f"⚠ Email alert failed: {str(e)}")

if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: send_alert.py <alert_type> <entity> <zone> <message> [severity] [slack_webhook] [email_recipient]")
        sys.exit(1)
    
    alert_type = sys.argv[1]
    entity = sys.argv[2]
    zone = sys.argv[3]
    message = sys.argv[4]
    severity = sys.argv[5] if len(sys.argv) > 5 else "WARNING"
    slack_webhook = sys.argv[6] if len(sys.argv) > 6 else None
    email_recipient = sys.argv[7] if len(sys.argv) > 7 else None
    
    # Create file alert (always)
    create_file_alert(alert_type, entity, zone, message, severity)
    
    # Send to Slack if webhook provided
    if slack_webhook and slack_webhook != "none":
        send_slack_alert(slack_webhook, alert_type, entity, zone, message, severity)
    
    # Send email if recipient provided
    if email_recipient and email_recipient != "none":
        send_email_alert(email_recipient, alert_type, entity, zone, message, severity)
    
    print(f"✓ Alert completed for {entity} in {zone}")
