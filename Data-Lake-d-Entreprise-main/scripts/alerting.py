"""
=============================================================================
Script d'Alerting - Notifications Slack/Email
=============================================================================
Auteur      : Personne C – DataOps / QA
Version     : 1.0
Date        : 2026-04-20
Description : Gère les alertes en cas d'échec de pipeline via Slack/Email.
              Intégré avec Airflow via callbacks.

Usage :
    - Configure dans dag_ingestion.py via on_failure_callback
    - Ou via Airflow configuration
=============================================================================
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from pathlib import Path

# Optional: Slack integration
try:
    from slack_sdk import WebClient
    from slack_sdk.errors import SlackApiError
    SLACK_AVAILABLE = True
except ImportError:
    SLACK_AVAILABLE = False

# Optional: Email integration
try:
    import smtplib
    from email.mime.text import MIMEText
    from email.mime.multipart import MIMEMultipart
    EMAIL_AVAILABLE = True
except ImportError:
    EMAIL_AVAILABLE = False

# ---------------------------------------------------------------------------
# Configuration du logger
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger("Alerting")

# ---------------------------------------------------------------------------
# CONFIGURATION DES ALERTES
# ---------------------------------------------------------------------------
ALERT_CONFIG = {
    "slack": {
        "enabled": False,
        "webhook_url": os.getenv("SLACK_WEBHOOK_URL", ""),
        "channel": "#data-lake-alerts",
        "mention": "@data-team",
    },
    "email": {
        "enabled": False,
        "smtp_server": os.getenv("SMTP_SERVER", "smtp.gmail.com"),
        "smtp_port": int(os.getenv("SMTP_PORT", "587")),
        "sender_email": os.getenv("SENDER_EMAIL", ""),
        "sender_password": os.getenv("SENDER_PASSWORD", ""),
        "recipients": ["data-team@company.com"],
    },
    "file": {
        "enabled": True,
        "log_dir": "logs/alerts",
    }
}


# ---------------------------------------------------------------------------
# SLACK ALERTING
# ---------------------------------------------------------------------------
def send_slack_alert(
    dag_id: str,
    task_id: str,
    status: str,
    error_message: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """Envoie une alerte Slack."""
    
    if not ALERT_CONFIG["slack"]["enabled"] or not SLACK_AVAILABLE:
        logger.warning("Slack not enabled or SDK not available")
        return False
    
    try:
        webhook_url = ALERT_CONFIG["slack"]["webhook_url"]
        if not webhook_url:
            logger.warning("Slack webhook URL not configured")
            return False
        
        color = "#FF0000" if status == "FAIL" else "#FFA500"
        
        blocks = [
            {
                "type": "header",
                "text": {
                    "type": "plain_text",
                    "text": f"🚨 Data Lake Alert - {status.upper()}",
                    "emoji": True
                }
            },
            {
                "type": "section",
                "fields": [
                    {
                        "type": "mrkdwn",
                        "text": f"*DAG:*\n{dag_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Task:*\n{task_id}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Status:*\n{status}"
                    },
                    {
                        "type": "mrkdwn",
                        "text": f"*Time:*\n{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    }
                ]
            }
        ]
        
        if error_message:
            blocks.append({
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": f"*Error:*\n\\\{error_message[:500]}\\\"
                }
            })
        
        payload = {
            "blocks": blocks,
            "text": f"Data Lake Alert: {dag_id}/{task_id} - {status}"
        }
        
        import urllib.request
        import urllib.error
        
        req = urllib.request.Request(
            webhook_url,
            data=json.dumps(payload).encode('utf-8'),
            headers={'Content-Type': 'application/json'}
        )
        
        try:
            with urllib.request.urlopen(req) as response:
                if response.status == 200:
                    logger.info("✓ Slack alert sent successfully")
                    return True
        except urllib.error.HTTPError as e:
            logger.error(f"Slack HTTP error: {e}")
            return False
    
    except Exception as e:
        logger.error(f"❌ Error sending Slack alert: {e}")
        return False


# ---------------------------------------------------------------------------
# EMAIL ALERTING
# ---------------------------------------------------------------------------
def send_email_alert(
    dag_id: str,
    task_id: str,
    status: str,
    error_message: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """Envoie une alerte par email."""
    
    if not ALERT_CONFIG["email"]["enabled"] or not EMAIL_AVAILABLE:
        logger.warning("Email not enabled or SMTP not available")
        return False
    
    try:
        config = ALERT_CONFIG["email"]
        
        subject = f"[Data Lake Alert] {dag_id}/{task_id} - {status}"
        
        body = f"""
Data Lake Pipeline Alert

DAG: {dag_id}
Task: {task_id}
Status: {status}
Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

Error Message:
{error_message if error_message else 'No error details'}

---
This is an automated alert from the Data Lake monitoring system.
Please investigate immediately if status is FAIL.
"""
        
        msg = MIMEMultipart()
        msg['From'] = config["sender_email"]
        msg['To'] = ", ".join(config["recipients"])
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        server = smtplib.SMTP(config["smtp_server"], config["smtp_port"])
        server.starttls()
        server.login(config["sender_email"], config["sender_password"])
        server.send_message(msg)
        server.quit()
        
        logger.info("✓ Email alert sent successfully")
        return True
    
    except Exception as e:
        logger.error(f"❌ Error sending email alert: {e}")
        return False


# ---------------------------------------------------------------------------
# FILE-BASED ALERTING (Toujours disponible)
# ---------------------------------------------------------------------------
def log_alert_to_file(
    dag_id: str,
    task_id: str,
    status: str,
    error_message: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """Enregistre l'alerte dans un fichier JSON."""
    
    try:
        config = ALERT_CONFIG["file"]
        log_dir = Path(config["log_dir"])
        log_dir.mkdir(parents=True, exist_ok=True)
        
        alert = {
            "timestamp": datetime.now().isoformat(),
            "dag_id": dag_id,
            "task_id": task_id,
            "status": status,
            "error_message": error_message,
            "context": {
                "execution_date": str(context.get("execution_date")) if context else None,
                "try_number": context.get("try_number") if context else None,
            } if context else None
        }
        
        alert_log = log_dir / "alerts.jsonl"
        with open(alert_log, "a") as f:
            f.write(json.dumps(alert) + "\n")
        
        day_log = log_dir / f"alerts_{datetime.now().strftime('%Y-%m-%d')}.json"
        alerts = []
        if day_log.exists():
            with open(day_log, "r") as f:
                alerts = json.load(f)
        
        alerts.append(alert)
        with open(day_log, "w") as f:
            json.dump(alerts, f, indent=2)
        
        logger.info(f"✓ Alert logged to file: {alert_log}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Error logging alert to file: {e}")
        return False


# ---------------------------------------------------------------------------
# FONCTION PRINCIPALE D'ALERTING
# ---------------------------------------------------------------------------
def send_alert(
    dag_id: str,
    task_id: str,
    status: str = "FAIL",
    error_message: Optional[str] = None,
    context: Optional[Dict[str, Any]] = None
) -> bool:
    """
    Envoie une alerte via tous les canaux configurés.
    
    Args:
        dag_id: Identifiant du DAG
        task_id: Identifiant de la tâche
        status: FAIL, WARNING, SUCCESS
        error_message: Message d'erreur détaillé
        context: Contexte Airflow
    
    Returns:
        True si au moins une alerte a été envoyée
    """
    
    logger.info(f"\n{'='*70}")
    logger.info(f"SENDING ALERT - {dag_id}/{task_id} - {status}")
    logger.info(f"{'='*70}\n")
    
    results = {
        "slack": send_slack_alert(dag_id, task_id, status, error_message, context),
        "email": send_email_alert(dag_id, task_id, status, error_message, context),
        "file": log_alert_to_file(dag_id, task_id, status, error_message, context),
    }
    
    logger.info(f"\nAlert Results:")
    for channel, result in results.items():
        status_symbol = "✓" if result else "✗"
        logger.info(f"  {status_symbol} {channel}: {result}")
    
    return any(results.values())


# ---------------------------------------------------------------------------
# AIRFLOW CALLBACKS
# ---------------------------------------------------------------------------
def task_failure_callback(context: Dict[str, Any]) -> None:
    """Callback Airflow pour les échecs de tâche."""
    
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    exception = context.get("exception", "Unknown error")
    
    send_alert(
        dag_id=dag_id,
        task_id=task_id,
        status="FAIL",
        error_message=str(exception),
        context=context
    )


def task_retry_callback(context: Dict[str, Any]) -> None:
    """Callback Airflow pour les retries de tâche."""
    
    dag_id = context["dag"].dag_id
    task_id = context["task"].task_id
    try_number = context.get("try_number", 1)
    
    send_alert(
        dag_id=dag_id,
        task_id=task_id,
        status="WARNING",
        error_message=f"Task retry attempt #{try_number}",
        context=context
    )


def task_success_callback(context: Dict[str, Any]) -> None:
    """Callback Airflow pour les succès de tâche (optionnel)."""
    pass


if __name__ == "__main__":
    logger.info("Testing alerting system...\n")
    
    send_alert(
        dag_id="test_dag",
        task_id="test_task",
        status="FAIL",
        error_message="This is a test alert"
    )
