from airflow.hooks.base import BaseHook
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from datetime import datetime
import pandas as pd
from connectors.Doris_hook_connector import DorisHook
from airflow.models import Variable

# Custom function to send email using connection from Airflow
def send_email_from_connection(to, subject, html_content, conn_id="smtp_default"):
    conn = BaseHook.get_connection(conn_id)

    smtp_host = conn.host
    smtp_port = conn.port or 587
    smtp_user = conn.login
    smtp_password = conn.password
    smtp_mail_from = conn.extra_dejson.get("from_email", conn.login)
    use_tls = not conn.extra_dejson.get("disable_tls", False)
    use_ssl = conn.extra_dejson.get("use_ssl", False)

    msg = MIMEText(html_content, "html")
    msg["Subject"] = subject
    msg["From"] = formataddr(("Airflow", smtp_mail_from))
    msg["To"] = ", ".join(to)

    # Use SSL or TLS based on the connection settings
    # if use_ssl:
    #     server = smtplib.SMTP_SSL(smtp_host, smtp_port)
    # else:
    #     server = smtplib.SMTP(smtp_host, smtp_port)
    #     if use_tls:
    #         server.starttls()
    server = smtplib.SMTP(smtp_host, smtp_port)
    server.starttls()
    server.login(smtp_user, smtp_password)
    server.sendmail(smtp_mail_from, to, msg.as_string())
    server.quit()

# Function to log task status to Doris
def log_to_doris(context, status):
    ti = context["ti"]
    try:
        log_data = {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": ti.run_id,
            "execution_date": context["execution_date"],
            "start_time": ti.start_date,
            "end_time": ti.end_date,
            "status": status,
            "created_at": datetime.utcnow()
        }
        DorisHook().insert_log("task_audit", log_data)
    except Exception as e:
        print(f"[Audit Logger] Failed to insert audit log: {e}")

# Function to send email after task completion
def send_custom_email(context, status):
    try:
        ti = context["ti"]
        dag_email_list = context["dag"].default_args.get("user", [])
        
        if isinstance(dag_email_list, str):
            dag_email_list = [dag_email_list]

        # Fetch global email for notifications
        global_email = Variable.get("default_notification_email", default_var=None)
        
        # Combine DAG-specific emails and global email if available
        recipients = list(set(dag_email_list + ([global_email] if global_email else [])))
        print(recipients)

        # Email subject and body
        subject = f"[Airflow] Task {status.capitalize()}: {ti.dag_id}.{ti.task_id}"
        log_url = ti.log_url.replace("localhost:8080", "34.95.178.165:32002")
        body = f"""
        DAG: {ti.dag_id}<br>
        Task: {ti.task_id}<br>
        Run ID: {ti.run_id}<br>
        Execution Date: {context["execution_date"]}<br>
        Log URL: <a href="{log_url}">{log_url}</a>
        <br><br>
        """

        send_email_from_connection(
            to=recipients,
            subject=subject,
            html_content=body,
            conn_id="smtp_default"
        )
    except Exception as e:
        print(f"[Audit Logger] Failed to send {status} email: {e}")

def success_callback(context):
    log_to_doris(context, "success")
    #send_custom_email(context, "success")

def failure_callback(context):
    log_to_doris(context, "failed")
    send_custom_email(context, "failed")
