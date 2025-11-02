import logging
import azure.functions as func
import os
import json
import time
import pymssql
import smtplib
from email.message import EmailMessage

logging.info("📦 Deployed site packages: %s", os.listdir('/home/site/wwwroot/.python_packages/lib/site-packages'))

cors_headers = {
    "Access-Control-Allow-Origin": "https://https://delightful-tree-0888c340f.1.azurestaticapps.net",
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

def send_email(recipient: str, subject: str, body: str) -> None:
    """Send email using Gmail SMTP (free)."""
    sender = os.getenv("FEEDBACK_EMAIL")
    eml_pass = os.getenv("FEEDBACK_PASS")

    if not sender or not eml_pass:
        logging.info(f"Email: {sender}, Pass: {eml_pass}")
        raise EnvironmentError("Missing FEEDBACK_EMAIL or FEEDBACK_PASS environment variables")

    msg = EmailMessage()
    msg["From"] = sender
    msg["To"] = recipient
    msg["Subject"] = subject
    msg.set_content(body)

    try:
        with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
            smtp.starttls()
            smtp.login(sender, eml_pass)
            smtp.send_message(msg)
        logging.info(f"✅ Email successfully sent to {recipient}")
    except Exception as e:
        logging.exception(f"❌ Failed to send email: {e}")
        raise



def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method == "OPTIONS":
        return func.HttpResponse(status_code=204, headers=cors_headers)

    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse(
            json.dumps({"error": "Invalid JSON"}),
            status_code=400,
            mimetype="application/json"
        )

    name = data.get("name")
    feedback = data.get("feedback")

    if not name or not feedback:
        return func.HttpResponse(
            json.dumps({"error": "Both 'name' and 'feedback' are required."}),
            status_code=400,
            mimetype="application/json"
        )

    logging.info(f"INSERT INTO Narangba.Feedback (Name, Feedback) VALUES ('{name}', '{feedback}')")

    try:
        username = os.environ["SQL_USER"]
        password = os.environ["SQL_PASSWORD"]
        server = os.environ["SQL_SERVER"]
        db = os.environ["SQL_DB"]
        table="[Narangba].[Feedback]"
        variables="[Name], [Feedback]"

        logging.info(f"Collected Information: Username = {username}, Password = {password}, Server = {server}, DB = {db}")


        max_retries = 3
        for attempt in range(max_retries):
            try:
                with pymssql.connect(server, username, password, db) as conn:
                    logging.info(f"Connecting to Server {server} with DB {db}")
                    with conn.cursor() as cursor:
                        cursor.execute(f"INSERT INTO {table} ({variables}) VALUES (%s, %s);", (name, feedback))
                    conn.commit()
                break
            except pymssql.OperationalError as e:
                if attempt < max_retries - 1:
                        logging.warning(f"Retrying DB connection in 5 seconds... Attempt {attempt + 1}")
                        time.sleep(5)
                else:
                    raise

        logging.info("✅ Feedback saved to SQL database")
        try:
            recipient="rpope@purenv.au"
            subject="New Feedback for Narangba Dashboard!"
            body =  (
            f"Hello,\n\n"
            f"A new feedback submission has been added to the Narangba Dashboard.\n\n"
            f"Name: {name}\n"
            f"Feedback: {feedback}\n\n"
            f"Cheers,\nThe Narangba Dashboard Bot"
        )

            send_email(recipient, subject, body)

        except Exception as e:
            logging.exception(f"❌ Error sending email: {e}")

    except Exception as e:
        logging.exception("❌ Database error")
        return func.HttpResponse(
            json.dumps({"error": "Server error", "details": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

    return func.HttpResponse(
        json.dumps({"code": 200, "message": "Feedback submitted successfully."}),
        status_code=200,
        mimetype="application/json",
        headers=cors_headers
    )
