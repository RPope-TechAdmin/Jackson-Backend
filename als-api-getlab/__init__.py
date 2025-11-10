# File: /HttpTriggerGetData/__init__.py
import os
import io
import json
import logging
import requests
import azure.functions as func
from datetime import datetime, timedelta
from docx import Document

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Fetching data and generating Word document...")

    try:
        # === Environment variables ===
        auth_url = os.environ["API_AUTH_URL"]
        data_url = os.environ["API_DATA_URL"]
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        # Default: last 7 days, page=1
        to_dt = datetime.utcnow()
        from_dt = to_dt - timedelta(days=7)
        from_param = from_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        to_param = to_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        page_param = "1"

        # === Step 1: Authenticate ===
        auth_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }
        auth_payload = {
            "Username": username,
            "Password": password,
        }

        auth_resp = requests.post(auth_url, headers=auth_headers, json=auth_payload, timeout=10)
        auth_resp.raise_for_status()

        auth_data = auth_resp.json()

        # Support multiple possible token structures
        token = (
            auth_data.get("Token")
            or auth_data.get("token")
            or (auth_data.get("Data", {}).get("Token"))
            or (auth_data.get("data", {}).get("token"))
        )

        if not token:
            raise ValueError(f"No token found in auth response: {auth_data}")
        
        # === Step 2: Fetch data ===
        params = {
            "From": from_param,
            "To": to_param,
            "Page": page_param,
        }

        data_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        data_resp = requests.get(data_url, headers=data_headers, params=params, timeout=20)
        if data_resp.status_code == 401:
            # Retry once using plain token (some ALS APIs require this)
            logging.warning("Bearer header rejected — retrying with raw token header.")
            data_headers = {
                "Accept": "application/json",
                "Authorization": token,
            }
            data_resp = requests.get(data_url, headers=data_headers, params=params, timeout=20)

        data_resp.raise_for_status()
        data = data_resp.json()

        # === Step 3: Create Word document ===
        doc = Document()
        doc.add_heading("API Data Export", level=1)
        doc.add_paragraph(f"Fetched from: {data_url}")
        doc.add_paragraph(f"From: {from_param}")
        doc.add_paragraph(f"To: {to_param}")
        doc.add_paragraph(f"Page: {page_param}")
        doc.add_paragraph("")

        def write_json_to_doc(d, indent=0):
            for key, value in d.items():
                if isinstance(value, dict):
                    doc.add_paragraph(" " * indent + f"{key}:", style="List Bullet")
                    write_json_to_doc(value, indent + 2)
                elif isinstance(value, list):
                    doc.add_paragraph(" " * indent + f"{key}:")
                    for item in value:
                        if isinstance(item, dict):
                            write_json_to_doc(item, indent + 4)
                        else:
                            doc.add_paragraph(" " * (indent + 2) + str(item))
                else:
                    doc.add_paragraph(" " * indent + f"{key}: {value}")

        if isinstance(data, dict):
            write_json_to_doc(data)
        elif isinstance(data, list):
            for item in data:
                if isinstance(item, dict):
                    write_json_to_doc(item)
                    doc.add_paragraph("---")
                else:
                    doc.add_paragraph(str(item))

        # === Step 4: Save and return ===
        file_stream = io.BytesIO()
        doc.save(file_stream)
        file_stream.seek(0)

        filename = f"api_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.docx"
        return func.HttpResponse(
            open(filename, "rb").read(),
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
