# File: /HttpTriggerGetData/__init__.py
import os
import io
import json
import logging
import requests
import azure.functions as func
from docx import Document

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Authenticating Login...")

    try:
        # === Load environment variables ===
        auth_url = "https://als-client-api.azurewebsites.net/api/user/authenticate"
        data_url = os.environ["API_DATA_URL"]
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

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

        token = auth_resp.json().get("Token") or auth_resp.json().get("token")
        if not token:
            logging.error("No Token Returned")
            raise ValueError("No token returned from authentication response.")

        # === Step 2: Fetch data ===
        data_headers = {
            "Accept": "application/json",
            "Authorization": f"Bearer {token}",
        }

        logging.info(f"Collected Headers:{data_headers}")

        data_resp = requests.get(data_url, headers=data_headers, timeout=15)
        data_resp.raise_for_status()
        data = data_resp.json()

        # === Step 3: Create Word document ===
        doc = Document()
        doc.add_heading("API Data Export", level=1)
        doc.add_paragraph(f"Fetched from: {data_url}")
        doc.add_paragraph("")

        # Write key-value pairs recursively (simplified)
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

        # === Step 4: Save to memory and return ===
        file_stream = io.BytesIO()
        doc.save(file_stream)
        file_stream.seek(0)

        return func.HttpResponse(
            body=file_stream.read(),
            mimetype="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            headers={
                "Content-Disposition": 'attachment; filename="api_data.docx"'
            },
            status_code=200,
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
