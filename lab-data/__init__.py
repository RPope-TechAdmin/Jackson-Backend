import azure.functions as func
from requests_toolbelt.multipart import decoder
from io import BytesIO
import pdfplumber
import json
import re
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Received request")

        content_type = req.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return func.HttpResponse(
                json.dumps({"error": "Expected multipart/form-data"}),
                mimetype="application/json",
                status_code=400
            )

        body = req.get_body()

        try:
            multipart_data = decoder.MultipartDecoder(body, content_type)
        except Exception as e:
            logging.exception("Failed to decode multipart")
            return func.HttpResponse(
                json.dumps({"error": "Multipart parsing failed", "details": str(e)}),
                mimetype="application/json",
                status_code=500
            )

        file_content = None

        for part in multipart_data.parts:
            content_disposition = part.headers.get(b'Content-Disposition', b'').decode()
            if 'filename="' in content_disposition and content_disposition.endswith('.pdf"'):
                file_content = part.content
                logging.info("PDF file part found and read")
                break

        if not file_content:
            return func.HttpResponse(
                json.dumps({"error": "No PDF file uploaded"}),
                mimetype="application/json",
                status_code=400
            )

        sql_queries = []

        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue
                    for row in table:
                        if not row or len(row) < 2:
                            continue
                        raw_name = row[0]
                        if raw_name is None:
                            continue
                        name = raw_name.strip()
                        numeric_count = sum(
                            1 for cell in row[1:]
                            if cell and isinstance(cell, str) and re.match(r'^-?\d+(\.\d+)?$', cell.strip())
                        )
                        sql = f"INSERT INTO your_table (name, count) VALUES ('{name}', {numeric_count});"
                        sql_queries.append(sql)

        return func.HttpResponse(
            json.dumps({"queries": sql_queries}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )
