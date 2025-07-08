import azure.functions as func
from multipart import MultipartParser
from io import BytesIO
import pdfplumber
import json
import re
import logging

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Received request.")

        content_type = req.headers.get('Content-Type', '')
        if 'multipart/form-data' not in content_type:
            return func.HttpResponse(
                json.dumps({"error": "Expected multipart/form-data"}),
                mimetype="application/json",
                status_code=400
            )

        # Parse multipart form
        boundary = content_type.split("boundary=")[-1]
        if not boundary:
            return func.HttpResponse(
                json.dumps({"error": "No boundary found in content-type"}),
                mimetype="application/json",
                status_code=400
            )

        body = req.get_body()
        parser = MultipartParser(BytesIO(body), boundary.encode())

        file_content = None
        for part in parser.parts():
            logging.info(f"Part received: name={part.name}, filename={part.filename}")
            if part.name == "file" and part.filename and part.filename.endswith(".pdf"):
                file_content = part.file.read()

        if not file_content:
            return func.HttpResponse(
                json.dumps({"error": "No file uploaded or invalid file type"}),
                mimetype="application/json",
                status_code=400
            )

        # FIXED: no need for BytesIO again
        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table:
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
        logging.exception("Unhandled exception in function")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            mimetype="application/json",
            status_code=500
        )
