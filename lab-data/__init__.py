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

        # ✅ Extract boundary from header using regex
        import re
        match = re.search(r'boundary="?([^";]+)"?', content_type, re.IGNORECASE)
        if not match:
            return func.HttpResponse(
                json.dumps({"error": "Could not extract boundary from content-type"}),
                mimetype="application/json",
                status_code=400
            )

        boundary = match.group(1)
        logging.info(f"Boundary extracted: {boundary}")

        # ✅ Now safely parse multipart body
        try:
            body = req.get_body()
            parser = MultipartParser(BytesIO(body), boundary.encode())
        except Exception as e:
            logging.exception("Failed to parse multipart form")
            return func.HttpResponse(
                json.dumps({"error": "Multipart parsing failed", "details": str(e)}),
                mimetype="application/json",
                status_code=500
            )

        body = req.get_body()
        parser = MultipartParser(BytesIO(body), boundary.encode())

        file_content = None
        for part in parser.parts():
            logging.info(f"Part received: name={part.name}, filename={part.filename}")
            if part.name == "file" and part.filename and part.filename.endswith(".pdf"):
                if hasattr(part.file, 'read'):
                        file_content = part.file.read()
                elif isinstance(file_content, BytesIO):
                    file_content = file_content.read()
                else:
                    raise ValueError(f"Unrecognized file format: {type(part.file)}")

        if not file_content:
            return func.HttpResponse(
                json.dumps({"error": "No file uploaded or invalid file type"}),
                mimetype="application/json",
                status_code=400
            )

        logging.info(f"Type of file_content: {type(file_content)}")
        logging.info(f"First 100 bytes: {file_content[:100]}")
        try:
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
    except Exception as e:
            logging.exception("Unhandled exception in function")
            return func.HttpResponse(
                json.dumps({"error": "Internal server error", "details": str(e)}),
                mimetype="application/json",
                status_code=500
            )
