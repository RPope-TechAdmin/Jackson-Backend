import logging
import azure.functions as func
import os
import tempfile
import json
import pyodbc
import pdfplumber
import re
from sqlalchemy import create_engine, text

def main(req: func.HttpRequest) -> func.HttpResponse:
    # Read uploaded file from form-data
    file = req.files.get('file')  # 'file' must match the frontend key

    if not file:
        return func.HttpResponse("No file uploaded", status_code=400)

    # Save to temporary file
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as tmp_file:
        tmp_file.write(file.read())
        tmp_path = tmp_file.name

    # Process PDF using pdfplumber
    sql_queries = []
    with pdfplumber.open(tmp_path) as pdf:
        for page in pdf.pages:
            tables = page.extract_tables()
            for table in tables:
                if not table:
                    continue
                for row in table:
                    logging.info(f"Processing Row: {row}")
                    if not row or len(row) < 2:
                        continue  # Skip blank or incomplete rows

                    raw_name = row[0]
                    if raw_name is None:
                        continue  # Skip rows where name is missing

                    name = raw_name.strip()
                    numeric_count = sum(
                        1 for cell in row[1:]
                        if cell and isinstance(cell, str) and re.match(r'^-?\d+(\.\d+)?$', cell.strip())
                    )
                    sql = f"INSERT INTO your_table (name, count) VALUES ('{name}', {numeric_count});"
                    sql_queries.append(sql)


    # Optionally clean up
    os.remove(tmp_path)

    return func.HttpResponse(
        body={"queries": sql_queries},
        mimetype="application/json",
        status_code=200
    )
