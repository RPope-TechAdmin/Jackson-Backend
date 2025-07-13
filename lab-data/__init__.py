import azure.functions as func
from requests_toolbelt.multipart import decoder
from io import BytesIO
import pdfplumber
import json
import re
import logging

# Define valid field sets for each query type
FIELD_MAP = {
    "ds-pfas": [
        "Perfluorobutane sulfonic acid", "Perfluoropentane sulfonic acid", "Perfluorohexane sulfonic acid",
        "Perfluoroheptane sulfonic acid", "Perfluorooctane sulfonic acid", "Perfluorodecane sulfonic acid",
        "Perfluorobutanoic acid", "Perfluoropentanoic acid", "Perfluorohexanoic acid", "Perfluoroheptanoic",
        "Perfluorooctanoic acid", "Perfluorononanoic acid", "Perfluorodecanoic acid", "Perfluoroundecanoic acid",
        "Perfluorododecanoic acid", "Perfluorotridecanoic acid", "Perfluorotetradecanoic acid",
        "Perfluorooctane sulfonamide", "N-Methyl perfluorooctane sulfonamide",
        "N-Ethyl perfluorooctane sulfonamide", "N-Methyl perfluorooctane sulfonamidoethanol",
        "N-Ethyl perfluorooctane sulfonamidoethanol", "N-Methyl perfluorooctane sulfonamidoacetic acid",
        "N-Ethyl perfluorooctane sulfonamidoacetic acid", "4:2 Fluorotelomer sulfonic acid",
        "6:2 Fluorotelomer sulfonic acid", "8:2 Fluorotelomer sulfonic acid",
        "10:2 Fluorotelomer sulfonic acid", "Sum of PFAS", "Sum of PFHxS and PFOS",
        "Sum of TOP C4 - C14 Carboxylates and C4-C8 Sulfonates", "Sum of TOP C4 - C14 as Fluorine",
        "13C4-PFOS", "13C8-PFOA"
    ],
    "ds-int": ["Nitrogen", "Phosphorus", "Potassium", "Calcium"],
    "ds-ext": ["DDT", "Glyphosate", "Chlorpyrifos", "Atrazine"]
}

def normalize(text):
    return re.sub(r'[^\w\s]', '', text).lower().strip()

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Handling PDF upload and data extraction")

        content_type = req.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return func.HttpResponse(json.dumps({"error": "Expected multipart/form-data"}), status_code=400)

        multipart_data = decoder.MultipartDecoder(req.get_body(), content_type)

        file_content, query_type = None, None
        for part in multipart_data.parts:
            content_disp = part.headers.get(b"Content-Disposition", b"").decode()
            if 'filename="' in content_disp and content_disp.endswith('.pdf"'):
                file_content = part.content
            elif 'name="query_type"' in content_disp:
                query_type = part.text.strip().lower()

        if not file_content:
            return func.HttpResponse(json.dumps({"error": "No PDF file uploaded"}), status_code=400)
        if not query_type or query_type not in FIELD_MAP:
            return func.HttpResponse(json.dumps({"error": "Invalid or missing query_type"}), status_code=400)

        target_fields = FIELD_MAP[query_type]
        insert_rows = []

        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page_index, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                logging.info(f"Page {page_index + 1} has {len(tables)} tables.")

                for table_index, table in enumerate(tables):
                    if not table or len(table) < 2:
                        logging.warning(f"Skipping empty or too-short table on page {page_index + 1}.")
                        continue

                    headers = [normalize(h) if h else "" for h in table[0]]
                    logging.info(f"Extracted headers from page {page_index + 1} table {table_index + 1}: {headers}")

                    matched_indices = {}
                    for field in target_fields:
                        for i, header in enumerate(headers):
                            if normalize(field) in header:
                                matched_indices[field] = i
                                break

                    if not matched_indices:
                        logging.warning(f"No matching headers found on page {page_index + 1} table {table_index + 1}")
                        continue

                    for row_index, row in enumerate(table[1:]):
                        if not row or len(row) < 3:
                            logging.info(f"Skipping row {row_index + 1} due to insufficient columns: {row}")
                            continue

                        sample_location = row[0].strip() if row[0] else "Unknown"
                        row_values = [f"'{sample_location}'"]
                        row_has_data = False

                        for field in target_fields:
                            i = matched_indices.get(field)
                            val = "NULL"
                            if i is not None and i < len(row):
                                raw = row[i]
                                if raw:
                                    raw = raw.strip()
                                    if raw not in ["", "-"]:
                                        if re.match(r'^-?\d+(\.\d+)?$', raw):
                                            val = raw
                                        else:
                                            val = f"'{raw.replace('\'', '\'\'')}'"
                                        row_has_data = True
                            row_values.append(val)

                        if row_has_data:
                            insert_rows.append(f"({', '.join(row_values)})")
                        else:
                            logging.info(f"Row {row_index + 1} matched headers but had no usable values.")

        if not insert_rows:
            return func.HttpResponse(json.dumps({"error": "No valid data found in PDF"}), status_code=400)

        insert_fields = ['[Sample Location]'] + [f"[{f}]" for f in target_fields]
        sql_query = f"INSERT INTO {query_type} ({', '.join(insert_fields)})\nVALUES\n  {',\n  '.join(insert_rows)};"

        return func.HttpResponse(
            json.dumps({"query": sql_query}),
            mimetype="application/json",
            status_code=200
        )

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            status_code=500
        )
