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
        "erfluorooctanoic acid", "Perfluorononanoic acid", "Perfluorodecanoic acid", "Perfluoroundecanoic acid",
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
    return set(re.sub(r'[^\w\s]', '', text).lower().split())

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
        query_type = None

        for part in multipart_data.parts:
            headers = part.headers.get(b'Content-Disposition', b'').decode()
            if 'filename="' in headers and headers.endswith('.pdf"'):
                file_content = part.content
                logging.info("PDF file part found and read")
            elif 'name="query_type"' in headers:
                query_type = part.text.strip().lower()

        if not file_content:
            return func.HttpResponse(
                json.dumps({"error": "No PDF file uploaded"}),
                mimetype="application/json",
                status_code=400
            )

        if not query_type:
            return func.HttpResponse(
                json.dumps({"error": "Missing query_type in form data"}),
                mimetype="application/json",
                status_code=400
            )

        TARGET_FIELDS = FIELD_MAP.get(query_type)
        if not TARGET_FIELDS:
            return func.HttpResponse(
                json.dumps({"error": f"Unknown query type: {query_type}"}),
                mimetype="application/json",
                status_code=400
            )

        insert_rows = []
        DATA_OFFSET = 3

        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    if not table or len(table) < 2:
                        continue

                    headers = [h.strip() if h else f"col{i}" for i, h in enumerate(table[0])]
                    header_map = {}
                    for i, h in enumerate(headers):
                        if h.lower().startswith("col") or set(h.strip()) == {'-'}:
                            continue
                        for field in TARGET_FIELDS:
                            if normalize(field).issubset(normalize(h)):
                                header_map[field] = i
                                break

                    if not header_map:
                        continue

                    for row in table[1:]:
                        if not row or len(row) < 2:
                            continue

                        sample_location = row[0].strip() if row[0] else "Unknown"
                        row_values = [f"'{sample_location}'"]

                        for field in TARGET_FIELDS:
                            i = header_map.get(field)
                            val = "NULL"
                            if i is not None and (i + DATA_OFFSET) < len(row):
                                raw = row[i + DATA_OFFSET]
                                if raw:
                                    raw = raw.strip()
                                    if raw in ["-", ""]:
                                        val = "NULL"
                                    elif re.match(r'^-?\d+(\.\d+)?$', raw):
                                        val = raw
                                    else:
                                        val = f"'{raw.replace("'", "''")}'"
                            row_values.append(val)

                        insert_rows.append(f"({', '.join(row_values)})")

        if not insert_rows:
            return func.HttpResponse(
                json.dumps({"error": "No valid data found in PDF"}),
                mimetype="application/json",
                status_code=400
            )

        insert_fields = ['[Sample Location]'] + [f"[{f}]" for f in TARGET_FIELDS]
        sql = f"INSERT INTO {query_type} ({', '.join(insert_fields)})\nVALUES\n  {',\n  '.join(insert_rows)};"

        return func.HttpResponse(
            json.dumps({"query": sql}),
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