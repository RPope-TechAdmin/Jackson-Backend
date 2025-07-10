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
        "TOP_C Perfluorooctane sulfonamide", "TOP_C N-Methyl perfluorooctane sulfonamide",
        "TOP_C N-Ethyl perfluorooctane sulfonamide", "TOP_C N-Methyl perfluorooctane sulfonamidoethanol",
        "TOP_C N-Ethyl perfluorooctane sulfonamidoethanol", "TOP_C N-Methyl perfluorooctane sulfonamidoacetic acid",
        "TOP_C N-Ethyl perfluorooctane sulfonamidoacetic acid", "TOP_D 4:2 Fluorotelomer sulfonic acid",
        "TOP_D 6:2 Fluorotelomer sulfonic acid", "TOP_D 8:2 Fluorotelomer sulfonic acid",
        "TOP_D 10:2 Fluorotelomer sulfonic acid", "TOP_P Sum of PFAS", "TOP_P Sum of PFHxS and PFOS",
        "TOP_P Sum of TOP C4 - C14 Carboxylates and C4-C8 Sulfonates", "TOP_P Sum of TOP C4 - C14 as Fluorine",
        "TOP_S 13C4-PFOS", "TOP_S 13C8-PFOA"
    ],
    "ds-int": ["Nitrogen", "Phosphorus", "Potassium", "Calcium"],
    "ds-ext": ["DDT", "Glyphosate", "Chlorpyrifos", "Atrazine"]
}


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

        rows_data = []
        headers = []
        DATA_OFFSET = 3

        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                logging.info(f"Found {len(tables)} tables on page {page.page_number}")
                for table in tables:
                    logging.info("Full table:")
                    for r in table:
                        logging.info(str(r))

                    if not table or len(table) < 2:
                        continue

                    raw_headers = table[0]
                    headers = [h.strip() if h else f"col{i}" for i, h in enumerate(raw_headers)]
                    logging.info(f"Extracted headers: {headers}")

                    for debug_row in table[1:3]:
                        logging.info(f"Sample row: {debug_row}")

                    field_indexes = []
                    matched_headers = []
                    for i, h in enumerate(headers):
                        for field in TARGET_FIELDS:
                            logging.info(f"Comparing field '{field.lower()}' in header '{h.lower()}'")
                            if field.lower() in h.lower():
                                field_indexes.append(i)
                                matched_headers.append(h)
                                break

                    logging.info(f"Matched fields: {matched_headers}")

                    if not field_indexes:
                        continue

                    for row in table[1:]:
                        if not row or len(row) < 2:
                            continue

                        name = row[0].strip() if row[0] else "Unknown"
                        values = [f"'{name}'"]

                        for i in field_indexes:
                            Index_Real = i + DATA_OFFSET
                            if Index_Real >= len(row):
                                logging.warning(f"Index {Index_Real} out of range for row: {row}")
                                values.append("NULL")
                                continue
                            try:
                                val = row[Index_Real].strip() if row[Index_Real] else None
                                if val in ["-", ""]:
                                    values.append("NULL")
                                elif re.match(r'^-?\d+(\.\d+)?$', val):
                                    values.append(val)
                                else:
                                    values.append(f"'{val.replace("'", "''")}'")
                            except Exception as e:
                                logging.warning(f"Failed to process value at index {Index_Real}: {e}")
                                values.append("NULL")

                        rows_data.append(f"({', '.join(values)})")

        if not rows_data:
            return func.HttpResponse(
                json.dumps({"error": "No valid data found in PDF"}),
                mimetype="application/json",
                status_code=400
            )

        insert_fields = ['name'] + [headers[i] for i in field_indexes]
        sql = f"INSERT INTO {query_type} ({', '.join(insert_fields)})\nVALUES\n  {',\n  '.join(rows_data)};"

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
