import azure.functions as func
from requests_toolbelt.multipart import decoder
from io import BytesIO
import pdfplumber
import json
import re
import logging

FIELD_MAP = {
    "ds-pfas": [
        "Sample Location",
        "Sampling Date/Time",
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
    ]
}

def normalize(text):
    return re.sub(r'[^\w\s]', '', text).lower().strip()

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Parsing multipart form data...")
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
        analyte_fields = target_fields[2:]  # skip Sample Location and Date/Time
        normalized_analytes = [normalize(f) for f in analyte_fields]

        rows = []
        logging.info("Opening PDF...")
        with pdfplumber.open(BytesIO(file_content)) as pdf:
            for page_number, page in enumerate(pdf.pages):
                logging.info(f"Processing page {page_number + 1}...")
                tables = page.extract_tables()
                for t_idx, table in enumerate(tables):
                    if not table or len(table) < 3:
                        continue

                    # Skip tables that contain no known analytes
                    analyte_labels = [normalize(r[0]) for r in table[3:] if r and r[0]]
                    if not any(any(normalize(f) in a for f in analyte_fields) for a in analyte_labels):
                        logging.info(f"Skipping table {t_idx} (no analytes found)")
                        continue

                    sample_locations = table[0][3:]
                    sample_datetimes = table[1][3:]

                    for col_index, sample_location in enumerate(sample_locations):
                        if not sample_location or sample_location.strip() == '----':
                            continue

                        date_val = sample_datetimes[col_index] if col_index < len(sample_datetimes) else "NULL"
                        sample_location = sample_location.strip()
                        sample_datetime = date_val.strip() if date_val else "NULL"

                        row_dict = {
                            "Sample Location": f"'{sample_location}'",
                            "Sampling Date/Time": f"'{sample_datetime}'" if sample_datetime != "NULL" else "NULL"
                        }

                        i = 3
                        while i < len(table):
                            row = table[i]
                            if not row or len(row) < (col_index + 4):
                                i += 1
                                continue

                            analyte_lines = [row[0].strip()] if row[0] else []
                            j = i + 1
                            while j < len(table) and (not table[j][0] or table[j][0].strip() == ''):
                                analyte_lines.append(table[j][0].strip() if table[j][0] else '')
                                j += 1

                            analyte = ' '.join(analyte_lines).strip()
                            match = next((f for f in analyte_fields if normalize(analyte) in normalize(f) or normalize(f) in normalize(analyte)), None)

                            logging.info({
                                "analyte_raw": analyte,
                                "matched": match,
                                "sample_location": sample_location,
                                "sampling_datetime": sample_datetime,
                                "column_index": col_index + 3
                            })

                            if match:
                                val_row = table[j - 1]
                                val = val_row[col_index + 3] if col_index + 3 < len(val_row) else None
                                if val:
                                    val = val.strip()
                                    if val in ["", "-", "----"]:
                                        row_dict[match] = "NULL"
                                    elif re.match(r'^-?\d+(\.\d+)?$', val):
                                        row_dict[match] = val
                                    else:
                                        row_dict[match] = f"'{val.replace("<", "")}'"
                                else:
                                    row_dict[match] = "NULL"

                            i = j

                        row_values = [row_dict.get(field, "NULL") for field in target_fields]
                        rows.append(f"           ({', '.join(row_values)})")

        if not rows:
            return func.HttpResponse(json.dumps({"error": "No valid data found in PDF"}), status_code=400)

        rows = []
        for row_dict in combined_rows.values():
            row_values = [row_dict.get(field, "NULL") for field in target_fields]
            rows.append(f"           ({', '.join(row_values)})")

        columns_sql = ",\n           ".join([f"[{f}]" for f in target_fields])
        sql = f"INSERT INTO [Jackson].[DSPFAS]\n           ({columns_sql})\n     VALUES\n" + ",\n".join(rows) + ";"


        return func.HttpResponse(json.dumps({"query": sql}), mimetype="application/json", status_code=200)

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            status_code=500
        )
