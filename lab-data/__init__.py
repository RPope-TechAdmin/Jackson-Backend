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
        "Sum of TOP C4  C14 Carboxylates and C4 C8 Sulfonates", "Sum of TOP C4 - C14 as Fluorine",
        "13C4-PFOS", "13C8-PFOA"
    ]
}

# Abbreviation-to-full-name mapping
ABBREV_TO_FULL = {
    "mefosa": "N-Methyl perfluorooctane sulfonamide",
    "etfosa": "N-Ethyl perfluorooctane sulfonamide",
    "mefose": "N-Methyl perfluorooctane sulfonamidoethanol",
    "etfose": "N-Ethyl perfluorooctane sulfonamidoethanol",
    "2355319": "N-Methyl perfluorooctane sulfonamidoacetic acid",
    "2991506": "N-Ethyl perfluorooctane sulfonamidoacetic acid"
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
        combined_rows = {}  # key = (sample_location, sample_datetime), value = field dict

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

                        key = (sample_location, sample_datetime)
                        if key not in combined_rows:
                            combined_rows[key] = {
                                "Sample Location": f"'{sample_location}'",
                                "Sampling Date/Time": f"'{sample_datetime}'" if sample_datetime != "NULL" else "NULL"
                            }

                        row_dict = combined_rows[key]

                        i = 3
                        while i < len(table):
                            row = table[i]
                            if not row or len(row) < (col_index + 4):
                                i += 1
                                continue

                            analyte_lines = [row[0].strip()] if row[0] else []
                            j = i + 1
                            while j < len(table):
                                next_line = table[j][0] if table[j][0] else ''
                                next_line_stripped = next_line.strip()
                                if next_line_stripped == '' or re.match(r'^[A-Za-z()\\d\\s\\-]+$', next_line_stripped):
                                    analyte_lines.append(next_line_stripped)
                                    j += 1
                                else:
                                    break
                                analyte_lines.append(table[j][0].strip() if table[j][0] else '')
                                j += 1

                            analyte = ' '.join(analyte_lines).strip()
                            normalized_analyte = normalize(analyte)

                            # Skip blank or non-analyte labels
                            if not analyte or normalized_analyte in ["", "result", "results", "cas", "parameter"]:
                                logging.info(f"Skipping non-analyte label: '{analyte}'")
                                i = j
                                continue

                            # Strict match first
                            match = next((f for f in analyte_fields if normalize(f) == normalized_analyte), None)

                            if not match:
                                abbrev_found = re.findall(r'\b[a-z]{2,6}\b', normalized_analyte)
                                for abbrev in abbrev_found:
                                    if abbrev in ABBREV_TO_FULL:
                                        full_name = ABBREV_TO_FULL[abbrev]
                                        if full_name in analyte_fields:
                                            match = full_name
                                            logging.info(f"Abbreviation matched: {abbrev} → {full_name}")
                                            break

                            # Then fuzzy fallback if analyte is long enough
                            if not match and len(normalized_analyte) > 10:
                                match = next((f for f in analyte_fields if normalize(f) in normalized_analyte or normalized_analyte in normalize(f)), None)
                            logging.info({
                                "analyte_raw": analyte,
                                "matched": match,
                                "sample_location": sample_location,
                                "sampling_datetime": sample_datetime,
                                "column_index": col_index + 3
                            })

                            if not match:
                                logging.warning(f"Unmatched analyte: '{analyte}' (normalized: '{normalized_analyte}')")
                                i = j
                                continue

                            val_row = table[j - 1] if j - 1 < len(table) else table[i]
                            val = val_row[col_index + 3] if col_index + 3 < len(val_row) else None

                            if val:
                                val = val.strip()
                                if val in ["", "-", "----"]:
                                    row_dict[match] = "NULL"
                                elif re.match(r'^-?\d+(\.\d+)?$', val.replace("<", "")):
                                    row_dict[match] = val.replace("<", "")
                                else:
                                    row_dict[match] = f"'{val.replace('<', '')}'"
                            else:
                                row_dict[match] = "NULL"

                            i = j

        if not combined_rows:
            return func.HttpResponse(json.dumps({"error": "No valid data found in PDF"}), status_code=400)

        rows = []
        for row_dict in combined_rows.values():
            row_values = [row_dict.get(field, "NULL") for field in target_fields]
            rows.append(f"           ({', '.join(row_values)})")

        try:
            columns_sql = ",\n           ".join([f"[{f}]" for f in target_fields])
            sql = f"INSERT INTO [Jackson].[DSPFAS]\n           ({columns_sql})\n     VALUES\n" + ",\n".join(rows) + ";"

            logging.info("Generated SQL query successfully.")
            return func.HttpResponse(
                json.dumps({"query": sql}),
                mimetype="application/json",
                status_code=200
            )
        except Exception as e:
            logging.exception("Failed to build SQL query")
            return func.HttpResponse(
                json.dumps({"error": "SQL build failed", "details": str(e)}),
                mimetype="application/json",
                status_code=500
            )

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            status_code=500
        )