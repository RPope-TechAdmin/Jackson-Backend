import azure.functions as func
from requests_toolbelt.multipart import decoder
from io import BytesIO
import pdfplumber
import os
import json
import re
import logging
import pymssql
import time
from sqlalchemy import create_engine, text

cors_headers = {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

FIELD_MAP = {
    "ds-pfas": [
        "File Name",
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
    ],
    "ds-int": [
        "File Name",
        "Sample Location",
        "Sampling Date/Time",
        "Electrical Conductivity @ 25°C","Nitrite + Nitrate as N",
        "Total Kjeldahl Nitrogen as N","Total Nitrogen as N","Total Phosphorus as P"
    ],
    "ds-ext": [
        "File Name",
        "Sample Location",
        "Sampling Date/Time",
        "Total Arsenic","Total Beryllium","Total Cadmium","Total Chromium",
        "Total Copper","Total Cobalt","Total Nickel","Total Lead","Total Zinc","Total Manganese","Total Selenium","Total Silver",
        "Total Boron","Total Mercury","Total Organic Carbon","TPH Silica C10 - C14 Fraction","TPH Silica C15 - C28 Fraction",
        "TPH Silica C29 - C36 Fraction","TPH Silica C10 - C36 Fraction (sum)","TRH C10 - C16 Fraction","TRH C16 - C34 Fraction",
        "TRH C34 - C40 Fraction","TRH C10 - C40 Fraction (sum)","TRH C10 - C16 Fraction minus Naphthalene","Phenol","2-Chlorophenol","2-Methylphenol",
        "3- & 4-Methylphenol","2-Nitrophenol","2,4-Dimethylphenol","2,6-Dichlorophenol","4-Chloro-3-methylphenol","2,4,6-Trichlorophenol",
        "2,4,5-Trichlorophenol","Pentachlorophenol","Sum of Phenols","TPH C6 - C9 Fraction","TRH NEPMC6 - C10 Fraction C6_C10",
        "TRH NEPMC6 - C10 Fraction minus BTEX","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes",
        "Sum of BTEX","Naphthalene","Escherichia coli","Phenol-d6","2-Chlorophenol-D4","2,4,6-Tribromophenol","2-Fluorobiphenyl",
        "Anthracene-d10","4-Terphenyl-d14","1,2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene"
    ]
}

ABBREV_TO_FULL = {
    "mefosa": "N-Methyl perfluorooctane sulfonamide",
    "etfosa": "N-Ethyl perfluorooctane sulfonamide",
    "mefose": "N-Methyl perfluorooctane sulfonamidoethanol",
    "etfose": "N-Ethyl perfluorooctane sulfonamidoethanol",
    "mefosaa": "N-Methyl perfluorooctane sulfonamidoacetic acid",
    "etfosaa": "N-Ethyl perfluorooctane sulfonamidoacetic acid"
}

CAS_TO_FULL = {
    "2355-31-9": "N-Methyl perfluorooctane sulfonamidoacetic acid",  # MeFOSAA
    "2991-50-6": "N-Ethyl perfluorooctane sulfonamidoacetic acid",   # EtFOSAA
    "31506-32-8": "N-Methyl perfluorooctane sulfonamide",             # MeFOSA
    "4151-50-2": "N-Ethyl perfluorooctane sulfonamide",              # EtFOSA
    "24448-09-7": "N-Methyl perfluorooctane sulfonamidoethanol",     # MeFOSE
    "1691-99-2": "N-Ethyl perfluorooctane sulfonamidoethanol",        # EtFOSE
    "7440-38-2": "Total Arsenic",
    "7440-41-7": "Total Beryllium",
    "7440-43-9": "Total Cadmium",
    "7440-47-3": "Total Chromium",
    "7440-50-8": "Total Copper",
    "7440-48-4": "Total Cobalt",
    "7440-02-0": "Total Nickel",
    "7439-92-1": "Total Lead",
    "7440-66-6": "Total Zinc",
    "7439-96-5": "Total Manganese",
    "7782-49-2": "Total Selenium",
    "7440-22-4": "Total Silver",
    "7440-62-2": "Total Vanadium",
    "7440-42-8": "Total Boron",
    "7439-97-6": "Total Mercury",
    "108-95-2": "Phenol",
    "95-57-8": "2-Chlorophenol",
    "95-48-7": "2-Methylphenol",
    "1319-77-3": "3- & 4-Methylphenol",
    "88-75-5": "2-Nitrophenol",
    "105-67-9": "2,4-Dimethylphenol",
    "120-83-2": "2,4-Dichlorophenol",
    "87-65-0": "2,6-Dichlorophenol",
    "59-50-7": "4-Chloro-3-methylphenol",
    "88-06-2": "2,4,6-Trichlorophenol",
    "95-95-4": "2,4,5-Trichlorophenol",
    "87-86-5": "Pentachlorophenol",
    "C6_C10": "TRH NEPMC6 - C10 Fraction C6_C10",
    "71-43-2": "Benzene",
    "108-88-3": "Toluene",
    "100-41-4": "Ethylbenzene",
    "108-38-3 106-42-3": "meta- & para-Xylene",
    "95-47-6": "ortho-Xylene",
    "91-20-3": "Naphthalene",
    "13127-88-3": "Phenol-d6",
    "93951-73-6": "2-Chlorophenol-D4",
    "118-79-6": "2,4,6-Tribromophenol",
    "321-60-8": "2-Fluorobiphenyl",
    "1719-06-8": "Anthracene-d10",
    "1718-51-0": "4-Terphenyl-d14",
    "17060-07-0": "1,2-Dichloroethane-D4",
    "2037-26-5": "Toluene-D8",
    "460-00-4": "4-Bromofluorobenzene",
}

QUERY_TYPE_TO_TABLE = {
    "ds-pfas": "[Jackson].[DSPFAS]",
    "ds-int": "[Jackson].[DSInt]",
    "ds-ext": "[Jackson].[DSExt]"
}

def normalize(text):
    if not text:
        return ''
    # Replace long dash sequences with space, remove punctuation, and collapse spaces
    text = re.sub(r'[-–—]+', ' ', text)
    text = re.sub(r'[^\w\s]', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text.lower().strip()

PARTIAL_MATCH_MAP = {
      normalize("Sum of TOP C4 - C14 Carboxylates and C4"): "Sum of TOP C4 - C14 Carboxylates and C4-C8 Sulfonates",
      normalize("^ C6 - C10 Fraction minus BTEX C6_C10-BTEX(F1)"): "TRH NEPMC6 - C10 Fraction minus BTEX",
      normalize("C10 - C14 Fraction"): "TPH Silica C10 - C14 Fraction",
      normalize("C15 - C28 Fraction"): "TPH Silica C15 - C28 Fraction",
      normalize("C29 - C36 Fraction"): "TPH Silica C29 - C36 Fraction",
      normalize("^ C10 - C36 Fraction (sum)"): "TPH Silica C10 - C36 Fraction (sum)",
      normalize(">C10 - C16 Fraction"): "TRH C10 - C16 Fraction",
      normalize(">C16 - C34 Fraction"): "TRH C16 - C34 Fraction",
      normalize(">C34 - C40 Fraction"): "TRH C34 - C40 Fraction",
      normalize("^ >C10 - C40 Fraction (sum)"): "TRH C10 - C40 Fraction (sum)",
      normalize(">C10 - C16 Fraction minus Naphthalene (F2)"): "TRH C10 - C16 Fraction minus Naphthalene",
      normalize("^ C6 - C10 Fraction minus BTEX C6_C10-BTEX (F1)"): "TRH NEPMC6 - C10 Fraction minus BTEX"
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Parsing multipart form data...")
        content_type = req.headers.get("Content-Type", "")
        if "multipart/form-data" not in content_type:
            return func.HttpResponse(json.dumps({"error": "Expected multipart/form-data", "Details": str(e)}), status_code=400, mimetype="application/json")

        multipart_data = decoder.MultipartDecoder(req.get_body(), content_type)

        file_name, file_content, query_type = None, None, None
        for part in multipart_data.parts:
            content_disp = part.headers.get(b"Content-Disposition", b"").decode()
            if 'filename="' in content_disp and content_disp.endswith('.pdf"'):
                file_content = part.content
                match = re.search(r'filename="(.+?)"', content_disp)
                if match:
                    file_name = match.group(1)
            
            elif 'name="query_type"' in content_disp:
                    query_type = part.text.strip().lower()

        if not file_content:
            return func.HttpResponse(json.dumps({"error": "No PDF file uploaded", "Details": str(e)}), status_code=400, mimetype="application/json")
        if not query_type or query_type not in FIELD_MAP:
            return func.HttpResponse(json.dumps({"error": "Invalid or missing query_type", "Details": str(e)}), status_code=400, mimetype="application/json")

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
                                "File Name": f"'{file_name}'" if file_name else "NULL",
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

                            # Check for known partial match
                            if not match:
                                if normalized_analyte in PARTIAL_MATCH_MAP:
                                    match = PARTIAL_MATCH_MAP[normalized_analyte]
                                    logging.info(f"Partial match override: '{analyte}' → '{match}'")
                                else:
                                    match = None
                            
                            # Match on CAS number if abbreviation fails
                            if not match:
                                cas_hits = re.findall(r'\b\d{2,7}-\d{2}-\d\b', analyte)
                                for cas in cas_hits:
                                    if cas in CAS_TO_FULL:
                                        full_name = CAS_TO_FULL[cas]
                                        if full_name in analyte_fields:
                                            match = full_name
                                            logging.info(f"CAS matched: {cas} → {full_name}")
                                            break
                            
                            # Match abbreviation if fuzzy fails
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
            return func.HttpResponse(json.dumps({"error": "No valid data found in PDF", "Details": str(e)}), status_code=400, mimetype="application/json")

        rows = []
        for row_dict in combined_rows.values():
            row_values = [row_dict.get(field, "NULL") for field in target_fields]
            rows.append(f"           ({', '.join(row_values)})")

        
        table_name = QUERY_TYPE_TO_TABLE.get(query_type)
        if not table_name:
            return func.HttpResponse(json.dumps({"error": f"Invalid query_type: {query_type}", "Details": str(e)}), status_code=400, mimetype="application/json")

        columns_sql = ", ".join([f"[{f}]" for f in target_fields])
        sql = f"INSERT INTO {table_name} ({columns_sql}) VALUES" + ", ".join(rows) + ";"


        try:

            username = os.environ["SQL_USER"]
            password = os.environ["SQL_PASSWORD"]
            server = os.environ["SQL_SERVER"]
            db = os.environ["SQL_DB_LAB"]

            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with pymssql.connect(server, username, password, db) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(sql)
                        conn.commit()
                    break  # success
                except pymssql.OperationalError as e:
                    if attempt < max_retries - 1:
                        logging.warning(f"Retrying DB connection in 5 seconds... Attempt {attempt + 1}")
                        time.sleep(5)
                    else:
                        raise



            logging.info("✅ Data inserted into SQL Server.")
            return func.HttpResponse(
                json.dumps({"status": "success", "inserted_rows": len(rows)}),
                status_code=200,
                mimetype="application/json"
            )

        except Exception as e:
            logging.exception("❌ Database insert failed.")
            return func.HttpResponse(
                json.dumps({"error": "Database insert failed", "details": str(e)}),
                status_code=500,
                mimetype="application/json"
            )

    except Exception as e:
        logging.exception("Unhandled exception")
        return func.HttpResponse(
            json.dumps({"error": "Internal server error", "details": str(e)}),
            status_code=500,
            mimetype="application/json"
        )