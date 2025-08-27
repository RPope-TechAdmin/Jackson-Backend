import os
import time
import logging
from io import BytesIO
from datetime import datetime
from typing import Dict, List, Iterable, Tuple
import azure.functions as func
import pyodbc
import openpyxl

# ========= CONFIG =========
# Read from Function App settings (Configuration → Application settings)
SQL_SERVER = "purenvqld.database.windows.net"
SQL_DATABASE = "Laboratory"
SQL_USERNAME = "reportabledatadownloader"
SQL_PASSWORD = "Rep0r7D47aD0wn"

# Map HTML group ids → fully-qualified table names (include schema!)
GROUP_TO_TABLE: Dict[str, str] = {
    "external": "Jackson.DSExt",     # External Analytes
    "pfastopa": "Jackson.DSPFAS",    # PFAS TOPA
    "internal": "Jackson.DSInt",     # Internal Analytes
}

# Whitelist analyte columns per table to avoid SQL injection via column names.
# ⚠️ 
ALLOWED_COLUMNS: Dict[str, Iterable[str]] = {
    "Jackson.DSExt": {
        "Total Arsenic","Total Beryllium","Total Cadmium","Total Chromium",
        "Total Copper","Total Cobalt","Total Nickel","Total Lead","Total Zinc","Total Manganese","Total Selenium","Total Silver","Total Vanadium",
        "Total Boron","Total Mercury","Total Organic Carbon","TPH Silica C10 - C14 Fraction","TPH Silica C15 - C28 Fraction",
        "TPH Silica C29 - C36 Fraction","TPH Silica C10 - C36 Fraction (sum)","TRH C10 - C16 Fraction","TRH C16 - C34 Fraction",
        "TRH C34 - C40 Fraction","TRH C10 - C40 Fraction (sum)","TRH C10 - C16 Fraction minus Naphthalene","Phenol","2-Chlorophenol","2-Methylphenol",
        "3- & 4-Methylphenol","2-Nitrophenol","2,4-Dimethylphenol","2,6-Dichlorophenol","4-Chloro-3-methylphenol","2,4,6-Trichlorophenol",
        "2,4,5-Trichlorophenol","Pentachlorophenol","Sum of Phenols","TPH C6 - C9 Fraction","TRH NEPMC6 - C10 Fraction C6_C10",
        "TRH NEPMC6 - C10 Fraction minus BTEX","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes",
        "Sum of BTEX","Naphthalene","Escherichia coli","Phenol-d6","2-Chlorophenol-D4","2,4,6-Tribromophenol","2-Fluorobiphenyl",
        "Anthracene-d10","4-Terphenyl-d14","1,2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene", "Sulfate", "Sulfur"
        
    },
    "Jackson.DSPFAS": {
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
    },
    "Jackson.DSInt": {
        "Electrical Conductivity @ 25°C","Nitrite + Nitrate as N",
        "Total Kjeldahl Nitrogen as N","Total Nitrogen as N","Total Phosphorus as P"}
}

# Non-analyte identifier columns you always want back
ID_COLUMNS = ["File Name", "Sample Location", "Sampling Date/Time"]

# ========= DB CONNECT =========
def connect_with_fallback(timeout_seconds: int = 60) -> pyodbc.Connection:
    """
    Try ODBC Driver 18 then 17. Increase Connection Timeout and retry a few times
    (useful if Azure SQL Serverless is resuming).
    """
    drivers = ["ODBC Driver 18 for SQL Server", "ODBC Driver 17 for SQL Server"]
    last_exc = None

    for driver in drivers:
        conn_str = (
            f"Driver={{{driver}}};"
            f"Server=tcp:{SQL_SERVER},1433;"
            f"Database={SQL_DATABASE};"
            f"Uid={SQL_USERNAME};"
            f"Pwd={SQL_PASSWORD};"
            "Encrypt=yes;"
            "TrustServerCertificate=no;"
            f"Connection Timeout={timeout_seconds};"
        )
        for attempt in range(3):
            try:
                return pyodbc.connect(conn_str)
            except Exception as e:
                last_exc = e
                logging.warning(f"Connect attempt {attempt+1}/3 with {driver} failed: {e}")
                time.sleep(3)
    # If we get here, all attempts failed
    raise last_exc

# ========= HELPERS =========
def normalize_payload(data) -> Dict[str, List[str]]:
    """
    Support two payload shapes:
      A) { "selections": { "external": ["arsenic", ...], "pfastopa": ["pfos"] }, "startDate": "...", "endDate": "..." }
      B) { "selections": [ { "table": "Jackson.DSExt", "analyte":"arsenic" }, ... ], ... }
    Returns a dict { group_key_or_table: [analytes...] } keyed by group-id (preferred) if possible.
    """
    sel = data.get("selections", [])
    if isinstance(sel, dict):
        # Already grouped by HTML group id
        return {k: list(v) for k, v in sel.items() if v}
    elif isinstance(sel, list):
        grouped: Dict[str, List[str]] = {}
        for item in sel:
            table = item.get("table")
            analyte = item.get("analyte")
            if not table or not analyte:
                continue
            # Try to reverse-map table back to group id; if not found, use the table name as the key
            group_key = None
            for g, t in GROUP_TO_TABLE.items():
                if t.lower() == table.lower():
                    group_key = g
                    break
            key = group_key if group_key else table
            grouped.setdefault(key, []).append(analyte)
        return grouped
    else:
        return {}

def whitelist_columns(table: str, requested: Iterable[str]) -> List[str]:
    allowed = set(ALLOWED_COLUMNS.get(table, []))
    return [c for c in requested if c in allowed]

def build_select_sql(table: str, analyte_cols: List[str]) -> str:
    # Build "SELECT SampleID, SampleDate, col1, col2 FROM schema.table WHERE SampleDate BETWEEN ? AND ?"
    selected_cols = ID_COLUMNS + analyte_cols
    cols_sql = ", ".join(f"[{c}]" for c in selected_cols)  # bracket-quote identifiers
    return f"SELECT {cols_sql} FROM {table} WHERE [SampleDate] BETWEEN ? AND ?"

def safe_sheet_name(name: str) -> str:
    # Excel sheet name: max 31 chars, no []:*?/\
    bad = '[]:*?/\\'
    for ch in bad:
        name = name.replace(ch, "-")
    return (name or "Sheet")[:31]

# ========= MAIN FUNCTION =========

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing /download-excel request")
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON body.", status_code=400)

    start_date = data.get("startDate")
    end_date = data.get("endDate")
    if not start_date or not end_date:
        return func.HttpResponse("Both startDate and endDate are required.", status_code=400)

    grouped = normalize_payload(data)
    if not grouped:
        return func.HttpResponse("No analytes selected.", status_code=400)

    # Open workbook
    wb = openpyxl.Workbook()
    wb.remove(wb.active)

    try:
        conn = connect_with_fallback(timeout_seconds=60)
        cursor = conn.cursor()

        any_rows_written = False

        for group_key, analytes in grouped.items():
            table = GROUP_TO_TABLE.get(group_key, group_key)
            if table not in ALLOWED_COLUMNS:
                logging.warning(f"Skipping unknown/unauthorized table: {table}")
                continue

            analyte_cols = whitelist_columns(table, analytes)
            if not analyte_cols:
                logging.info(f"No valid analyte columns for {table}, requested: {analytes}")
                continue

            sql = build_select_sql(table, analyte_cols)
            logging.info(f"Running query for {table}: {sql}")
            cursor.execute(sql, (start_date, end_date))
            rows = cursor.fetchall()
            columns = [d[0] for d in cursor.description]

            logging.info(f"Grouped selections: {grouped}")
            logging.info(f"SQL about to run: {sql}")
            logging.info(f"Row count returned: {len(rows)}")


            ws = wb.create_sheet(title=safe_sheet_name(group_key))
            ws.append(columns)

            if rows:
                for row in rows:
                    ws.append(list(row))
                any_rows_written = True
            else:
                ws.append(["No data found for this selection."])

        if not wb.worksheets:  # safety: if nothing created
            ws = wb.create_sheet(title="Results")
            ws.append(["No data found at all."])

        output = BytesIO()
        wb.save(output)
        output.seek(0)
        filename = f"results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return func.HttpResponse(
            body=output.getvalue(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logging.error(f"Download error: {e}", exc_info=True)
        return func.HttpResponse(f"Error: {e}", status_code=500)
    finally:
        try: cursor.close()
        except Exception: pass
        try: conn.close()
        except Exception: pass