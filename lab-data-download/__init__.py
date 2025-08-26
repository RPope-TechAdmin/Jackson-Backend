import logging
import pyodbc
import openpyxl
from io import BytesIO
from datetime import datetime
import azure.functions as func

def get_connection():
    # Use ODBC Driver 18 (recommended for Azure SQL)
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=purenvqld.database.windows.net;"
        "DATABASE=Laboratory;"
        "UID=reportabledatadownloader;"   # 👈 Must include @servername
        "PWD=Rep0r7D47aD0wn;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    logging.info(f"Connection String: {conn_str}")
    return pyodbc.connect(conn_str)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing /download-excel request")
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    selections = data.get("selections", [])
    start_date = data.get("startDate")
    end_date = data.get("endDate")

    if not selections:
        return func.HttpResponse("No analytes selected", status_code=400)

    # Group analytes by table
    grouped = {}
    for sel in selections:
        grouped.setdefault(sel["table"], []).append(sel["analyte"])

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Data"

    headers_written = False

    try:
        conn = get_connection()
        cursor = conn.cursor()

        for table, analytes in grouped.items():
            placeholders = ",".join(["?"] * len(analytes))
            query = f"""
                SELECT *
                FROM [Jackson].[{table}]
                WHERE Analyte IN ({placeholders})
                AND SampleDate BETWEEN ? AND ?
            """
            params = analytes + [start_date, end_date]
            cursor.execute(query, params)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]

            if not headers_written:
                ws.append(columns)
                headers_written = True

            for row in rows:
                ws.append(list(row))

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return func.HttpResponse(
            body=output.getvalue(),
            status_code=200,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": f"attachment; filename={filename}"}
        )

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(f"Error: {str(e)}", status_code=500)
    finally:
        cursor.close()
        conn.close()
