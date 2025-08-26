import logging
import os
import pymssql
import openpyxl
from io import BytesIO
from datetime import datetime
import azure.functions as func

cors_headers = {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

def get_connection():
    return pymssql.connect(
        server= os.environ["SQL_SERVER"],
        user=os.environ["SQL_USERNAME_DOWNLOAD"],
        password=os.environ["SQL_PASSWORD_DOWNLOAD"],
        database= os.environ["SQL_DB_LAB"]
    )

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing excel download request")
    try:
        data = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    selections = data.get("selections", [])
    start_date = data.get("startDate")
    end_date = data.get("endDate")

    if not selections:
        return func.HttpResponse("No analytes selected", status_code=400)

    # ✅ Group analytes by table
    grouped = {}
    for sel in selections:
        grouped.setdefault(sel["table"], []).append(sel["analyte"])

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Reportable Data"

    headers_written = False

    try:
        conn = get_connection()
        cursor = conn.cursor()

        for table, analytes in grouped.items():
            placeholders = ",".join(["%s"] * len(analytes))
            query = f"""
                SELECT *
                FROM {table}
                WHERE Analyte IN ({placeholders})
                AND SampleDate BETWEEN %s AND %s
            """
            params = analytes + [start_date, end_date]
            cursor.execute(query, tuple(params))
            rows = cursor.fetchall()
            columns = [col[0] for col in cursor.description]

            if not headers_written:
                ws.append(columns)
                headers_written = True

            for row in rows:
                ws.append(list(row))

        output = BytesIO()
        wb.save(output)
        output.seek(0)

        filename = f"Reportable Data Downloaded {datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

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