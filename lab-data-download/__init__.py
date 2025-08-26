from flask import Flask, request, send_file, jsonify
import pymssql
import openpyxl
from io import BytesIO
import os
from datetime import datetime

app = Flask(__name__)

cors_headers = {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}
# DB configuration
DB_CONFIG = {
    "server": os.environ["SQL_SERVER"],
    "database": os.environ["SQL_DB_LAB"],
    "username": os.environ["SQL_USERNAME_DOWNLOAD"],
    "password": os.environ["SQL_PASSWORD_DOWNLOAD"]
}

def get_connection():
    return pymssql.connect(
        server=DB_CONFIG["server"],
        user=DB_CONFIG["username"],
        password=DB_CONFIG["password"],
        database=DB_CONFIG["database"]
    )

@app.route("/download-excel", methods=["POST"])
def download_excel():
    data = request.get_json()
    selections = data.get("selections", [])
    start_date = data.get("startDate")
    end_date = data.get("endDate")

    if not selections:
        return jsonify({"error": "No analytes selected"}), 400
    if not start_date or not end_date:
        return jsonify({"error": "Date range required"}), 400

    try:
        conn = get_connection()
        cursor = conn.cursor()

        # Build query dynamically with placeholders
        placeholders = ",".join(["%s"] * len(selections))
        query = f"""
            SELECT *
            FROM YourTable
            WHERE Analyte IN ({placeholders})
            AND SampleDate BETWEEN %s AND %s
        """

        params = selections + [start_date, end_date]
        cursor.execute(query, tuple(params))

        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]

        # Create Excel workbook
        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Data"

        # Write headers
        ws.append(columns)

        # Write rows
        for row in rows:
            ws.append(list(row))

        # Save to memory
        output = BytesIO()
        wb.save(output)
        output.seek(0)

        # Send file to browser
        filename = f"data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        print("Error:", e)
        return jsonify({"error": str(e)}), 500
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    app.run(port=3000, debug=True)
