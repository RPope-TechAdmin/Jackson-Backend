# File: /HttpTriggerAPI/__init__.py
import os
import json
import logging
import requests
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Authenticating with ALS API...")
    try:
        api_url = "https://als-client-api.azurewebsites.net/api/user/authenticate"

        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        headers = {
            "accept": "application/json;odata.metadata=minimal;odata.streaming=true",
            "Content-Type": "application/json;odata.metadata=minimal;odata.streaming=true",
        }

        # Some ALS API instances reject "Data": "string"; using null is often correct.
        payload = {
            "Username": username,
            "Password": password,
        }

        response = requests.post(api_url, headers=headers, json=payload, timeout=10)

        if response.status_code == 401:
            logging.error("Unauthorized: Check credentials or API environment.")
            return func.HttpResponse(
                json.dumps({
                    "error": "Unauthorized. Verify credentials or API URL.",
                    "details": response.text,
                }),
                mimetype="application/json",
                status_code=401,
            )

        response.raise_for_status()
        token = response.json()

        return func.HttpResponse(
            json.dumps({"status": "success", "response": token}),
            mimetype="application/json",
            status_code=200,
        )

    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
