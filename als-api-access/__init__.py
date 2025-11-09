# File: /HttpTriggerAPI/__init__.py
import os
import json
import logging
import requests
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("ALS API authentication triggered.")
    try:
        api_url = "https://als-client-api-testing.azurewebsites.net/api/user/authenticate"

        # Load credentials from environment variables
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        headers = {
            "accept": "application/json;odata.metadata=minimal;odata.streaming=true",
            "Content-Type": "application/json;odata.metadata=minimal;odata.streaming=true",
        }

        payload = {
            "Data": "string",  # If required by the API spec; otherwise remove
            "Username": username,
            "Password": password,
        }

        # Make POST request to the authentication endpoint
        resp = requests.post(api_url, headers=headers, json=payload, timeout=10)
        resp.raise_for_status()
        token_response = resp.json()

        return func.HttpResponse(
            json.dumps({"status": "success", "token_response": token_response}),
            mimetype="application/json",
            status_code=200,
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )
