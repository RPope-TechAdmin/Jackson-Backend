# File: /HttpTriggerAPI/__init__.py
import os
import json
import logging
import requests
import azure.functions as func

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("API proxy function triggered.")
    try:
        # Load secrets from environment variables
        api_base_url = os.environ["API_BASE_URL"]
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        # Authenticate with third-party API
        auth_resp = requests.post(
            "https://als-client-api.azurewebsites.net/api/user/authenticate",
            json={"Data": "string", "Username": username, "Password": password},
            timeout=10,
        )
        auth_resp.raise_for_status()
        token = auth_resp.json().get("Token")
        rtoken=auth_resp.json().get("RefreshToken")
        logging.info(f"Token: {token}, Refresh Token: {rtoken}")
        if not token or not rtoken:
            raise ValueError("Authentication or Refresh token not found in response.")

        # Access a protected endpoint
        data_resp = requests.get(
            f"{api_base_url}/data",
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
        )
        data_resp.raise_for_status()
        data = data_resp.json()

        return func.HttpResponse(
            json.dumps({"status": "success", "data": data}),
            mimetype="application/json",
            status_code=200,
        )

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
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
