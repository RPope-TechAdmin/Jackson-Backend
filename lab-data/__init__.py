import azure.functions as func

cors_headers= {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net",
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    if req.method=="OPTIONS":
        return func.HttpResponse("Function reached successfully.", status_code=200, headers=cors_headers)
