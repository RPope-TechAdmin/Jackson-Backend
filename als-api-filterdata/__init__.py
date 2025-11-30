import os
import io
import json
import logging
import requests
import azure.functions as func
from datetime import datetime, timedelta
from docx import Document
from pathlib import Path

cors_headers = {
    "Access-Control-Allow-Origin": "https://delightful-tree-0888c340f.1.azurestaticapps.net", 
    "Access-Control-Allow-Methods": "POST, OPTIONS, GET",
    "Access-Control-Allow-Headers": "Content-Type, Accept",
    "Access-Control-Max-Age": "86400"
}

TABLE_FIELD_MAP = {
    "DMS": {
        "File","Sample Date","Sample Location","pH Value","Total Soluble Salts","Moisture Content","Extractable Boron","Calcium","Magnesium","Sodium","Potassium","Arsenic","Barium"
        ,"Boron","Cadmium","Chromium","Copper","Lead","Nickel","Selenium","Silver","Vanadium","Zinc","ICP-MS Arsenic","Mercury","Hexavalent Chromium","Trivalent Chromium"
        ,"C10 - C14 Fraction","C15 - C28 Fraction","C29 - C36 Fraction","C10 - C36 Fraction (sum)",">C10 - C16 Fraction",">C16 - C34 Fraction",">C34 - C40 Fraction",">C10 - C40 Fraction (sum)"
        ,">C10 - C16 Fraction minus Naphthalene (F2)","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol","2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol"
        ,"4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","PAH Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene"
        ,"Fluoranthene","Pyrene","Benz(a)anthracene","Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene"
        ,"Benzo(a)pyrene TEQ (zero)","Benzo(a)pyrene TEQ (half LOR)","Benzo(a)pyrene TEQ (LOR)","Sum of polycyclic aromatic hydrocarbons","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)"
        ,"C6 - C9 Fraction","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes","Sum of BTEX","Naphthalene"
    },  
    "DML": {
        "File","Sample Location","Sample Date","pH Value","Total Dissolved Solids @180°C","Calcium","Magnesium","Potassium","Sodium","Arsenic","Barium","Beryllium","Boron","Cadmium"
        ,"Chromium","Copper","Cobalt","Lead","Nickel","Zinc","Manganese","Selenium","Vanadium","Silver","Mercury","Trivalent Chromium","Hexavalent Chromium","C10 - C14 Fraction","C10 - C36 Fraction (sum)"
        ,"C15 - C28 Fraction","C29 - C36 Fraction",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction"
        ,"C6 - C9 Fraction","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","Acenaphthene","Acenaphthylene","Anthracene","Benz(a)anthracene","Benzo(a)pyrene","Benzo(a)pyrene TEQ (zero)"
        ,"Benzo(b+j)fluoranthene","Benzo(g.h.i)perylene","Benzo(k)fluoranthene","Chrysene","Dibenz(a.h)anthracene","Fluoranthene","Fluorene","Indeno(1.2.3.cd)pyrene","PAH Naphthalene","Phenanthrene"
        ,"Pyrene","Sum of polycyclic aromatic hydrocarbons","2.4.5-Trichlorophenol","2.4.6-Trichlorophenol","2.4-Dichlorophenol","2.4-Dimethylphenol","2.6-Dichlorophenol","2-Chlorophenol"
        ,"2-Methylphenol","2-Nitrophenol","3- & 4-Methylphenol","4-Chloro-3-methylphenol","Pentachlorophenol","Phenol","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene"
        ,"Total Xylenes","Sum of BTEX","Naphthalene"
    },
    "BioRemediation": {
        "File","Sample Date","Sample Name","pH Value","Total Dissolved Solids @180°C","Nitrite + Nitrate as N","Total Kjeldahl Nitrogen as N","Total Nitrogen as N","Total Phosphorus as P"
        ,"Total Organic Carbon","Biochemical Oxygen Demand","Escherichia coli"
    },
    "DustSuppression": {
        "File","Sample Location","Sample Date","Arsenic","Beryllium","Cadmium","Chromium","Copper","Cobalt","Nickel","Lead","Zinc","Manganese","Selenium","Silver","Vanadium","Boron"
        ,"Mercury","Total Organic Carbon","C10 - C14 Fraction","C15 - C28 Fraction","C29 - C36 Fraction","C10 - C36 Fraction (sum)",">C10 - C16 Fraction",">C16 - C34 Fraction",">C34 - C40 Fraction"
        ,">C10 - C40 Fraction (sum)",">C10 - C16 Fraction minus Naphthalene (F2)","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol","2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol"
        ,"2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","Sum of Phenols","C6 - C9 Fraction","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)"
        ,"Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes","Sum of BTEX","Naphthalene","Escherichia coli","Sodium","Potassium","Magnesium","Calcium"
    },
    "EnvironmentalCreek": {
        "File","Sample Date","Sample Location","Suspended Solids (SS)","Total Organic Carbon","Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc","Mercury","Perfluorobutane sulfonic acid (PFBS)"
        ,"Perfluoropentane sulfonic acid (PFPeS)","Perfluorohexane sulfonic acid (PFHxS)","Perfluoroheptane sulfonic acid (PFHpS)","Perfluorooctane sulfonic acid (PFOS)","Perfluorodecane sulfonic acid (PFDS)"
        ,"Perfluorobutanoic acid (PFBA)","Perfluoropentanoic acid (PFPeA)","Perfluorohexanoic acid (PFHxA)","Perfluoroheptanoic acid (PFHpA)","Perfluorooctanoic acid (PFOA)","Perfluorononanoic acid (PFNA)"
        ,"Perfluorodecanoic acid (PFDA)","Perfluoroundecanoic acid (PFUnDA)","Perfluorododecanoic acid (PFDoDA)","Perfluorotridecanoic acid (PFTrDA)","Perfluorotetradecanoic acid (PFTeDA)"
        ,"Perfluorooctane sulfonamide (FOSA)","N-Methyl perfluorooctane sulfonamide (MeFOSA)","N-Ethyl perfluorooctane sulfonamide (EtFOSA)","N-Methyl perfluorooctane sulfonamidoethanol (MeFOSE)"
        ,"N-Ethyl perfluorooctane sulfonamidoethanol (EtFOSE)","N-Methyl perfluorooctane sulfonamidoacetic acid (MeFOSAA)","N-Ethyl perfluorooctane sulfonamidoacetic acid (EtFOSAA)","4:2 Fluorotelomer sulfonic acid (4:2 FTS)"
        ,"6:2 Fluorotelomer sulfonic acid (6:2 FTS)","8:2 Fluorotelomer sulfonic acid (8:2 FTS)","10:2 Fluorotelomer sulfonic acid (10:2 FTS)","Sum of PFAS","Sum of PFHxS and PFOS","Sum of PFAS (WA DER List)"
        ,"Total Organic Fluorine"
    },
    "Irrigation": {
        "File","Sample Date","Sample Name","pH Value","Total Dissolved Solids @180°C","Nitrite + Nitrate as N","Total Kjeldahl Nitrogen as N","Total Nitrogen as N"
        ,"Total Phosphorus as P","Total Organic Carbon","Biochemical Oxygen Demand","Escherichia coli"
    },
    "TreatedEffluent": {
        "File","Sample Date","Sample Name","pH Value","Total Dissolved Solids @180°C","Nitrite + Nitrate as N","Total Kjeldahl Nitrogen as N","Total Nitrogen as N"
        ,"Total Phosphorus as P","Total Organic Carbon","Biochemical Oxygen Demand","Escherichia coli"
    },
    "PFASSolid/Liquid": {
        "File","Sample Location","Sample Date","Perfluorobutane sulfonic acid (PFBS)","Perfluoropentane sulfonic acid (PFPeS)","Perfluorohexane sulfonic acid (PFHxS)","Perfluoroheptane sulfonic acid (PFHpS)"
        ,"Perfluorooctane sulfonic acid (PFOS)","Perfluorodecane sulfonic acid (PFDS)","Perfluorobutanoic acid (PFBA)","Perfluoropentanoic acid (PFPeA)","Perfluorohexanoic acid (PFHxA)"
        ,"Perfluoroheptanoic acid (PFHpA)","Perfluorooctanoic acid (PFOA)","Perfluorononanoic acid (PFNA)","Perfluorodecanoic acid (PFDA)","Perfluoroundecanoic acid (PFUnDA)","Perfluorododecanoic acid (PFDoDA)"
        ,"Perfluorotridecanoic acid (PFTrDA)","Perfluorotetradecanoic acid (PFTeDA)","Perfluorooctane sulfonamide (FOSA)","N-Methyl perfluorooctane sulfonamide (MeFOSA)","N-Ethyl perfluorooctane sulfonamide (EtFOSA)"
        ,"N-Methyl perfluorooctane sulfonamidoethanol (MeFOSE)","N-Ethyl perfluorooctane sulfonamidoethanol (EtFOSE)","N-Methyl perfluorooctane sulfonamidoacetic acid (MeFOSAA)","N-Ethyl perfluorooctane sulfonamidoacetic acid (EtFOSAA)"
        ,"4:2 Fluorotelomer sulfonic acid (4:2 FTS)","6:2 Fluorotelomer sulfonic acid (6:2 FTS)","8:2 Fluorotelomer sulfonic acid (8:2 FTS)","10:2 Fluorotelomer sulfonic acid (10:2 FTS)"
        ,"Sum of PFAS","Sum of PFHxS and PFOS","Sum of PFAS (WA DER List)"
    },
    "Spadable": {
        "File","Sample Date","Sample Location","Moisture Content","pH Value","Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc","Mercury","Selenium","C10 - C14 Fraction"
        ,"C15 - C28 Fraction","C29 - C36 Fraction","C10 - C36 Fraction (sum)",">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C16 - C34 Fraction",">C34 - C40 Fraction"
        ,">C10 - C40 Fraction (sum)","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","C6 - C9 Fraction","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes"
        ,"Sum of BTEX","Naphthalene","2.4.5-Trichlorophenol","2.4.6-Trichlorophenol","2.4-Dichlorophenol","2.4-Dimethylphenol","2.6-Dichlorophenol","2-Chlorophenol","2-Methylphenol","2-Nitrophenol"
        ,"3- & 4-Methylphenol","4-Chloro-3-methylphenol","Pentachlorophenol","Phenol","Acenaphthene","Acenaphthylene","Anthracene","Benz(a)anthracene","Benzo(a)pyrene","Benzo(a)pyrene TEQ (half LOR)"
        ,"Benzo(a)pyrene TEQ (LOR)","Benzo(a)pyrene TEQ (zero)","Benzo(b+j)fluoranthene","Benzo(g.h.i)perylene","Benzo(k)fluoranthene","Chrysene","Dibenz(a.h)anthracene","Fluoranthene","Fluorene"
        ,"Indeno(1.2.3.cd)pyrene","PAH Naphthalene","Phenanthrene","Pyrene","Sum of polycyclic aromatic hydrocarbons","Sodium"
    }
}
TEST_CODES = {
    "EG035T": {
        "Mercury"
    },
    "EP071SG": {
        ">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)"
        "C15 - C28 Fraction","C29 - C36 Fraction"
    },
    "EG020A-T": {
       " Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc","Beryllium","Boron","Cobalt","Manganese","Selenium","Vanadium"
    },
    "EP080": {
        "Benzene","C6 - C10 Fraction","C6 - C10 Fraction minus BTEX (F1)","C6 - C9 Fraction","Ethylbenzene","meta- & para-Xylene","Naphthalene","ortho-Xylene","Sum of BTEX","Toluene"
        "Total Xylenes"
    },
    "EP075(SIM)": {
        "2.4.5-Trichlorophenol","2.4.6-Trichlorophenol","2.4-Dichlorophenol","2.4-Dimethylphenol","2.6-Dichlorophenol","2-Chlorophenol","2-Methylphenol","2-Nitrophenol","3- & 4-Methylphenol"
        "4-Chloro-3-methylphenol","Pentachlorophenol","Phenol","Sum of Phenols"
    },
    "EG020B-T": {
        "Silver"
    },
    "MW006": {
        "Escherichia coli"
    },
    "EP005"	: {
        "Total Organic Carbon"
    },
    "EP231X": {
        "10:2 Fluorotelomer sulfonic acid (10:2 FTS)","4:2 Fluorotelomer sulfonic acid (4:2 FTS)","6:2 Fluorotelomer sulfonic acid (6:2 FTS)","8:2 Fluorotelomer sulfonic acid (8:2 FTS)"
        "N-Ethyl perfluorooctane sulfonamide (EtFOSA)","N-Ethyl perfluorooctane sulfonamidoacetic acid (EtFOSAA)","N-Ethyl perfluorooctane sulfonamidoethanol (EtFOSE)","N-Methyl perfluorooctane sulfonamide (MeFOSA)"
        "N-Methyl perfluorooctane sulfonamidoacetic acid (MeFOSAA)","N-Methyl perfluorooctane sulfonamidoethanol (MeFOSE)","Perfluorobutane sulfonic acid (PFBS)","Perfluorobutanoic acid (PFBA)"
        "Perfluorodecane sulfonic acid (PFDS)","Perfluorodecanoic acid (PFDA)","Perfluorododecanoic acid (PFDoDA)","Perfluoroheptane sulfonic acid (PFHpS)","Perfluoroheptanoic acid (PFHpA)"
        "Perfluorohexane sulfonic acid (PFHxS)","Perfluorohexanoic acid (PFHxA)","Perfluorononanoic acid (PFNA)","Perfluorooctane sulfonamide (FOSA)","Perfluorooctane sulfonic acid (PFOS)"
        "Perfluorooctanoic acid (PFOA)","Perfluoropentane sulfonic acid (PFPeS)","Perfluoropentanoic acid (PFPeA)","Perfluorotetradecanoic acid (PFTeDA)","Perfluorotridecanoic acid (PFTrDA)"
        "Perfluoroundecanoic acid (PFUnDA)","Sum of PFAS","Sum of PFAS (WA DER List)","Sum of PFHxS and PFOS"
    },
    "EP040": {
        "Total Organic Fluorine"	

    },
    "EA025H": {
        "Suspended Solids (SS)"
    },
    "EA055": {
        "Moisture Content"
    },
    "EG005T": {
        "Arsenic","Cadmium","Chromium","Copper","Lead","Nickel","Zinc"
    },
    "EP071SG-S": {
        ">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)"
        "C15 - C28 Fraction","C29 - C36 Fraction"
    },
    "EA002": {
        "pH Value"
    },
    "EG048G": {
        "Hexavalent Chromium"
    },
    "EG049G-Alk": {
        "Trivalent Chromium"
    },
    "EG020X-T": {
        "Arsenic"
    },
    "ED093S": {
        "Calcium","Magnesium","Potassium","Sodium"
    },
    "ED091": {
        "Boron"
    },
    "EA014": {
        "Total Soluble Salts"
    },
    "EN34": {
        "pH Value"
    },
    "EA005-P": {
        "pH Value"
    },
    "EG049G-T": {
        "Trivalent Chromium"
    },
    "EG050G-T": {
        "Hexavalent Chromium"
    },
    "ED093F": {
        "Calcium","Magnesium","Potassium","Sodium"
    },
    "EA015H": {
        "Total Dissolved Solids @180°C"
    },
    "EP030": {
        "Biochemical Oxygen Demand"
    },
    "EK067G": {
        "Total Phosphorus as P"
    },
    "EK062G": {
        "Total Nitrogen as N"
    },
    "EK061G": {
        "Total Kjeldahl Nitrogen as N"
    },
    "EK059G": {
        "Nitrite + Nitrate as N"
    },
    "EP071": {
        ">C10 - C16 Fraction",">C10 - C16 Fraction minus Naphthalene (F2)",">C10 - C40 Fraction (sum)",">C16 - C34 Fraction",">C34 - C40 Fraction","C10 - C14 Fraction","C10 - C36 Fraction (sum)"
        "C15 - C28 Fraction","C29 - C36 Fraction"
    },
    "EP231X (TOP)": {
        "Perfluorobutane sulfonic acid (PFBS)","Perfluorohexane sulfonic acid (PFHxS)","Perfluorooctane sulfonic acid (PFOS)","Perfluorobutanoic acid (PFBA)","Perfluorohexanoic acid (PFHxA)"
        "Perfluorooctanoic acid (PFOA)","Perfluorodecanoic acid (PFDA)","Perfluorododecanoic acid (PFDoDA)","Perfluorotetradecanoic acid (PFTeDA)","N-Methyl perfluorooctane sulfonamide (MeFOSA)"
        "N-Methyl perfluorooctane sulfonamidoethanol (MeFOSE)","N-Methyl perfluorooctane sulfonamidoacetic acid (MeFOSAA)","4:2 Fluorotelomer sulfonic acid (4:2 FTS)","8:2 Fluorotelomer sulfonic acid (8:2 FTS)"
        "Sum of PFAS","Sum of TOP C4 - C14 Carboxylates and C4 - C8 Sulfonates","Perfluoropentane sulfonic acid (PFPeS)","Perfluoroheptane sulfonic acid (PFHpS)","Perfluorodecane sulfonic acid (PFDS)"
        "Perfluoropentanoic acid (PFPeA)","Perfluoroheptanoic acid (PFHpA)","Perfluorononanoic acid (PFNA)","Perfluoroundecanoic acid (PFUnDA)","Perfluorotridecanoic acid (PFTrDA)","Perfluorooctane sulfonamide (FOSA)"
        "N-Ethyl perfluorooctane sulfonamide (EtFOSA)","N-Ethyl perfluorooctane sulfonamidoethanol (EtFOSE)","N-Ethyl perfluorooctane sulfonamidoacetic acid (EtFOSAA)","6:2 Fluorotelomer sulfonic acid (6:2 FTS)"
        "10:2 Fluorotelomer sulfonic acid (10:2 FTS)","Sum of PFHxS and PFOS","Sum of TOP C4 - C14 as Fluorine"
    },
}

PROJECT_MAP = {
    "Drill Mud Liquids":"DML",
    "Drill Mud Solids": "DMS",
    "Dust Suppression": "Dust Suppression",
    "Environmental Creek Testing": "Environmental Creek",
    "PFAS Solid/Liquid": "PFAS Solid/Liquid",
    "SBR Irr": "Irrigation",
    "Spadable Samples": "Spadable",
    "Stormwater": "Stormwater",
    "Treated Effluent": "TreatedEffluent",
    "BR project": "BioRemediation",
    "BR Project": "BioRemediation"
}

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Fetching and filtering lab data to generate SQL...")

    try:
        # === Environment variables ===
        auth_url = os.environ["API_AUTH_URL"]
        data_url = os.environ["API_DATA_URL"]
        username = os.environ["API_USERNAME"]
        password = os.environ["API_PASSWORD"]

        # === Get request parameters ===
        project_no = req.params.get("project_no")
        workorder_code = req.params.get("workorder_code")
        from_days_ago = int(req.params.get("from_days_ago", 40))

        # Default: last 7 days, page=1
        to_dt = datetime.utcnow()
        from_dt = to_dt - timedelta(days=from_days_ago)
        from_param = from_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        to_param = to_dt.strftime("%Y/%m/%d %H:%M:%S.000Z")
        
        # TODO: Implement pagination if more than one page of results is expected
        page_param = "16"

        # === Step 1: Authenticate ===
        auth_headers = {
            "Accept": "application/json",
            "Content-Type": "application/json; charset=utf-8",
        }
        auth_payload = {
            "Username": username,
            "Password": password,
        }

        auth_resp = requests.post(auth_url, headers=auth_headers, json=auth_payload, timeout=60)
        auth_resp.raise_for_status()
        auth_data = auth_resp.json()

        # Support multiple possible token structures
        token = (
            auth_data.get("Token")
            or auth_data.get("token")
            or (auth_data.get("Data", {}).get("Token"))
            or (auth_data.get("data", {}).get("token"))
        )
        if not token:
            raise ValueError(f"No token found in auth response: {auth_data}")
        
        # === Step 2: Fetch ALL PAGES of data ===
        def extract_records(api_data):
            """Extracts normalized list of records from any supported API structure."""
            if isinstance(api_data, dict):
                # Format A: {"Results": [...]}
                if "Results" in api_data and isinstance(api_data["Results"], list):
                    return api_data["Results"]

                # Format B: {"data": "[{...}, {...}]"} where "data" is a JSON string
                if "data" in api_data and isinstance(api_data["data"], str):
                    try:
                        return json.loads(api_data["data"])
                    except Exception:
                        logging.error("Failed to parse 'data' JSON string.")
                        return []

                # Format C: nested Data.Results
                if "Data" in api_data and "Results" in api_data["Data"]:
                    return api_data["Data"]["Results"]

            # Fallback: assume raw list
            if isinstance(api_data, list):
                return api_data

            logging.warning("Unrecognized API format. Returning empty result set.")
            return []

        all_records = []
        current_page = 1

        # First request (page 1)
        params = {"From": from_param, "To": to_param, "Page": str(current_page)}
        data_headers = {"Accept": "application/json", "Authorization": f"Bearer {token}"}

        logging.info(f"Fetching page {current_page}...")

        resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)
        if resp.status_code == 401:
            data_headers["Authorization"] = token
            resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)

        resp.raise_for_status()
        data_json = resp.json()

        # Extract page 1's data
        page_records = extract_records(data_json)
        all_records.extend(page_records)

        # Determine total pages
        total_pages = (
            data_json.get("TotalPages")
            or data_json.get("totalPages")
            or data_json.get("Pages")
        )

        # Compute pages if API provides counts instead
        if not total_pages:
            total_count = (
                data_json.get("TotalCount")
                or data_json.get("totalCount")
                or data_json.get("Count")
            )
            page_size = (
                data_json.get("PageSize")
                or data_json.get("pageSize")
                or len(page_records)
            )

            if total_count and page_size:
                total_pages = max(1, (total_count + page_size - 1) // page_size)

        if not total_pages:
            logging.info("API does not provide page counts. Assuming only 1 page.")
            total_pages = 1

        logging.info(f"Total pages detected: {total_pages}")

        # Fetch remaining pages
        for current_page in range(2, int(total_pages) + 1):
            logging.info(f"Fetching page {current_page}...")

            params["Page"] = str(current_page)
            resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)
            if resp.status_code == 401:
                data_headers["Authorization"] = token
                resp = requests.get(data_url, headers=data_headers, params=params, timeout=60)

            resp.raise_for_status()
            page_json = resp.json()

            page_records = extract_records(page_json)
            all_records.extend(page_records)

        logging.info(f"Total combined records fetched: {len(all_records)}")

        # Replace old sample_records with combined data
        sample_records = all_records

        # === Step 3: Process data and generate SQL ===
        sql_statements = process_lab_json(
            sample_records,
            project_no=project_no,
            workorder_code=workorder_code
        )

        # === Step 4: Return SQL file ===
        sql_content = "\n".join(sql_statements)
        filename = f"lab_data_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.sql"

        return func.HttpResponse(
            body=sql_content,
            mimetype="application/sql",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    except Exception as e:
        logging.error(f"Error: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            mimetype="application/json",
            status_code=500,
        )

def build_sql_insert(sample_records, project_table):
    """
    Build one SQL INSERT per sample group.
    Includes all mapped analytes as columns; NULL where not found.
    """
    logging.info(f"Building SQL for project table: {project_table}")
    logging.info(f"Type of sample_records in build_sql_insert: {type(sample_records)}")
    fields = TABLE_FIELD_MAP.get(project_table, set())
    if not fields:
        logging.warning(f"No field mapping for table {project_table}")
        return None

    first_record = sample_records[0]
    values = {field: "NULL" for field in fields}
    logging.info(f"Type of first_record in build_sql_insert: {type(first_record)}")
    logging.info(f"First record content: {str(first_record)[:500]}")

    # Static fields
    if "File" in fields:
        values["File"] = f"'{first_record.get('Submission', '')}'"
    if "Sample Location" in fields:
        values["Sample Location"] = f"'{first_record.get('SampleID1', '')}'"
    if "Sample Name" in fields:
        values["Sample Name"] = f"'{first_record.get('SampleID1', '')}'"
    if "Sample Date" in fields:
        sample_date = first_record.get("SampleDate", "")
        if sample_date:
            try:
                parsed_date = datetime.strptime(sample_date, "%d/%m/%Y").strftime("%Y-%m-%d")
            except ValueError:
                parsed_date = sample_date
        else:
            parsed_date = ""
        values["Sample Date"] = f"'{parsed_date}'"

    # Fill analytes
    for rec in sample_records:
        compound = rec.get("Compound")
        result = rec.get("Result")
        if compound in fields and result not in [None, ""]:
            values[compound] = f"'{result}'"

    # Generate SQL
    field_list = ", ".join([f"[{f}]" for f in fields])
    value_list = ", ".join([values[f] for f in fields])
    sql = f"INSERT INTO [Jackson].[{project_table}] ({field_list}) VALUES ({value_list});"
    return sql


def process_lab_json(data, project_no=None, workorder_code=None):
    """
    Groups JSON lab data by sample and generates SQL inserts.
    """
    logging.info(f"Processing lab JSON. Data type: {type(data)}")
    if isinstance(data, str):
        logging.info("Data is a string, attempting to parse JSON.")
        data = json.loads(data)

    logging.info(f"Data type after initial check: {type(data)}")

    # Optional filtering
    logging.info(f"Filtering with project_no: '{project_no}' and workorder_code: '{workorder_code}'")

    def norm(val):
        """Normalize for reliable matching."""
        if val is None:
            return ""
        return str(val).strip().lower().replace("(", "").replace(")", "")

    pn = norm(project_no)
    wo = norm(workorder_code)

    filtered = [
        rec for rec in data
        if (not pn or norm(rec.get("ProjectNo")) == pn)
        and (not wo or norm(rec.get("WorkorderCode")) == wo)
    ]

    logging.info(f"Found {len(filtered)} records after filtering.")
    if filtered:
        logging.info(f"First filtered record: {str(filtered[0])[:500]}")
    if not filtered:
        logging.warning("No matching records found.")
        return []

    # Group by (Submission, SampleID1, SampleDate)
    grouped = {}
    for rec in filtered:
        key = (rec.get("Submission"), rec.get("SampleID1"), rec.get("SampleDate"))
        grouped.setdefault(key, []).append(rec)

    sql_statements = []

    # PATCH A — determine project table **per group**
    for records in grouped.values():

        record_project = records[0].get("ProjectNo") or records[0].get("Site")
        project_table = PROJECT_MAP.get(record_project)

        if not project_table:
            logging.warning(f"No project table found for project: {record_project}")
            continue

        sql = build_sql_insert(records, project_table)
        if sql:
            sql_statements.append(sql)

    return sql_statements

def write_sql_to_file(sql_statements, output_path="output_inserts.sql"):
    """
    Write all generated SQL statements to a file for review.
    """
    path = Path(output_path)
    path.write_text("\n".join(sql_statements))
    logging.info(f"✅ Wrote {len(sql_statements)} SQL statements to {path.resolve()}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)

    # Example usage
    with open("sample_lab_data.json", "r") as f:
        lab_data = json.load(f)

    sqls = process_lab_json(lab_data, project_no="88798", workorder_code="EB2537666")

    if sqls:
        write_sql_to_file(sqls)
        print(f"Generated {len(sqls)} SQL insert statements.")
    else:
        print("No SQL statements generated.")