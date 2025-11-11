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
        "Total Copper","Total Cobalt","Total Nickel","Total Lead","Total Zinc","Total Manganese","Total Selenium","Total Silver","Total Vanadium",
        "Total Boron","Total Mercury","Total Organic Carbon","TPH Silica C10 - C14 Fraction","TPH Silica C15 - C28 Fraction",
        "TPH Silica C29 - C36 Fraction","TPH Silica C10 - C36 Fraction (sum)","TRH C10 - C16 Fraction","TRH C16 - C34 Fraction",
        "TRH C34 - C40 Fraction","TRH C10 - C40 Fraction (sum)","TRH C10 - C16 Fraction minus Naphthalene","Phenol","2-Chlorophenol","2-Methylphenol",
        "3- & 4-Methylphenol","2-Nitrophenol","2,4-Dimethylphenol","2,6-Dichlorophenol","4-Chloro-3-methylphenol","2,4,6-Trichlorophenol",
        "2,4,5-Trichlorophenol","Pentachlorophenol","Sum of Phenols","TPH C6 - C9 Fraction","TRH NEPMC6 - C10 Fraction C6_C10",
        "TRH NEPMC6 - C10 Fraction minus BTEX","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes",
        "Sum of BTEX","Naphthalene","Escherichia coli","Phenol-d6","2-Chlorophenol-D4","2,4,6-Tribromophenol","2-Fluorobiphenyl",
        "Anthracene-d10","4-Terphenyl-d14","1,2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene", "Sulfate", "Sulfur"
    ],
    "ineff": [
        "File", "Sample Date", "pH", "TDS", "Nitrate", "Kjeldahl", "Total Nitrogen", "Total Phosphorus", "Total Organic Carbon", "Biochemical Oxygen Demand", "E. coli"
    ],
    "treff": [
        "File", "Sample Date", "pH", "TDS", "Nitrate", "Kjeldahl", "Total Nitrogen", "Total Phosphorus", "Total Organic Carbon", "Biochemical Oxygen Demand", "E. coli"
    ],
    "d6full": [
        "File","Sample Date","pH","TDS","Calcium","Magnesium","Sodium","Potassium","Arsenic","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Nickel","Selenium"
        ,"Silver","Vanadium","Zinc","Boron","Mercury","Trivalent Chromium","Hexavalent Chromium","TPH C6-C9","TPH C10-C14","TPH C15-C28","TPH C29-C36","TPH C10-C36 Sum"
        ,"TRH C10-C16","TRH C16-C34","TRH C34-C40","TRH C10-C40 Sum","TRH C10-C16 Minus Naphthalene","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol","2-Nitrophenol"
        ,"2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol"
        ,"PAH Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene","Chrysene","Benzo(b+j)fluoranthene"
        ,"Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons","Benzo(a)pyrene TEQ (zero)"
        ,"TRH C6-C10","C6-C10 Minus BTEX","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene","Total Xylenes","Sum of BTEX","BTEXN Naphthalene","Phenol-d6"
        ,"2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14","1.2-Dichloroethane-D4","Toluene-D8","4-Bromofluorobenzene"
    ],
    "d6int": [
        "File","Sample Date","Sulfate as SO4 - Turbidimetric","Chloride","Fluoride","Total Organic Carbon","Chemical Oxygen Demand"
    ],
    "d6pfas": [
        "File","Sample Date","Perfluoropropane sulfonic acid (PFPrS)","Perfluorobutane sulfonic acid (PFBS)","Perfluoropentane sulfonic acid (PFPeS)","Perfluorohexane sulfonic acid (PFHxS)"
        ,"Perfluoroheptane sulfonic acid (PFHpS)","Perfluorooctane sulfonic acid (PFOS)","Perfluorononane sulfonic acid (PFNS)","Perfluorodecane sulfonic acid (PFDS)","Perfluorobutanoic acid (PFBA)"
        ,"Perfluoropentanoic acid (PFPeA)","Perfluorohexanoic acid (PFHxA)","Perfluoroheptanoic acid (PFHpA)","Perfluorooctanoic acid (PFOA)","Perfluorononanoic acid (PFNA)","Perfluorodecanoic acid (PFDA)"
        ,"Perfluoroundecanoic acid (PFUnDA)","Perfluorododecanoic acid (PFDoDA)","Perfluorotridecanoic acid (PFTrDA)","Perfluorotetradecanoic acid (PFTeDA)","Perfluorohexadecanoic acid (PFHxDA)"
        ,"Perfluorooctane sulfonamide (FOSA)","N-Methyl perfluorooctane sulfonamide (MeFOSA)","N-Ethyl perfluorooctane sulfonamide (EtFOSA)","N-Methyl perfluorooctane sulfonamidoethanol (MeFOSE)"
        ,"N-Ethyl perfluorooctane sulfonamidoethanol (EtFOSE)","N-Methyl perfluorooctane sulfonamidoacetic acid (MeFOSAA)","N-Ethyl perfluorooctane sulfonamidoacetic acid (EtFOSAA)"
        ,"4:2 Fluorotelomer sulfonic acid (4:2 FTS)","6:2 Fluorotelomer sulfonic acid (6:2 FTS)","8:2 Fluorotelomer sulfonic acid (8:2 FTS)","10:2 Fluorotelomer sulfonic acid (10:2 FTS)"
        ,"Sum of PFAS","Sum of PFHxS and PFOS","Sum of TOP C4 - C14 Carboxylates and C4 - C8 Sulfonates","Sum of TOP C4 - C14 as Fluorine","Sum of PFAS (WA DER List)","13C4-PFOS","13C8-PFOA"
    ],
    "d7full": [
        "File","Sample Date","pH","TDS","Calcium","Magnesium","Sodium","Potassium","Arsenic","Cadmium","Chromium","Cobalt","Copper","Lead","Manganese","Nickel","Selenium","Silver"
        ,"Vanadium","Zinc","Boron","Mercury","Trivalent Chromium","Hexavalent Chromium","TPH C6-C9","TPH C10-C14","TPH C15-C28","TPH C29-C36","TPH C10-C36 Sum","TRH C10-C16","TRH C16-C34"
        ,"TRH C34-C40","TRH C10-C40 Sum","TRH C10-C16 Minus Naphthalene","Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol","2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol"
        ,"2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol","2.4.5-Trichlorophenol","Pentachlorophenol","PAH Naphthalene","Acenaphthylene","Acenaphthene","Fluorene"
        ,"Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene","Chrysene","Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene"
        ,"Benzo(g.h.i)perylene","Sum of polycyclic aromatic hydrocarbons","Benzo(a)pyrene TEQ (zero)","TRH C6-C10","C6-C10 Minus BTEX","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene"
        ,"ortho-Xylene","Total Xylenes","Sum of BTEX","BTEXN Naphthalene","Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14","1.2-Dichloroethane-D4"
        ,"Toluene-D8","4-Bromofluorobenzene"
    ],
    "ccpcomp": [
        "File","Sample Date","Sample Location","pH Value","Total Soluble Salts","Moisture Content","Extractable Boron","Calcium","Magnesium","Sodium","Potassium","Arsenic","Barium"
        ,"Boron","Cadmium","Chromium","Copper","Lead","Nickel","Selenium","Silver","Vanadium","Zinc","Arsenic ICP-MS","Mercury","Hexavalent Chromium","Trivalent Chromium","C10 - C14 Fraction"
        ,"C15 - C28 Fraction","C29 - C36 Fraction","C10 - C36 Fraction (sum)",">C10 - C16 Fraction",">C16 - C34 Fraction",">C34 - C40 Fraction",">C10 - C40 Fraction (sum)",">C10 - C16 Fraction minus Naphthalene (F2)"
        ,"Phenol","2-Chlorophenol","2-Methylphenol","3- & 4-Methylphenol","2-Nitrophenol","2.4-Dimethylphenol","2.4-Dichlorophenol","2.6-Dichlorophenol","4-Chloro-3-methylphenol","2.4.6-Trichlorophenol"
        ,"2.4.5-Trichlorophenol","Pentachlorophenol","PAH Naphthalene","Acenaphthylene","Acenaphthene","Fluorene","Phenanthrene","Anthracene","Fluoranthene","Pyrene","Benz(a)anthracene","Chrysene"
        ,"Benzo(b+j)fluoranthene","Benzo(k)fluoranthene","Benzo(a)pyrene","Indeno(1.2.3.cd)pyrene","Dibenz(a.h)anthracene","Benzo(g.h.i)perylene","Benzo(a)pyrene TEQ (zero)","Benzo(a)pyrene TEQ (half LOR)"
        ,"Benzo(a)pyrene TEQ (LOR)","Total PAH's","C6 - C9 Fraction","C6 - C10 Fraction","C6 - C10 Fraction  minus BTEX (F1)","Benzene","Toluene","Ethylbenzene","meta- & para-Xylene","ortho-Xylene"
        ,"Total Xylenes","Sum of BTEX","BTEXN Naphthalene","Phenol-d6","2-Chlorophenol-D4","2.4.6-Tribromophenol","2-Fluorobiphenyl","Anthracene-d10","4-Terphenyl-d14","1.2-Dichloroethane-D4"
        ,"Toluene-D8","4-Bromofluorobenzene"
    ],
    "in-situ": [
        "Sample Location","Sample Date","pH","EC","TDS","DO"
    ],
    "lfsw": [
        "Sample Date","DateR2","pHR1","pHR2","TDSR1","TDSR2","ECR1","ECR2","DOR1","DOR2","TSSR1","TSSR2","TOCR1","TOCR2"
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
    "14808-79-8": "Sulfate",
    "63705-05-5": "Sulfur",
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

RAW_NON_ANALYTE_LABELS = [
    "results", "result", "cas number", "parameter", "compound", "unit",
    "sampling date", "sample id", "sub-matrix", "matrix",
    "ep075", "ep080", "eg020t", "phenolic compounds", "btexn",
    "surrogate", "notes", "qc", "page", "work order", "client", "project",
    "EG020T: Total Metals by ICP-MS","EG035T: Total Recoverable Mercury by FIMS","EP005: Total Organic Carbon (TOC)",
    "EP071 SG: Total Petroleum Hydrocarbons - Silica gel cleanup","EP071 SG: Total Recoverable Hydrocarbons - NEPM 2013 Fractions - Silica gel cleanup",
    "EP071 SG: Total Recoverable Hydrocarbons - NEPM 2013 Fractions - Silica gel cleanup - Continued",
    "EP075(SIM)A: Phenolic Compounds","EP080/071: Total Petroleum Hydrocarbons","EP080/071: Total Recoverable Hydrocarbons - NEPM 2013 Fractions",
    "EP080: BTEXN","MW006: Thermotolerant Coliforms & E.coli by MF","EP075(SIM)S: Phenolic Compound Surrogates",
    "EP075(SIM)T: PAH Surrogates","EP080S: TPH(V)/BTEX Surrogates","EA005P: pH by PC Titrator","EA015: Total Dissolved Solids dried at 180 ± 5 °C","EK059G: Nitrite plus Nitrate as N (NOx) by Discrete Analyser",
    "EK061G: Total Kjeldahl Nitrogen By Discrete Analyser","EK062G: Total Nitrogen as N (TKN + NOx) by Discrete Analyser","EK067G: Total Phosphorus as P by Discrete Analyser",
    "EP005: Total Organic Carbon (TOC)", "EP030: Biochemical Oxygen Demand (BOD)","MW006: Thermotolerant Coliforms & E.coli by MF"
]


QUERY_TYPE_TO_TABLE = {
    "ds-pfas": "[Jackson].[DSPFAS]",
    "ds-int": "[Jackson].[DSInt]",
    "ds-ext": "[Jackson].[DSExt]",
    "ineff": "[Jackson].[IncomingEffluent]",
    "treff": "[Jackson].[TreatedEffluent]"
}

