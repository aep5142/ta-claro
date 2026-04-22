import httpx
from datetime import datetime
import os
from dotenv import load_dotenv
import re
from supabase import create_client
load_dotenv()

#Getting variables from .env
def return_cmf_api_key():
    """Returns the CMF API keys stored in .env"""
    
    CMF_API_KEY = os.environ["CMF_API_KEY"]
    return CMF_API_KEY

def return_database_credentials():
    """Returns the Supabase credentials stored in .env"""

    return os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"]


def normalize_uf_date(raw_date):
    """Converts CMF date strings into ISO dates for Postgres upserts."""

    for date_format in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw_date, date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError(f"Unsupported UF date format: {raw_date}")


#Creating variables to modify API call

def historical_ufs():
    """Retrives the historical UF value from today to its beginnings of the data
    back in 1990"""
    
    #Retrieving the API Key and the CMF Link
    api_key = return_cmf_api_key()
    UF_HISTORICAL_LINK = os.environ["CMF_UF_HISTORICAL"]
    #Rebuilding the API Link
    #Base API
    format_base_api = r"<format>"
    yr_base_api = r"<year>"
    mth_base_api = r"<month>"
    api_key_str = r"<api_key>"

    #Desired ones
    format = "json"
    now = datetime.now()
    next_year = now.year + 1 if now.month == 12 else now.year
    next_month = 1 if now.month == 12 else now.month + 1
    now_yr, now_month, now_day = str(next_year), str(next_month), str(now.day)


    #Rebuilding the starting poi
    UF_HISTORICAL_LINK = re.sub(format_base_api, format, UF_HISTORICAL_LINK)
    UF_HISTORICAL_LINK = re.sub(yr_base_api, now_yr, UF_HISTORICAL_LINK)
    UF_HISTORICAL_LINK = re.sub(mth_base_api, now_month, UF_HISTORICAL_LINK)
    UF_HISTORICAL_LINK = re.sub(api_key_str, api_key, UF_HISTORICAL_LINK)

    #Getting the new clean data
    response = httpx.get(UF_HISTORICAL_LINK)
    response.raise_for_status()
    data = response.json()
    data = data["UFs"]
    
    for d in data:
        d["Fecha"] = normalize_uf_date(d["Fecha"])
        d["Valor"] = float(d["Valor"].replace(".", "").replace(",", "."))
    
    return data


def populates_uf_values():
    """Upserts historical UF values into the existing uf_values table."""

    historical_data = historical_ufs()
    transformed_data = [
        {
            "uf_date": entry["Fecha"],
            "value": entry["Valor"],
        }
        for entry in historical_data
    ]

    supabase_url, supabase_service_role_key = return_database_credentials()
    supabase = create_client(supabase_url, supabase_service_role_key)
    response = (
        supabase.table("uf_values")
        .upsert(transformed_data, on_conflict="uf_date")
        .execute()
    )

    return response


if __name__ == "__main__":
    populates_uf_values()
