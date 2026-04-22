import asyncio
import logging
import httpx
from datetime import datetime
import os
from dotenv import load_dotenv
import re
from supabase import create_client
load_dotenv()

RUN_DAY_OF_MONTH = 21
RETRY_INTERVAL_S = 3600

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("uf-worker")

#Getting variables from .env
def return_cmf_api_key():
    """Returns the CMF API keys stored in .env"""
    
    CMF_API_KEY = os.environ["CMF_API_KEY"]
    return CMF_API_KEY

def return_database_credentials():
    """Returns the Supabase credentials stored in .env"""

    return os.environ["SUPABASE_URL"], os.environ["SUPABASE_SERVICE_ROLE_KEY"]


SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY = return_database_credentials()
sb = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


def normalize_uf_date(raw_date):
    """Converts CMF date strings into ISO dates for Postgres upserts."""

    for date_format in ("%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw_date, date_format).date().isoformat()
        except ValueError:
            continue

    raise ValueError(f"Unsupported UF date format: {raw_date}")


#Creating variables to modify API call

async def historical_ufs(client):
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
    response = await client.get(UF_HISTORICAL_LINK, timeout=30)
    response.raise_for_status()
    data = response.json()
    data = data["UFs"]
    
    for d in data:
        d["Fecha"] = normalize_uf_date(d["Fecha"])
        d["Valor"] = float(d["Valor"].replace(".", "").replace(",", "."))
    
    return data


def should_run_today():
    """Returns True on or after the scheduled day of the month."""

    return datetime.now().day >= RUN_DAY_OF_MONTH


def current_month_key():
    """Returns the current month key in YYYY-MM format."""

    now = datetime.now()
    return f"{now.year:04d}-{now.month:02d}"


def current_month_already_synced():
    """Returns True when this month's UF sync already succeeded."""

    month_key = current_month_key()
    response = (
        sb.table("uf_sync_runs")
        .select("month_key")
        .eq("month_key", month_key)
        .limit(1)
        .execute()
    )
    return bool(response.data)


async def populates_uf_values(client):
    """Upserts historical UF values into the existing uf_values table."""

    historical_data = await historical_ufs(client)
    transformed_data = [
        {
            "uf_date": entry["Fecha"],
            "value": entry["Valor"],
        }
        for entry in historical_data
    ]

    response = (
        sb.table("uf_values")
        .upsert(transformed_data, on_conflict="uf_date")
        .execute()
    )

    sb.table("uf_sync_runs").upsert(
        {"month_key": current_month_key()},
        on_conflict="month_key",
    ).execute()

    return response


async def run_worker():
    """Runs the monthly UF sync worker with hourly retries after the 21st."""

    async with httpx.AsyncClient() as client:
        while True:
            try:
                if not should_run_today():
                    log.info("Skipping UF sync: waiting for day %s.", RUN_DAY_OF_MONTH)
                elif current_month_already_synced():
                    log.info("Skipping UF sync: current month is already recorded in uf_sync_runs.")
                else:
                    await populates_uf_values(client)
                    log.info("UF sync completed successfully.")
            except Exception as exc:
                log.warning("UF sync failed: %s: %s", type(exc).__name__, exc)

            await asyncio.sleep(RETRY_INTERVAL_S)


if __name__ == "__main__":
    asyncio.run(run_worker())
