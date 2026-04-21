import httpx
from datetime import datetime
import os
from dotenv import load_dotenv
import re
load_dotenv()

#Getting variables from .env
CMF_API_KEY = os.environ["CMF_API_KEY"]
UF_HISTORICAL_LINK = os.environ["CMF_UF_HISTORICAL"]

#Creating variables to modify API call


#Rebuilding the API Link
#Base API
format_base_api = r"<format>"
yr_base_api = r"<year>"
mth_base_api = r"<month>"
api_key_str = r"<api_key>"

#Desired ones
format = "json"
now = datetime.now()
now_yr, now_month, now_day = str(now.year), str(now.month + 1), str(now.day)


UF_HISTORICAL_LINK = re.sub(format_base_api, format, UF_HISTORICAL_LINK)
UF_HISTORICAL_LINK = re.sub(yr_base_api, now_yr, UF_HISTORICAL_LINK)
UF_HISTORICAL_LINK = re.sub(mth_base_api, now_month, UF_HISTORICAL_LINK)
UF_HISTORICAL_LINK = re.sub(api_key_str, CMF_API_KEY, UF_HISTORICAL_LINK)

print(UF_HISTORICAL_LINK)

