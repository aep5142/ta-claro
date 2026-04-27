from datetime import datetime
from zoneinfo import ZoneInfo

SANTIAGO_TZ = ZoneInfo("America/Santiago")


def now_santiago() -> datetime:
    return datetime.now(SANTIAGO_TZ)
