import asyncio

from data.workers.cmf_monthly_worker import run_worker


if __name__ == "__main__":
    asyncio.run(run_worker())
