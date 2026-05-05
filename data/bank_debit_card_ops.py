import asyncio

from data.workers.bank_debit_card_ops_worker import run_worker


if __name__ == "__main__":
    asyncio.run(run_worker())
