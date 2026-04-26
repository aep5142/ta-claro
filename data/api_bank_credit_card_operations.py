import asyncio

from data.workers.bank_credit_card_operations_worker import run_worker


if __name__ == "__main__":
    asyncio.run(run_worker())
