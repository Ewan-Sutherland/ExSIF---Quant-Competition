from __future__ import annotations

import time

import config
from bot import AlphaBot
from brain_client import BrainClient
from generator import AlphaGenerator
from scheduler import Scheduler
from storage import Storage


def main() -> None:
    storage = Storage(config.DB_PATH)
    generator = AlphaGenerator()
    scheduler = Scheduler(max_slots=config.MAX_CONCURRENT_SIMS)

    client = BrainClient(
        username=config.BRAIN_USERNAME,
        password=config.BRAIN_PASSWORD,
        base_url="https://api.worldquantbrain.com",
    )

    bot = AlphaBot(
        storage=storage,
        client=client,
        generator=generator,
        scheduler=scheduler,
    )

    bot.recover_running_from_storage()
    bot.mark_stale_runs_timed_out()

    print("[START] Alpha bot is running")

    while True:
        try:
            bot.tick()
        except KeyboardInterrupt:
            print("\n[STOP] Interrupted by user")
            break
        except Exception as exc:
            print(f"[MAIN_LOOP_ERROR] {exc}")

        time.sleep(config.POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()