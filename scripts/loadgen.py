"""Synthetic traffic generator: simulates users walking a signup/purchase funnel.

uv run python scripts/loadgen.py --api-key pk_... --rps 2000 --duration 60
"""

import argparse
import asyncio
import random
import time
import uuid
from datetime import UTC, datetime

import httpx

FUNNEL = [("page_view", 1.0), ("signup", 0.4), ("add_to_cart", 0.6), ("purchase", 0.5)]
COUNTRIES = ["RU", "KZ", "BY", "DE", "US"]


def user_session(user_id: str) -> list[dict[str, object]]:
    session = uuid.uuid4().hex
    props = {"country": random.choice(COUNTRIES), "platform": random.choice(["web", "ios"])}
    events: list[dict[str, object]] = []
    for name, probability in FUNNEL:
        if random.random() > probability:
            break
        events.append(
            {
                "event_id": str(uuid.uuid4()),
                "name": name,
                "user_id": user_id,
                "session_id": session,
                "timestamp": datetime.now(UTC).isoformat(),
                "properties": props,
            }
        )
    return events


async def worker(
    client: httpx.AsyncClient,
    url: str,
    rps: float,
    batch: int,
    users: int,
    deadline: float,
    stats: dict[str, int],
) -> None:
    interval = batch / rps
    while time.monotonic() < deadline:
        started = time.monotonic()
        events: list[dict[str, object]] = []
        while len(events) < batch:
            events.extend(user_session(f"user-{random.randrange(users)}"))
        try:
            response = await client.post(url, json={"events": events[:batch]})
            key = "accepted" if response.status_code == 202 else f"http_{response.status_code}"
        except httpx.HTTPError:
            key = "errors"
        stats[key] = stats.get(key, 0) + (batch if key == "accepted" else 1)
        await asyncio.sleep(max(0.0, interval - (time.monotonic() - started)))


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", default="http://localhost:8001")
    parser.add_argument("--api-key", required=True)
    parser.add_argument("--rps", type=float, default=1000, help="events per second")
    parser.add_argument("--batch", type=int, default=100)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--users", type=int, default=10_000)
    parser.add_argument("--duration", type=float, default=30)
    args = parser.parse_args()

    stats: dict[str, int] = {}
    deadline = time.monotonic() + args.duration
    started = time.monotonic()
    async with httpx.AsyncClient(headers={"X-Api-Key": args.api_key}, timeout=10) as client:
        await asyncio.gather(
            *(
                worker(
                    client,
                    f"{args.url}/v1/events",
                    args.rps / args.concurrency,
                    args.batch,
                    args.users,
                    deadline,
                    stats,
                )
                for _ in range(args.concurrency)
            )
        )
    elapsed = time.monotonic() - started
    print(f"{stats} in {elapsed:.1f}s -> {stats.get('accepted', 0) / elapsed:.0f} events/s")


if __name__ == "__main__":
    asyncio.run(main())
