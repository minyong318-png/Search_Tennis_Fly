# cache_facilities.py
import json
import asyncio
import aiohttp

from tennis_core import get_connector, HEADERS, init_session, fetch_facilities

OUT = "facilities_cache.json"

async def main():
    async with aiohttp.ClientSession(connector=get_connector(), headers=HEADERS) as session:
        await init_session(session)
        facilities = await fetch_facilities(session)

    with open(OUT, "w", encoding="utf-8") as f:
        json.dump(facilities, f, ensure_ascii=False, indent=2)

    print(f"[OK] saved: {OUT} (count={len(facilities)})")

if __name__ == "__main__":
    asyncio.run(main())
