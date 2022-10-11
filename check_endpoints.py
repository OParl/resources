import asyncio
from pathlib import Path

import httpx
import yaml
from httpx import AsyncClient
from tqdm.auto import tqdm


async def test(client: AsyncClient, name: str, url: str):
    try:
        response = await client.get(url)
        response.raise_for_status()
        data = response.json()
    except httpx.HTTPError as e:
        tqdm.write(f"✗ {name} ({url}) failed {type(e).__name__}: {e}")
        return

    if "/1.0/" in url:
        try:
            new_url = url.replace("/1.0/", "/1.1/")
            response = await client.get(new_url)
            response.raise_for_status()
            data = response.json()
            tqdm.write(f"❗ Update for {name}: {url} -> {new_url}")
        except httpx.HTTPError:
            pass

    if data["type"] not in [
        "https://schema.oparl.org/1.0/System",
        "https://schema.oparl.org/1.1/System",
    ]:
        tqdm.write(f"✗ {name} ({url}) is not valid oparl")


async def main():
    data = yaml.safe_load(Path("endpoints.yml").read_text())

    async with AsyncClient() as client:
        futures = [
            asyncio.ensure_future(test(client, entry["title"], entry["url"]))
            for entry in data
        ]
        for i in tqdm(asyncio.as_completed(futures), total=len(futures)):
            await i


if __name__ == "__main__":
    asyncio.run(main())
