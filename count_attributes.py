#!/usr/bin/env python3

"""
Prints statistics about which attributes is used how often, by default using the oparl mirror
"""

import argparse
import asyncio
import time
from collections import defaultdict

import aiohttp
from tqdm import tqdm

mirror_url = "https://mirror.oparl.org/"

# These are the oparl 1.0 mandatory lists plus optionally those supported by the oparl mirror
list_names = [
    "agendaItem",
    "consultation",
    "file",
    "legislativeTermList",
    "locationList",
    "meeting",
    "membership",
    "organization",
    "paper",
    "person",
]


async def read_list(session, semaphore, url):
    objects = []
    # We don't want to take the server down with 300 parallel requests, so we only do n (sequential) lists in parallel
    async with semaphore:
        while url:
            start = time.perf_counter()
            async with session.get(url) as response:
                page = await response.json()

            end = time.perf_counter()
            tqdm.write(f"Loaded {url} in {end - start}s")

            objects += page["data"]
            url = page["links"].get("next")

    return objects


async def main(entrypoint, semaphore):
    stats = defaultdict(lambda: defaultdict(int))
    async with aiohttp.ClientSession() as session:
        async with session.get(entrypoint) as response:
            system = await response.json()
        bodies = await read_list(session, semaphore, system["body"])
        tasks = []
        for body in bodies:
            for list_name in list_names:
                if list_name in body:
                    tasks.append(read_list(session, semaphore, body[list_name]))

        pbar = tqdm(total=len(tasks))

        for external_list in asyncio.as_completed(tasks):
            for entry in await external_list:
                for key in entry.keys():
                    stats[entry["type"]][key] += 1

            pbar.update()
        pbar.close()

    for type_id, values in stats.items():
        type_name = "oparl:" + type_id.split("/")[-1]
        print(f"Stats for {type_name}")
        for key, count in values.items():
            print(f" - {key}: {count}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--entrypoint", default=mirror_url)
    parser.add_argument("--max-parallel", default=8, type=int)
    args = parser.parse_args()

    total_start = time.perf_counter()
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(args.max_parallel)
    loop.run_until_complete(main(args.entrypoint, semaphore))
    total_end = time.perf_counter()
    print(f"Loading the complete dataset took {total_end - total_start}s")
