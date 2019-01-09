#!/usr/bin/env python3

"""
Prints statistics about which attributes is used how often, by default using the oparl mirror

As of 2019-01-05:

Stats for oparl:Person
 - body: 7368
 - created: 5690
 - familyName: 5635
 - formOfAddress: 5592
 - gender: 3708
 - givenName: 5631
 - id: 7368
 - keyword: 3769
 - location: 5231
 - locationObject: 5231
 - membership: 4810
 - modified: 5730
 - name: 5638
 - oparl-mirror:originalId: 7368
 - phone: 663
 - type: 7368
 - email: 1098
 - title: 3258
 - affix: 142
 - deleted: 151
Stats for oparl:Organization
 - body: 2534
 - created: 1750
 - deleted: 7
 - id: 2534
 - meeting: 2534
 - modified: 1740
 - oparl-mirror:originalId: 2534
 - type: 2534
 - name: 1744
 - organizationType: 1744
 - startDate: 1663
 - classification: 1200
 - membership: 939
 - post: 654
 - shortName: 1139
 - endDate: 318
 - location: 480
 - website: 107
Stats for oparl:Location
 - bodies: 6872
 - created: 6751
 - description: 2640
 - id: 6872
 - locality: 6095
 - meeting: 754
 - modified: 6821
 - oparl-mirror:originalId: 6872
 - postalCode: 6096
 - room: 731
 - streetAddress: 5996
 - subLocality: 3135
 - type: 6872
 - person: 5264
 - organization: 387
 - web: 145
 - geojson: 447
 - papers: 443
Stats for oparl:Meeting
 - agendaItem: 23479
 - body: 28295
 - created: 28220
 - end: 28044
 - id: 28295
 - location: 26630
 - modified: 28220
 - name: 28044
 - oparl-mirror:originalId: 28295
 - organization: 28006
 - start: 28044
 - type: 28295
 - web: 5485
 - invitation: 12842
 - participant: 5274
 - resultsProtocol: 4333
 - auxiliaryFile: 229
 - keyword: 22559
 - deleted: 326
Stats for oparl:Paper
 - body: 101839
 - consultation: 99893
 - created: 101830
 - date: 97088
 - id: 101839
 - mainFile: 91539
 - modified: 101830
 - name: 101296
 - oparl-mirror:originalId: 101839
 - paperType: 97527
 - reference: 101295
 - type: 101839
 - underDirectionOf: 65394
 - location: 7296
 - deleted: 613
 - auxiliaryFile: 35464
 - originatorPerson: 937
 - web: 26112
Stats for oparl:Membership
 - body: 31295
 - created: 28002
 - id: 31295
 - modified: 28469
 - oparl-mirror:originalId: 31295
 - organization: 31294
 - person: 6580
 - role: 28441
 - startDate: 28459
 - type: 31295
 - votingRight: 23941
 - endDate: 9140
Stats for oparl:Consultation
 - body: 160820
 - created: 159983
 - id: 160820
 - meeting: 142211
 - modified: 159983
 - oparl-mirror:originalId: 160820
 - organization: 159866
 - paper: 159994
 - type: 160820
 - agendaItem: 140742
 - role: 52000
Stats for oparl:AgendaItem
 - auxiliaryFile: 11661
 - body: 316482
 - consultation: 138735
 - created: 316300
 - id: 316482
 - modified: 316300
 - name: 316300
 - number: 311709
 - oparl-mirror:originalId: 316482
 - resolutionFile: 87308
 - result: 139946
 - type: 316482
 - meeting: 82955
 - public: 82955
 - web: 55456
Stats for oparl:File
 - accessUrl: 288399
 - agendaItem: 103678
 - body: 288399
 - created: 288390
 - date: 228857
 - downloadUrl: 288399
 - id: 288399
 - mimeType: 288390
 - modified: 288390
 - name: 288399
 - oparl-mirror:downloaded: 283347
 - oparl-mirror:originalAccessUrl: 288399
 - oparl-mirror:originalDownloadUrl: 268495
 - oparl-mirror:originalId: 288399
 - sha1Checksum: 288112
 - sha512Checksum: 283347
 - size: 288276
 - type: 288399
 - meeting: 17405
 - paper: 167316
 - fileName: 19904
Stats for oparl:LegislativeTerm
 - body: 4
 - created: 2
 - endDate: 4
 - id: 4
 - modified: 2
 - name: 4
 - oparl-mirror:originalId: 4
 - startDate: 4
 - type: 4
 - keyword: 2
"""

import argparse
import asyncio
import time
from collections import defaultdict

import aiohttp
from aiohttp import ContentTypeError
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
                try:
                    page = await response.json()
                except ContentTypeError as e:
                    tqdm.write(f"Failed to load {url} with {e}")
                    tqdm.write(f"{response.text()}")
                    return objects

            end = time.perf_counter()
            tqdm.write(f"Loaded {url} in {end - start}s")

            objects += page["data"]
            url = page["links"].get("next")

    return objects


async def run(entrypoint, semaphore):
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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--entrypoint", default=mirror_url)
    parser.add_argument("--max-parallel", default=8, type=int)
    args = parser.parse_args()
    start = time.perf_counter()
    loop = asyncio.get_event_loop()
    semaphore = asyncio.Semaphore(args.max_parallel)
    loop.run_until_complete(run(args.entrypoint, semaphore))
    end = time.perf_counter()
    print(f"Loading the complete dataset took {end - start}s")


if __name__ == "__main__":
    main()
