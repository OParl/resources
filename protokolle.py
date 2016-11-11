#!/usr/bin/env python3
"""
Measure how many protocols are published for the meetings of the
Berzirksausschüsse in Munich using the OParl API of München Transparent

This is meant to be very simple, so the cache won't see any update on the
server (yet). If you want to include updates from the server, delete the cache
folder.

NOTE: This is not a scientific study but an example for using the OParl API, so
the numbers are a bit sloppy.
"""

import os
import requests
import json
import sys

from urllib.parse import urlparse
from oparl_cache import OParlCache


def main():
    entrypoint = "https://www.muenchen-transparent.de/oparl/v1.0/list/body"
    cacher = OParlCache(entrypoint, "/home/konsti/oparl/schema", "/home/konsti/cache", True)

    bodies = cacher.get_from_cache(entrypoint)
    bodies.pop(0)  # Remove the Stadtrat

    bezirkausschuesse_stats = []

    for ba in bodies:
        meetings = cacher.get_from_cache(ba["meeting"])

        total = 0
        good = 0
        bad = 0
        undecided = 0

        for meeting in meetings:
            # Exclude minor meetings
            if "UA" in meeting["name"]:
                continue
            assert "Vollgremium" in meeting["name"], meeting["name"]

            # Exclude the future and the near present
            if meeting["start"][0:4] == "2017":
                continue
            elif meeting["start"][0:4] == "2016" and int(meeting["start"][5:7]) >= 10:
                continue

            total += 1

            for fileurl in meeting["auxiliaryFile"]:
                file = cacher.get_from_cache(fileurl)
                name = file["name"]
                good_words = ["protokol", "Protokoll", "Niederschrift", "Prot"]
                bad_words = ["Einladung", "Nachtrag", "nachtrag", "Anwesenheitsliste", "Tagesordnung", "TO ", "to-", "Ladung"]
                if any(x in name for x in good_words):  # good
                    good += 1
                    break
                elif any(x in name for x in bad_words):  # bad
                    pass
                else:  # ugly
                    # print("Unklar: {}".format(name))
                    undecided += 1
                    break
            else:
                bad += 1

        assert total == good + bad + undecided
        line = [ba["shortName"], total, good, bad, undecided, int(good * 100 / total)]
        bezirkausschuesse_stats.append(line)

    format_string = "{:5} | {:5} | {:4} | {:3} | {:4} | {:4}"
    print(format_string.format("BA", "Total", "Good", "Bad", "Ugly", "Score"))
    print("-" * 41)
    format_string += "%" # The score is in percent
    for line in bezirkausschuesse_stats:
        print(format_string.format(*line))

if __name__ == '__main__':
    main()
