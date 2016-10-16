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


def get_cached(url):
    """
    Fetches any URL using a simple file cache for known URLs.

    :param url: str
    :return: str
    """
    parsed = urlparse(url)

    # Unique filepath for caching requests to this url
    # Note that os.path.join doesn't work here due to leading slashes
    filepath = os.path.abspath("cache/" + parsed.netloc + "".join(parsed[2:])) + ".json"

    if os.path.isfile(filepath):
        with open(filepath) as f:
            return f.read()

    response = requests.get(url)
    response.raise_for_status()

    # Not cached? Let's cache it
    if not os.path.exists(os.path.dirname(filepath)):
        os.makedirs(os.path.dirname(filepath))
    with open(filepath, 'w') as f:
        f.write(response.text)

    # Show we're not stalled
    sys.stdout.write("x")
    sys.stdout.flush()

    return response.text


def external_list(url):
    """
    Yields all objects of an external list.

    :param url: url of the external list
    :yields: Iterator over all objects in the external list
    """
    while 1:
        page = json.loads(get_cached(url))
        for i in page["data"]:
            yield i

        if "next" in page["links"].keys():
            url = page["links"]["next"]
        else:
            break


def main():
    entrypoint = "https://www.muenchen-transparent.de/oparl/v1.0/list/body/"

    bodies = external_list(entrypoint)
    next(bodies)  # Remove the Stadtrat

    bezirkausschuesse_stats = []

    for ba in bodies:
        meetings = external_list(ba["meeting"])

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
                file = json.loads(get_cached(fileurl))
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
