#!/usr/bin/env python3
"""
OParl file cache

Downloads the contents of an OParl API into a file based cache, allowing easy retieval and incremental cache updates

## Usage
Create an `OParlCache` object with the OParl entrypoint. Use the load_to_cache method to download the contents of the
API. You can the retrieve objects using get_from_cache(). Update by calling `load_to_cache`. Note that embedded objects
are stripped out and replaced by their id, by which you can retrieve them from the cache.

You can also use the script as a command line tool. Use "--help" for usage information.

## Implementation
The cache folder contains one "cache_info.json" with an entry for each entrypoint. Each entry list the external lists
with the date the list was last updated. All OParl entities are stored in a file under the cache folder whose path is
computed from its id by removing the double slash after the protocol and removing the OParl filter parameters. For
external lists only the ids of the elements are stored in the file.
"""

import argparse
import datetime
import json
import os
import requests

from collections import OrderedDict, defaultdict
from urllib.parse import urlparse, urlencode, parse_qs, urlunparse
from dateutil.tz import tzlocal
from threading import Lock
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import islice

from validate_examples import validate_object


class OParlCache:
    def __init__(self, entrypoint, schemadir, cachedir, validate, external_list_limit=None, max_workes=None):
        self.schema = {}
        self.cachedir = cachedir
        self.entrypoint = entrypoint
        self.cache_info_file = os.path.join(self.cachedir, "cache_info.json")
        self.external_lists = []
        self.object_pairs_hook = OrderedDict
        self.validate = validate
        self.thread_pool = ThreadPoolExecutor(max_workers=max_workes)
        self.futures = []
        self.external_lists_lock = Lock()
        self.external_list_limit = external_list_limit

        for schemafile in os.listdir(schemadir):
            with open(os.path.join(schemadir, schemafile)) as file:
                self.schema[os.path.splitext(schemafile)[0]] = self.json_load_hooked(file)

        if os.path.isfile(self.cache_info_file):
            with open(self.cache_info_file) as f:
                cache_info = self.json_load_hooked(f)
            for i in cache_info:
                if i["entrypoint"] == self.entrypoint:
                    self.external_lists = i["external_lists"]
                    break
        else:
            os.makedirs(os.path.dirname(self.cache_info_file), exist_ok=True)
            with open(self.cache_info_file, "w") as f:
                json.dump(self.json_loads_hooked("[]"), f)

    @staticmethod
    def iso8601_now():
        return datetime.datetime.now().replace(microsecond=0, tzinfo=tzlocal()).isoformat()

    def json_loads_hooked(self, data):
        return json.loads(data, object_pairs_hook=self.object_pairs_hook)

    def json_load_hooked(self, data):
        return json.load(data, object_pairs_hook=self.object_pairs_hook)

    def add_external_list(self, element):
        self.external_lists_lock.acquire()
        print("Scheduled: " + element["url"])
        self.external_lists.append(element)
        self.futures.append(self.thread_pool.submit(self.parse_external_list, element["url"], element["last_update"]))
        self.external_lists_lock.release()

    def get_path_from_url(self, url_raw):
        """
        :param url_raw:
        :return: the path to where the corresponding url is cached
        """
        url = urlparse(url_raw)
        url_options = url.params

        query = parse_qs(url.query)
        query.pop("modified_since", None)
        query.pop("modified_until", None)
        query.pop("created_since", None)
        query.pop("created_until", None)

        if query != {}:
            url_options += "?" + urlencode(query)
        if url.fragment != "":
            url_options += "#" + url.fragment

        return os.path.join(self.cachedir, url.scheme + "::" + url.netloc, url.path[1:] + url_options + ".json")

    def download_external_list(self, url):
        """
        Yields all the objects from all pages of an external list
        :param url:
        :return:
        """
        while True:
            print("- " + url)
            response = requests.get(url)
            response.raise_for_status()
            contents = response.json(object_pairs_hook=self.object_pairs_hook)
            if "next" in contents["links"]:
                url = contents["links"]["next"]

            for i in contents["data"]:
                yield i

            if "next" not in contents["links"]:
                return

    def write_to_cache(self, url, cacheable):
        filepath = self.get_path_from_url(url)
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w') as f:
            json.dump(cacheable, f, indent=4)

    def parse_entry(self, key, entry, entry_def):
        if entry_def["type"] == "array":
            i = 0
            while i < len(entry):
                entry[i] = self.parse_entry(key + "[" + str(i) + "]", entry[i], entry_def["items"])
                i += 1
        elif entry_def["type"] == "object":
            if entry["type"] == "Feature":
                return entry
            self.parse_object(entry)
            return entry["id"]
        elif "references" in entry_def and entry_def["references"] == "externalList":
            if entry not in [i["url"] for i in self.external_lists]:
                self.add_external_list({"url": entry, "last_update": None})
        return entry

    def parse_object(self, target):
        oparl_type = target["type"].split("/")[-1]
        definition = self.schema[oparl_type]["properties"]

        for key in set(target.keys()) & set(definition.keys()):
            target[key] = self.parse_entry(key, target[key], definition[key])

        self.write_to_cache(target["id"], target)

    def parse_external_list(self, url_raw, last_update):
        """
        :param url_raw: The url of the external list
        :param last_update: The datetime in iso 8601 format when the lisdt was last synced or None
        :return: the new last_update date
        """
        this_sync = self.iso8601_now()

        # Add the filter to the url
        url_parts = list(urlparse(url_raw))
        query = parse_qs(url_parts[4])
        if last_update:
            query.update({"modified_since": last_update})
        url_parts[4] = urlencode(query)
        url = urlunparse(url_parts)

        collected_messages = defaultdict(int)
        urls = []

        if not self.external_list_limit:
            external_list_iterator = self.download_external_list(url)
        else:
            external_list_iterator = islice(self.download_external_list(url), self.external_list_limit)

        for i in external_list_iterator:
            if self.validate:
                valid, messages = validate_object(i, schema=self.schema)
                if not valid:
                    for j in messages:
                        collected_messages[j] += 1
            urls.append(i["id"])
            self.parse_object(i)

        if last_update:
            with open(self.get_path_from_url(url)) as f:
                old_urls = self.json_load_hooked(f)
        else:
            old_urls = []

        self.write_to_cache(url, old_urls + urls)

        self.external_lists_lock.acquire()
        for i in self.external_lists:
            if i["url"] == url_raw:
                i["last_update"] = this_sync
                break

        self.save()
        self.external_lists_lock.release()

        return collected_messages

    def load_to_cache(self):
        response = requests.get(self.entrypoint)
        response.raise_for_status()
        entryobject = response.json(object_pairs_hook=self.object_pairs_hook)
        self.parse_object(entryobject)

        for i in self.external_lists:
            print("Scheduled: " + i["url"])
            self.futures.append(self.thread_pool.submit(self.parse_external_list, i["url"], i["last_update"]))

        for i in self.futures:
            collected_messages = i.result()  # keep i.result() to have the future be executed
            for key, value in collected_messages.items():
                print("{:>5}x: {}".format(value, key))

        self.thread_pool.shutdown()

    def save(self):
        with open(self.cache_info_file) as f:
            cache_info = self.json_load_hooked(f)

        for i in cache_info:
            if i["entrypoint"] == self.entrypoint:
                i["external_lists"] = self.external_lists
                break
        else:
            cache_info.append({
                "entrypoint": self.entrypoint,
                "external_lists": self.external_lists
            })

        with open(self.cache_info_file, "w") as f:
            json.dump(cache_info, f, indent=4)

    def get_from_cache(self, url):
        """
        Returnes cached API results for all cached objects

        :param url:
        :return:
        """
        if not os.path.isfile(self.get_path_from_url(url)):
            return None

        with open(self.get_path_from_url(url)) as f:
            loaded = self.json_load_hooked(f)

        if type(loaded) == list:
            for i, j in enumerate(loaded):
                with open(self.get_path_from_url(j)) as f:
                    loaded[i] = self.json_load_hooked(f)

        return loaded


def main():
    parser = argparse.ArgumentParser(description="CLI of the python OParl cache ")
    parser.add_argument("--entrypoint", default="http://localhost:8080/oparl/v1.0")
    parser.add_argument("--schemadir", default="~/oparl/schema/")
    parser.add_argument("--cachedir", default="~/cache/")
    parser.add_argument("--max-workers", default=None, type=int)
    parser.add_argument("--external-list-limit", default=None, type=int,
                        help="Limits the number of objects retrieved from the external lists. \
                        This is for the validation and benchmarking purposes")

    # Why do boolean flags no works?
    parser.add_argument('--validate', dest='validate', action='store_true')
    parser.add_argument('--no-validate', dest='validate', action='store_false')
    parser.set_defaults(feature=True)

    args = parser.parse_args()

    oparl_cache = OParlCache(args.entrypoint, os.path.expanduser(args.schemadir), os.path.expanduser(args.cachedir),
                             args.validate, args.external_list_limit)
    oparl_cache.load_to_cache()


if __name__ == '__main__':
    main()
