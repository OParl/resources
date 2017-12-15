#!/usr/bin/env python3
# ********************************************************************
# Copyright 2017, Stefan "eFrane" Graupner
#
# This file is part of OParl's extra resources.
#
# liboparl is free software: you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public License
# as published by the Free Software Foundation, either
# version 3 of the License, or (at your option) any later
# version.
#
# liboparl is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied
# warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
# PURPOSE. See the GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with liboparl.
# If not, see http://www.gnu.org/licenses/.
# *********************************************************************

import gi

gi.require_version('OParl', '0.2')
from gi.repository import OParl

import requests
import yaml


def resolve(_, url):
    try:
        response = requests.get(url)
        return OParl.ResolveUrlResult(resolved_data=response.text, success=True, status_code=response.status_code)
    except Exception as e:
        print("ARRR", e)
        return OParl.ResolveUrlResult(resolved_data=None, success=False, status_code=-1)


def main():
    endpoints = []

    with open('endpoints.list') as endpoints_list:
        lines = endpoints_list.readlines()

    for endpoint_uri in lines:
        endpoint_uri = endpoint_uri.strip()
        if endpoint_uri == "" or endpoint_uri[0] == ';':
            # ";" marks comments
            continue
        print("Processing: ", endpoint_uri)

        client = OParl.Client()
        client.connect("resolve_url", resolve)

        system = client.open(endpoint_uri)

        endpoint = {
            'titel': system.get_name(),
            'url': system.get_id(),
        }

        endpoints.append(endpoint)

    with open('endpoints.yml', 'w') as endpoint_db:
        # todo: dump to yml
        endpoint_db.write(yaml.dump(endpoints))


if __name__ == '__main__':
    main()
