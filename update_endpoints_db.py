#!/usr/bin/env python3
#********************************************************************
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
#*********************************************************************

"""
Example for using OParl in Python 3
"""
import gi
gi.require_version('OParl', '0.2')
from gi.repository import OParl

import urllib.request
from yaml import dump

def resolve(_, url, status):
    try:
        req = urllib.request.urlopen(url)
        status= req.getcode()
        data = req.read()
        return data.decode('utf-8')
    except urllib.error.HTTPError as e:
        status = e.getcode()
        return None
    except Exception as e:
        status = -1
        return None

client = OParl.Client()
client.connect("resolve_url", resolve)

endpoints = []

with open('endpoints.list', 'r') as endpoints_list:
    lines = endpoints_list.readlines()
    for endpoint_uri in lines:
        if endpoint_uri[0] == ';':
            # ";" marks comments
            continue

        try:
            endpoint_uri = endpoint_uri.strip()
            system = client.open(endpoint_uri)

            endpoint = {
                'name': system.get_name(),
                'url': endpoint_uri,
                'product': system.get_product(),
                'vendor': system.get_vendor(),
                'contact': {
                    'name': system.get_contact_name(),
                    'email': system.get_contact_email(),
                },
                'license': system.get_license(),
                'oparl_version': system.get_oparl_version(),
            }
        except:
            print("Failed to read data from: {}".format(endpoint_uri))

        try:
            endpoint['number_of_bodies'] = len(system.get_body())
        except:
            endpoint['number_of_bodies'] = -1

        endpoints.append(endpoint)

print(dump(endpoints))

with open('endpoints.yml') as endpoint_db:
    # todo: dump to yml
    pass
