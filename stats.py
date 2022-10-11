#!/usr/bin/env python3

import requests

with open("endpoints.list") as fp:
    lines = fp.readlines()

urls = []
for line in lines:
    line = "/".join(line.strip().split("/")[:-1])
    if not line.startswith(";") and "sdnetrim" in line:
        urls.append(line)

for url in urls:
    print(url)
    name = requests.get(url + "/body/1").json()["name"]
    papers = requests.get(url + "/body/1/paper").json()["pagination"]["totalElements"]
    meeting = requests.get(url + "/body/1/meeting").json()["pagination"][
        "totalElements"
    ]
    person = requests.get(url + "/body/1/person").json()["pagination"]["totalElements"]
    organization = requests.get(url + "/organization").json()["pagination"][
        "totalElements"
    ]

    print(
        "{} has {} papers, {} meetings, {} persons, {} organizations".format(
            name, papers, meeting, person, organization
        )
    )

if __name__ == "__main__":
    pass
