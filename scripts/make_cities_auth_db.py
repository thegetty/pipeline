#!/usr/bin/env python3 -B

"""
This script converts a CSV export of the City Authoriry DB JSON that
can be used as a service file in the provenance pipelines.

    ./scripts/scripts/make_cities_auth_db.py city_auth_db.csv > data/common/cities_auth_db.json
"""

from contextlib import suppress
import csv
import sys
import json
from time import sleep
import requests


if __name__ == "__main__":
    filename = sys.argv[1]

    with open(filename, "r", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        headers = next(reader)
        data = {}
        for row in reader:
            loc_verbatim = row[0].strip()
            loc_name = row[1].strip()
            loc_auth = row[2].strip()

            # if location verbatim new, ignore
            if loc_verbatim == "NEW":
                continue

            def guess_type(name, auth):
                name = name.strip()
                auth = auth.strip()
                if auth.startswith(name):
                    return "city"
                else:
                    return "address"

            def add_to_data_split(key, names, auths):
                names = names.replace("[multiple locations]", "").split(";")
                auths = auths.replace("[multiple locations]", "").split(";")
                types = [guess_type(name, auth) for name, auth in zip(names, auths)]
                
                verbatim_key = key
                key = str(key).strip().lower()
                data[key] = [
                    {
                        "name": name.strip(),
                        "authority": auth.strip(),
                        "type": type,
                        "verbatim": verbatim_key
                    }
                    for name, auth, type in zip(names, auths, types)
                ]

            def add_to_data(key, name, auth):
                verbatim_key = key
                key = str(key).strip().lower()
                data[key] = [
                    {
                        "name": name.strip(),
                        "authority": auth.strip(),
                        "type": guess_type(name, auth),
                        "verbatim": verbatim_key
                    }
                ]

            if not "Opéra; Galerie" in loc_verbatim and (
                ";" in loc_verbatim or "[multiple locations]" in loc_auth
            ):
                for name, auth in zip(loc_name, loc_auth):
                    add_to_data_split(loc_verbatim, loc_name, loc_auth)
            else:
                add_to_data(
                    loc_verbatim,
                    loc_name,
                    loc_auth,
                )

    print(json.dumps(data, indent=4, sort_keys=True, ensure_ascii=False))
