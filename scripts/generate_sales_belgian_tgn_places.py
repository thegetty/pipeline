#!/usr/bin/env python3 -B

"""
This script converts a CSV export of in the format of 'collapsed_belgian-places.csv'
can be used as a service file in the provenance pipelines.

    python ./generate_sales_belgian_tgn_places.py
"""

import csv
import sys
import requests
import json
import warnings
import time
from rdflib import Graph, URIRef

cache = {}  # don't spam tgn
aat_cache = {}  # don't spam aat

session = requests.Session()


def fetch_tgn_data(tgn_id: str):
    if "/" in tgn_id:
        raise

    if tgn_id in cache:
        return [cache[t["tgn_id"]]]

    tgn_api_url = f"https://vocab.getty.edu/tgn/{tgn_id}.jsonld"
    # print(f"Looking up {tgn_api_url}...")

    url = f"https://vocab.getty.edu/download/rdf?uri=http://vocab.getty.edu/tgn/{tgn_id}.rdf"
    data = {
        "latitude": "",
        "longitude": "",
        "place_type_preferred": "",
        "place_label": "",
        "place_type_label": "",
        "permalink": "",
        "tgn_id": tgn_id,
        "part_of": "",
    }

    ret = [data]

    try:
        g = Graph()
        latitude = URIRef("http://schema.org/latitude")
        longitude = URIRef("http://schema.org/longitude")
        place_type_preferred = URIRef(
            "http://vocab.getty.edu/ontology#placeTypePreferred"
        )
        place_preferred_label = URIRef("http://www.w3.org/2004/02/skos/core#prefLabel")
        permalink = URIRef("http://www.w3.org/2000/01/rdf-schema#seeAlso")

        g.parse(url, format="application/rdf+xml")

        for s, p, o in g.triples((None, latitude, None)):
            data["latitude"] = str(o)
        # lat can be null

        for s, p, o in g.triples((None, longitude, None)):
            data["longitude"] = str(o)
        # lon can be null

        for s, p, o in g.triples((None, place_type_preferred, None)):
            data["place_type_preferred"] = str(o)

        assert data["place_type_preferred"]

        for s, p, o in g.triples((None, place_preferred_label, None)):
            if o.language == "en":
                data["place_label"] = str(o)
                break

        if not data["place_label"]:
            for s, p, o in g.triples((None, place_preferred_label, None)):
                data["place_label"] = str(o)
                break

        assert data["place_label"]

        url = data["place_type_preferred"] + ".jsonld"
        if url not in aat_cache:
            response = session.get(url).json()
            data["place_type_label"] = response["_label"]
            aat_cache[url] = response["_label"]
        else:
            data["place_type_label"] = aat_cache[url]

        for s, p, o in g.triples((None, permalink, None)):
            data["permalink"] = str(o)
            break
        assert data["permalink"]

        response = session.get(tgn_api_url).json()
        parent = None
        for part in response.get("part_of", []):
            if "classified_as" in part:
                for cl in part["classified_as"]:
                    if (
                        cl["id"] == "http://vocab.getty.edu/aat/300449152"
                    ):  # preferred parent
                        parent = part
                        # print(f"Has preferred parent!")
        time.sleep(0.05)

        if parent:
            data["part_of"] = parent["id"].split("/")[-1]
            print(f"{data['place_label']} is part of {parent['_label']}")

            part_data = fetch_tgn_data(data["part_of"])
            # print(f"{parent['_label']} is part of {parent['_label']}")
            ret.extend(part_data)
        else:
            print(f"No parent for {data['place_label']}")

    except Exception as ex:
        print(f"Error: While getting {url}")
        print(ex)
        sys.exit()

    return ret


i = -1
limit = False
limit_no = 1000
if __name__ == "__main__":
    filename = sys.argv[1]

    with open(filename, "r", encoding="utf-8-sig") as csv_file:
        reader = csv.reader(csv_file, delimiter=",")
        headers = next(reader)

        data = {}
        for row in reader:
            i += 1
            print(f"Line: {i}")
            if limit and i == limit_no:
                break
            # read fields from csv
            field = row[2]
            pi_record_no = row[0]

            place = row[3]
            same_as = row[4]
            if "/" in same_as:
                same_as = same_as.split("/")[1]

            part_of = row[5]
            if "/" in part_of:
                part_of = part_of.split("/")[1]

            tgn_data = {}

            if same_as:
                if same_as not in cache:
                    temp = fetch_tgn_data(same_as)
                    for t in temp:
                        cache[t["tgn_id"]] = t
                    tgn_data["same_as"] = same_as
                else:
                    print(f"{cache[same_as]['place_label']} is in cache")
                    tgn_data["same_as"] = same_as

            if part_of:
                if part_of not in cache:
                    temp = fetch_tgn_data(part_of)
                    for t in temp:
                        cache[t["tgn_id"]] = t
                    tgn_data["part_of"] = part_of
                else:
                    print(f"{cache[part_of]['place_label']} is in cache")
                    tgn_data["part_of"] = part_of

            if pi_record_no not in data:
                data[pi_record_no] = {}

            if field not in data[pi_record_no]:
                data[pi_record_no][field] = {}

            assert place not in data[pi_record_no][field]
            data[pi_record_no][field][place] = tgn_data

    with open("../data/sales/sales_belgian_tgn.json", "w") as f:
        json.dump(data, f, indent=4, sort_keys=True, ensure_ascii=False)

    with open("../data/sales/belgian_tgn.json", "w") as f:
        json.dump(cache, f, indent=4, sort_keys=True, ensure_ascii=False)