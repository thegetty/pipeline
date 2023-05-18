#!/usr/bin/env python3 -B

"""
This script converts a CSV export of in the format of 'collapsed-knoedler-places-group2-v2.csv'
can be used as a service file in the provenance pipelines.

    python ./scriptscollapsed-knoedler-places-group2-v2.csv
"""

import sys
import json
import pandas as pd
import numpy as np

i = -1
limit = False
limit_no = 1000

if __name__ == "__main__":
    filename = sys.argv[1]

    csv_df = pd.read_csv(filename)
    csv_df["gvp:placeTypePreferred_label"] = ""
    csv_df["gvp:placeTypePreferred_aat_term"] = ""

    csv_df["same as"] = csv_df["same as"].astype("string")
    csv_df["part of"] = csv_df["part of"].astype("string")
    csv_df = csv_df.replace({np.nan: None})

    with open("./data/knoedler/tgn.json", "r") as f:
        tgn = json.load(f)

    for i, row in csv_df.iterrows():
        print(f"Line: {i}")
        if limit and i == limit_no:
            break

        place = row["place"]
        same_as = row["same as"]

        if same_as and "/" in same_as:
            same_as = same_as.split("/")[1]

        part_of = row["part of"]
        if part_of and "/" in part_of:
            part_of = part_of.split("/")[1]

        if same_as:
            row["gvp:placeTypePreferred_label"] = tgn[same_as]["place_type_label"]
            row["gvp:placeTypePreferred_aat_term"] = tgn[same_as][
                "place_type_preferred"
            ]
        elif part_of:
            row["gvp:placeTypePreferred_label"] = tgn[part_of]["place_type_label"]
            row["gvp:placeTypePreferred_aat_term"] = tgn[part_of][
                "place_type_preferred"
            ]
        else:
            pass

    csv_df.to_csv("collapsed-knoedler-places-group2-v2-preferred-places-types.csv")
