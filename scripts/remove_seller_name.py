import sys
import pandas as pd
import numpy as np
from collections import defaultdict
import json

if __name__ == "__main__":
    if len(sys.argv) < 3:
        cmd = sys.argv[0]
        print(
            f"""
    Usage: {cmd} knoedler_0.csv knoedler.csv
        """.lstrip()
        )
        sys.exit(1)

    headerPath = sys.argv[1]
    contentPath = sys.argv[2]
    header = pd.read_csv(headerPath, dtype=object)
    df = pd.read_csv(contentPath, dtype=object, names=header.columns)

    multipleRecords = df.groupby("knoedler_number").filter(lambda x: len(x) > 1)
    multipleRecords = multipleRecords.sort_values(['knoedler_number', 'entry_date_year', 'entry_date_month', 'entry_date_day'],ascending=True).groupby('knoedler_number')

    inventoryingRecords = []
    inventoryingDict = defaultdict(list)
    for group_name, df_group in multipleRecords:
        lastPurchase = ""
        for row_index, row in df_group.iterrows():
            if lastPurchase == "":
                lastPurchase = row["pi_record_no"]
            else:
                if not pd.isna(row["purchase_seller_auth_name_1"]):
                    inventoryingRecords.append(row["pi_record_no"])
                    inventoryingDict[group_name].append(row["pi_record_no"])
            if row["transaction"] == "Sold":
                lastPurchase = ""
    finalDf = df[df["pi_record_no"].isin(inventoryingRecords)]
    finalDf.to_csv("final.csv", index=False)

    finalDict = {"pi_record_no" : inventoryingRecords}
    outputJson = open("sellers_to_be_deleted.json", "w", encoding="utf-8")
    json.dump(finalDict, outputJson, indent=2)
