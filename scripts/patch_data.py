#!/usr/bin/env python3 -B

import json
import os
import sys

from pathlib import Path
from settings import output_file_path

files = []
if len(sys.argv) > 1:
	for p in sys.argv[1:]:
		path = Path(p)
		if path.is_dir():
			files += sorted(str(s) for s in path.rglob('*.json'))
		else:
			files.append(p)
else:
	files = sorted(Path(output_file_path).rglob('*.json'))
	
knoedler_database = {
    "id": "urn:uuid:c5feae26-2eb0-353d-a089-133f6f5b6b7a",
    "type": "LinguisticObject",
    "_label": "STAR Knoedler Database"
}

def delete_duplicate_digital_objects(references):
    to_delete = set()
    for i in range(0, len(references)):
        for j in range (i+1, len(references)):
            access_points_i = references[i]['access_point']
            access_points_j = references[j]['access_point']
            num_of_equal_access_points = 0
            for k in range(0, len(access_points_i)):
                if access_points_i[k]['id'] == access_points_j[k]['id']:
                    num_of_equal_access_points += 1
            if num_of_equal_access_points == len(access_points_i):
                to_delete.add(j)

    for i in reversed(list(to_delete)):
        del references[i]

    return references

"""
 "identified_by": [
        {
            "type": "Name",
            "content": "Knoedler Sale of Stock Number 11084 (1906-11-07)"
        }
    ],
    """


def fill_prov_name_info(data):
    name=data['_label']
    identified = [
        {
            "type" : "Name",
            "content" : name
        }
    ]
    
    data['identified_by'] = identified
    return data

for filename in files:
	with open(os.path.join(filename), 'r+') as file:
            data = json.load(file)
            # Add missing names for Provenance activities
            if data['type'] == 'Activity' and 'identified_by' not in data:
                data = fill_prov_name_info(data)

            if 'referred_to_by' in data:
                #### Delete duplicate digital objects 
                digital_references = [ref for ref in data['referred_to_by'] if ref['type'] == "DigitalObject"]
                if len(digital_references) > 1:
                    data['referred_to_by'] = delete_duplicate_digital_objects(data['referred_to_by'])

                ### Add the STAR Knoedler reference to all resources
                data['referred_to_by'].append(knoedler_database)
            else:
                data['referred_to_by'] = [knoedler_database]

            file.seek(0)
            json.dump(data, file, indent=4)
            file.truncate()