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
	
people_database = {
      "id":"urn:uuid:312aef48-fe99-3575-a209-5b1e5669064c",
      "type":"LinguisticObject",
      "_label":"STAR Person Authority Database"
    }

def delete_duplicate_dig_objects(references):
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



for filename in files:
	with open(os.path.join(filename), 'r+') as file:
            data = json.load(file)
            if 'referred_to_by' in data:
                ### Add the STAR people reference to all resources
                data['referred_to_by'].append(people_database)
            else:
                data['referred_to_by'] = [people_database]
            cla_res = [
                        {
                            "id": "http://vocab.getty.edu/aat/300393211",
                            "type": "Type",
                            "_label": "Location"
                        }
                    ]
            cla_note = [
                            {
                                "id": "http://vocab.getty.edu/aat/300418049",
                                "type": "Type",
                                "_label": "Brief Text"
                            }
                        ]
            # fix potential problem in a person or group, where a nested "classified_as" inside carried out fields was missing
            if 'carried_out' in data:
                carried_out = data['carried_out']

                for i in range(len(carried_out)):
                    if carried_out[i]['_label'] == 'Sojourn activity':
                        
                        class_as = carried_out[i]['classified_as']
                        for j in range(len(class_as)):
                            if class_as[j]['_label'] == 'Residing' or class_as[j]['_label'] == 'Establishment':
                                # print("is residing or establishment")
                                if not 'classified_as' in class_as[j]:
<<<<<<< HEAD
                                    # print("no classified as")
=======
>>>>>>> ca843a8f98e018aa43ce030626b1d3c5c9aa5a44
                                    data['carried_out'][i]['classified_as'][j]['classified_as'] = cla_res
                        # print("filename ", filename)
                        if 'referred_to_by' in carried_out[i]:
                            refer_by = carried_out[i]['referred_to_by']
                            for j in range(len(refer_by)):
                                clasas_inrefer = refer_by[j]['classified_as']
                                for z in range(len(clasas_inrefer)):
                                    if clasas_inrefer[z]['_label'] == "Note":
                                        if not 'classified_as' in clasas_inrefer[z]:
                                            data['carried_out'][i]['referred_to_by'][j]['classified_as'][z]['classified_as'] = cla_note
                              

            if 'identified_by' in data:
                place_list = data['identified_by']
                import json 

                unique_dict_set = set()  
                unique_place_list = []

                for place_dict in place_list:
                    dict_str = json.dumps(place_dict, sort_keys=True)
                    if dict_str not in unique_dict_set:
                        # Add the dictionary to the unique list and update the set
                        unique_place_list.append(place_dict)
                        unique_dict_set.add(dict_str)
                
                data['identified_by'] = unique_place_list

                              
            file.seek(0)
            json.dump(data, file, indent=4)
            file.truncate()