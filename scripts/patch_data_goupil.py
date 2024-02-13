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
	
goupil_database = {
      "id":"urn:uuid:75e8a92d-3df2-38cf-858f-94925f4edd47",
      "type":"LinguisticObject",
      "_label":"STAR Goupil Database"
    }
# import pdb; pdb.set_trace()

for filename in files:
	
	with open(os.path.join(filename), 'r+') as file:
		data = json.load(file)
		if 'referred_to_by' in data:
			### Add the STAR people reference to all resources
			data['referred_to_by'].append(goupil_database)

		else:
			data['referred_to_by'] = [goupil_database]
		file.seek(0)
		json.dump(data, file, indent=4)
		file.truncate()