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

for filename in files:
	with open(os.path.join(filename), 'r+') as file:
            data = json.load(file)

            if 'referred_to_by' in data:
                data['referred_to_by'].append(knoedler_database)
            else:
                data['referred_to_by'] = [knoedler_database]

            file.seek(0)
            json.dump(data, file, indent=4)
            file.truncate()