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
	
sales_scandi_database = {
    }
for filename in files:
	with open(os.path.join(filename), 'r+') as file:
            data = json.load(file)
            if 'referred_to_by' in data:
                ### Add the STAR scandi reference to all resources
                data['referred_to_by'].append(sales_scandi_database)
            else:
                data['referred_to_by'] = [sales_scandi_database]
