#!/usr/bin/env python3 -B

'''
This script converts a CSV export of the Knoedler Stock Numbers Concordance into JSON that
can be used as a service file in the knoedler pipelines.

    ./scripts/parse_knodler_stock_concordance.py data/common/objects_same.json

'''

import csv
import sys
import jsbeautifier 
import json

if __name__ == '__main__':
	filename = sys.argv[1]
	outfile = sys.argv[2]
	data = []
	
	with open(filename, 'r', encoding='utf-8-sig') as csv_file:
		reader = csv.reader(csv_file, delimiter=',')
		headers = next(reader)
		
		for row in reader:
			d = dict(zip(headers, row))
			sn = []
			for i in range(1,11):
				if "sn" + str(i) in d and d.get("sn" + str(i)).strip() != "" :
					sn.append(d.get("sn" + str(i)).strip())
			data.append(sn)
			
	data = {'objects':data}
	with open(outfile, "w") as f:
		options = jsbeautifier.default_options()
		options.indent_size = 4
		options.indent_with_tabs = True
		f.write(jsbeautifier.beautify(json.dumps(data), options))


