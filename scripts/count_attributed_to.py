#!/usr/bin/env python3 -B

'''
Counts the total number of object that have the specific modelling described in AR-43.
'''

import json
import os
import sys
from tqdm import tqdm
from scandir import scandir, walk

c = 0

src = './object/'
if len(sys.argv) > 1 :
    src = sys.argv[1]

print(src)

files = list()
for r, subdirs, _ in walk(src):    
    for subdir in subdirs:
        # print(subdir)
        for f in scandir(os.path.join(src, subdir)):
            files.append(f.path)

for fp in tqdm(files, mininterval=1):
    # print(f.path)
    with open(fp, mode="r") as obj:
        data = json.load(obj)
        if 'produced_by' in data:
            if 'attributed_by' in data['produced_by']:
                for attribute_assignment in data['produced_by']['attributed_by']:
                    if 'assigned_property' in attribute_assignment:
                        if attribute_assignment['assigned_property'] == 'carried_out_by':
                            c += 1
                        else:
                            pass
                            # print(f"Check {fp}")
                    
print(f"=========\n Counted : {c}")