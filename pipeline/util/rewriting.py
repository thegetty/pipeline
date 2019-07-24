import os
import sys
import json
import uuid
import pprint
import itertools
import multiprocessing
from pathlib import Path
from contextlib import suppress

from settings import output_file_path
from pipeline.util import CromObjectMerger
from cromulent.model import factory
from cromulent import model, reader

def filename_for(data: dict, original_filename: str, verify_uuid=False):
	'''
	For JSON `data` read from the file `original_filename`, return the filename to which
	it should be (re-)written. The new filename is based on the top-level 'id' member,
	which should be a UUID URN.

	If no valid UUID is found, returns `original_filename`.
	'''
	if 'id' not in data:
		print(f'*** no @id found for {original_filename}')
		return original_filename
	uri = data['id']
	if not uri.startswith('urn:uuid:'):
		if verify_uuid:
			print(f'*** @id does not appear to be a UUID URN in {original_filename}')
		return original_filename
	urn = uri[len('urn:uuid:'):]
	fn = f'{urn}.json'
	p = Path(original_filename)
	q = p.with_name(fn)
	return q

def rewrite_output_files(r, update_filename=False, parallel=False, **kwargs):
	print(f'Rewriting JSON output files')
	p = Path(output_file_path)
	files = sorted(p.rglob('*.json'))
	
	if parallel:
		j = 8
		pool = multiprocessing.Pool(j)
		args = (((f,), r, update_filename) for f in files)
		_ = pool.starmap(_rewrite_output_files, args)
	else:
		_rewrite_output_files(files, r, update_filename, **kwargs)

def _rewrite_output_files(files, r, update_filename=False, **kwargs):
	for i, f in enumerate(files):
		# print(f'{i} {f}', end="\r", flush=True)
		with open(f) as data_file:
			try:
				data = json.load(data_file)
			except json.decoder.JSONDecodeError:
				sys.stderr.write(f'Failed to load JSON during rewriting of {f}\n')
				raise
		d = r.rewrite(data, file=f)
		if update_filename:
			newfile = filename_for(d, original_filename=f, **kwargs)
		else:
			newfile = f
		if d == data and f == newfile:
			# nothing changed; do not rewrite the file
			continue
		else:
			pass
			# print(f'*** rewrote data in {f} --> {newfile}')
		if newfile != f:
			if os.path.exists(newfile):
				read = reader.Reader()
				merger = CromObjectMerger()
				with open(newfile, 'r') as fh:
					content = fh.read()
					try:
						m = read.read(content)
						n = read.read(d)
# 						print('========================= MERGING =========================')
# 						print('merging objects:')
# 						print(f'- {m}')
# 						print(f'- {n}')
						merger.merge(m, n)
					except model.DataError:
						print(f'Exception caught while merging data from {newfile}:')
						print(d)
						print(content)
						raise
					data = factory.toString(m, False)
					d = json.loads(data)
		with open(newfile, 'w') as data_file:
			json.dump(d, data_file, indent=2)
		if newfile != f:
			os.remove(f)
	if i:
		print(f'{i} files rewritten')

class JSONValueRewriter:
	def __init__(self, mapping):
		self.mapping = mapping

	def rewrite(self, d, *args, **kwargs):
		with suppress(TypeError):
			if d in self.mapping:
				return self.mapping[d]
		if isinstance(d, dict):
			return {k: self.rewrite(v, *args, **kwargs) for k, v in d.items()}
		elif isinstance(d, list):
			return [self.rewrite(v, *args, **kwargs) for v in d]
		elif isinstance(d, int):
			return d
		elif isinstance(d, float):
			return d
		elif isinstance(d, str):
			return d
		else:
			print(f'failed to rewrite JSON value: {d!r}')
			raise Exception(f'failed to rewrite JSON value: {d!r}')

