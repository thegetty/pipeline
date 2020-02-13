import os
import re
import sys
import ujson as json
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

def chunks(l, size):
	if len(l):
		for i in range(0, len(l), size):
			yield l[i:i+size]

def rewrite_output_files(r, update_filename=False, parallel=False, **kwargs):
	print(f'Rewriting JSON output files')
	p = Path(output_file_path)
	files = sorted(p.rglob('*.json'))

	if 'content_filter_re' in kwargs:
		print('rewriting with content filter: {kwargs["content_filter_re"]}')
	if parallel:
		j = 16
		pool = multiprocessing.Pool(j)

		partition_size = max(min(10000, int(len(files)/j)), 10)
		file_partitions = list(chunks(files, partition_size))
		args = list((file_partition, r, update_filename, i+1, len(file_partitions), kwargs) for i, file_partition in enumerate(file_partitions))
		print(f'{len(args)} worker partitions with size {partition_size}')
		_ = pool.starmap(_rewrite_output_files, args)
	else:
		_rewrite_output_files(files, r, update_filename, 1, 1, kwargs)

def _rewrite_output_files(files, r, update_filename, worker_id, total_workers, kwargs):
	i = 0
	if not files:
		return
	print(f'rewrite worker {worker_id} called with {len(files)} files [{files[0]} .. {files[-1]}]')
	rewritten_count = 0
	processed_count = 0
	for i, f in enumerate(files):
		processed_count += 1
		# print(f'{i} {f}', end="\r", flush=True)
		with open(f) as data_file:
			try:
				bytes = data_file.read()
				if 'content_filter_re' in kwargs:
					filter_re = kwargs['content_filter_re']
					if not re.search(filter_re, bytes):
						print(f'skipping   {f}')
						continue
					else:
						print(f'processing {f}')
				data = json.loads(bytes)
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
					except model.DataError as e:
						print(f'Exception caught while merging data from {newfile} ({str(e)}):')
						print(d)
						print(content)
						raise
					data = factory.toString(m, False)
					d = json.loads(data)
		with open(newfile, 'w') as data_file:
			rewritten_count += 1
			json.dump(d, data_file, indent=2, ensure_ascii=False)
		if newfile != f:
			os.remove(f)
	if rewritten_count:
		print(f'worker {worker_id}/{total_workers} finished with {rewritten_count}/{processed_count} files rewritten')
	else:
		print(f'worker {worker_id}/{total_workers} finished')

class JSONValueRewriter:
	def __init__(self, mapping, prefix=False):
		self.mapping = mapping
		self.prefix = prefix

	def rewrite(self, d, *args, **kwargs):
		with suppress(TypeError):
			if d in self.mapping:
				return self.mapping[d]
			if self.prefix:
				if isinstance(d, str):
					prefixes = [k for k in self.mapping if len(k) < len(d) and k == d[:len(k)]]
					if prefixes:
						k = prefixes[0]
						replace = self.mapping[k]
						updated = replace + d[len(replace):]
						return updated
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
