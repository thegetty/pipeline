import unittest
import os
import json
from contextlib import suppress
import pprint
from cromulent import model, vocab
from pipeline.io.file import MergingFileWriter
from cromulent.model import factory

class MergingFileWriterTests(unittest.TestCase):
	def setUp(self):
		self.path = '/tmp/pipeline_tests'
		self.writer = MergingFileWriter(directory=self.path)
		if not os.path.exists(self.path):
			os.mkdir(self.path)
		for uuid in ('0001', '0002'):
			f = self.expected_file(uuid)
			with suppress(FileNotFoundError):
				os.remove(f)

	def expected_file(self, id):
		f = os.path.join(self.path, 'test-model', f'{id}.json')
		return f

	def write_obj1(self, id):
		'''Writes a Person model object with a label and a PrimaryName'''
		p1 = vocab.Person(ident=f'urn:{id}', label='Greg')
		p1.identified_by = vocab.PrimaryName(content='Gregory Williams')
		w = MergingFileWriter(directory=self.path)
		d = self.obj_to_dict(p1, id)
		self.writer(d)

	def write_obj2(self, id):
		'''Writes a Person model object with a 'born' event'''
		p2 = vocab.Person(ident='urn:foo')
		p2.born = model.Birth()
		w = MergingFileWriter(directory=self.path)
		d = self.obj_to_dict(p2, id)
		self.writer(d)

	def obj_to_dict(self, o, uuid):
		d = {
			'_ARCHES_MODEL': 'test-model',
			'_CROM_FACTORY': factory,
			'uuid': uuid,
			'_OUTPUT': factory.toString(o, False)
		}
		return d

	def test_merge(self):
		id = '0001'
		self.write_obj1(id)
		self.write_obj2(id)
		f = self.expected_file(id)
		print(f'# expected file {f}')
		self.assertTrue(os.path.exists(f), 'merged file exists')
		with open(f) as f:
			j = json.load(f)
			self.assertEqual(j.get('_label'), 'Greg')
			self.assertIsInstance(j.get('born'), dict)

	def _write_two_objects(self, p1, p2, uuid):
		self.writer(self.obj_to_dict(p1, uuid))
		self.writer(self.obj_to_dict(p2, uuid))

	def _new_object(self, uuid):
		p = vocab.Person(ident=f'urn:{uuid}')
		p.identified_by = vocab.Identifier(content='Gregory Williams')
		return p

	def test_merge_multiple_identifiers(self):
		'''
		When merging two objects with the same Identifier content, ensure that the
		resulting object only has one Identifier.
		'''
		factory.auto_id_type = 'uuid'
		uri = 'tag:kasei.us,2019,test'

		uuid = '0002'
		p1 = self._new_object(uuid)
		p2 = self._new_object(uuid)

		self._write_two_objects(p1, p2, uuid)
		f = self.expected_file(uuid)
		print(f'# expected file {f}')
		self.assertTrue(os.path.exists(f), 'merged file exists')
		with open(f) as f:
			j = json.load(f)
			ids = j['identified_by']
			self.assertEqual(len(ids), 1)
			self.assertEqual(ids[0]['content'], 'Gregory Williams')
		

if __name__ == '__main__':
	unittest.main()
