import unittest
import os
import json
import pprint
from cromulent import model, vocab
from pipeline.io.file import MergingFileWriter
from cromulent.model import factory

class MergingFileWriterTests(unittest.TestCase):
	def setUp(self):
		self.path = '/tmp/pipeline_tests'
		if not os.path.exists(self.path):
			os.mkdir(self.path)
		f = self.expected_file('0001')
		os.remove(f)

	def expected_file(self, id):
		f = os.path.join(self.path, 'test-model', f'{id}.json')
		return f

	def setUp(self):
		self.path = '/tmp/pipeline_tests'
		if not os.path.exists(self.path):
			os.mkdir(self.path)
		self.writer = MergingFileWriter(directory=self.path)

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

if __name__ == '__main__':
	unittest.main()
