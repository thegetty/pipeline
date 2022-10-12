import os
import os.path
from os.path import exists
import hashlib
import json
import uuid
import pprint
import unittest
from pathlib import Path
from collections import defaultdict
import settings
import pathlib

from cromulent import model, vocab, reader
from cromulent.model import factory
from pipeline.util import CromObjectMerger
from pipeline.projects.sales import SalesPipeline
from pipeline.projects.people import PeoplePipeline
from pipeline.projects.knoedler import KnoedlerPipeline
from pipeline.projects.goupil import GoupilPipeline
from pipeline.projects.aata import AATAPipeline
from pipeline.projects.sales.util import SalesTree
from pipeline.nodes.basic import Serializer, AddArchesModel

MODELS = {
	'Bidding': 'model-bidding',
	'Acquisition': 'model-acquisition',
	'Activity': 'model-activity',
	'SaleActivity': 'model-sale-activity',
	'Event': 'model-event',
	'Group': 'model-groups',
	'HumanMadeObject': 'model-object',
	'LinguisticObject': 'model-lo',
	'Person': 'model-person',
	'Place': 'model-place',
	'ProvenanceEntry': 'model-activity',
	'Production': 'model-production',
	'Set': 'model-set',
	'VisualItem': 'model-visual-item',
	'Inventorying': 'model-inventorying'
}

class TestWriter():
	'''
	Deserialize the output of each resource and store in memory.
	Merge data for multiple serializations of the same resource.
	'''
	def __init__(self):
		self.output = {}
		self.merger = CromObjectMerger()
		super().__init__()

	def __call__(self, data: dict, *args, **kwargs):
		d = data['_OUTPUT']
		dd = json.loads(d)
		dr = data['_ARCHES_MODEL']
		if dr not in self.output:
			self.output[dr] = {}
		uu = data.get('uuid')
		if 'id' in dd:
			uu = hashlib.sha256(dd['id'].encode('utf-8')).hexdigest()
		elif not uu and 'uri' in data:
			uu = hashlib.sha256(data['uri'].encode('utf-8')).hexdigest()
# 			print(f'*** No UUID in top-level resource. Using a hash of top-level URI: {uu}')
		if not uu:
			uu = str(uuid.uuid4())
# 			print(f'*** No UUID in top-level resource;')
# 			print(f'*** Using an assigned UUID filename for the content: {uu}')
		fn = '%s.json' % uu
		data = json.loads(d)
		if fn in self.output[dr]:
			r = reader.Reader()
			model_object = r.read(d)
			merger = self.merger
			content = self.output[dr][fn]
			try:
				m = r.read(content)
				if m == model_object:
					self.output[dr][fn] = data
					return
				else:
					merger.merge(m, model_object)
					self.output[dr][fn] = json.loads(factory.toString(m, False))
					return
			except model.DataError:
				print(f'Exception caught while merging data from {fn}:')
				print(d)
				print(content)
				raise
		else:
			self.output[dr][fn] = data

	def process_model(self, model):
		data = {v['id']: v for v in model.values()}
		return data

	def process_output(self, output):
		data = {k: self.process_model(v) for k, v in output.items()}
		return data

	def processed_output(self):
		return self.process_output(self.output)

##########################################################################################

class SalesTestPipeline(SalesPipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, catalogs, auction_events, contents, **kwargs):
		self.uid_tag_prefix	= 'tag:getty.edu,2019:digital:pipeline:TESTS:REPLACE-WITH-UUID#'
		super().__init__(input_path, catalogs, auction_events, contents, **kwargs)
		self.writer = writer
		self.prev_post_sales_map = {}

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		services.update({
			'problematic_records': {},
			'location_codes': {}
		})
		return services

	def run(self, **options):
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		services = self.get_services(**options)
		super().run(services=services, **options)

		post_map = services['post_sale_map']
		self.generate_prev_post_sales_data(post_map)

	def load_prev_post_sales_data(self):
		return {}

	def persist_prev_post_sales_data(self, post_sale_rewrite_map):
		self.prev_post_sales_map = post_sale_rewrite_map

	def load_sales_tree(self):
		return SalesTree()

	def persist_sales_tree(self, g):
		self.sales_tree = g


class TestSalesPipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		settings.pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', str(pathlib.Path('data/common')))
		settings.pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', str(pathlib.Path('data')))
# 		os.environ['GETTY_PIPELINE_SERVICE_FILES_PATH'] = str(pathlib.Path('data/sales'))
		self.catalogs = {
			'header_file': 'tests/data/sales/sales_catalogs_info_0.csv',
			'files_pattern': 'tests/data/sales/empty.csv',
		}
		self.contents = {
			'header_file': 'tests/data/sales/sales_contents_0.csv',
			'files_pattern': 'tests/data/sales/empty.csv',
		}
		self.auction_events = {
			'header_file': 'tests/data/sales/sales_descriptions_0.csv',
			'files_pattern': 'tests/data/sales/empty.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		catalogs = self.catalogs.copy()
		events = self.auction_events.copy()
		contents = self.contents.copy()
		
		tests_path = Path(f'tests/data/sales/{test_name}')
		catalog_files = list(tests_path.rglob('sales_catalogs_info*'))
		event_files = list(tests_path.rglob('sales_descriptions*'))
		content_files = list(tests_path.rglob('sales_contents*'))

		if catalog_files:
			if exists(str(tests_path / 'sales_catalogs_info_0.csv')):
				catalogs['header_file'] = str(tests_path / 'sales_catalogs_info_0.csv')
			catalogs['files_pattern'] = str(tests_path / 'sales_catalogs_info_[!0]*')

		if event_files:
			if exists(str(tests_path / 'sales_descriptions_0.csv')):
				events['header_file'] = str(tests_path / 'sales_descriptions_0.csv')
			events['files_pattern'] = str(tests_path / 'sales_descriptions_[!0]*')

		if content_files:
			if exists(str(tests_path / 'sales_contents_0.csv')):
				contents['header_file'] = str(tests_path / 'sales_contents_0.csv')
			contents['files_pattern'] = str(tests_path / 'sales_contents_[!0]*')
		
		writer = TestWriter()
		pipeline = SalesTestPipeline(
				writer,
				input_path,
				catalogs=catalogs,
				auction_events=events,
				contents=contents,
				models=MODELS,
				limit=100,
				debug=True
		)
		pipeline.run()
		self.prev_post_sales_map = pipeline.prev_post_sales_map
		return writer.processed_output()

##########################################################################################

class AATATestPipeline(AATAPipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, *args, **kwargs):
		self.uid_tag_prefix	= 'tag:getty.edu,2019:digital:pipeline:TESTS:REPLACE-WITH-UUID#'
		super().__init__(input_path, *args, **kwargs)
		self.writer = writer

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
# 		services.update({
# 		})
		return services

	def run(self, **options):
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		services = self.get_services(**options)
		super().run(services=services, **options)


class TestAATAPipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		settings.pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', str(pathlib.Path('data/common')))
		settings.pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', str(pathlib.Path('data')))
# 		os.environ['GETTY_PIPELINE_SERVICE_FILES_PATH'] = str(pathlib.Path('data/aata'))
		self.patterns = {
			'abstracts_pattern': 'tests/data/aata/empty.xml',
			'journals_pattern': 'tests/data/aata/empty.xml',
			'series_pattern': 'tests/data/aata/empty.xml',
			'people_pattern': 'tests/data/aata/empty.xml',
			'corp_pattern': 'tests/data/aata/empty.xml',
			'geog_pattern': 'tests/data/aata/empty.xml',
			'subject_pattern': 'tests/data/aata/empty.xml',
			'tal_pattern': 'tests/data/aata/empty.xml',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		
		tests_path = Path(f'tests/data/aata/{test_name}')

		patterns = {
			'abstracts_pattern': 'AATA_[0-9]*.xml',
			'journals_pattern': 'AATA*Journal.xml',
			'series_pattern': 'AATA*Series.xml',
			'people_pattern': 'Auth_person.xml',
			'corp_pattern': 'Auth_corp.xml',
			'geog_pattern': 'Auth_geog.xml',
			'subject_pattern': 'Auth_subject.xml',
			'tal_pattern': 'Auth_TAL.xml'
		}

		kwargs = self.patterns.copy()
		for k, pattern in patterns.items():
			files = list(tests_path.rglob(pattern))
			if files:
				kwargs[k] = str(tests_path / pattern)

		writer = TestWriter()
		pipeline = AATATestPipeline(
				writer,
				input_path,
				models=MODELS,
				limit=100,
				debug=True,
				**kwargs,
		)
		pipeline.run()
		return writer.processed_output()

	def verify_content(self, data, **kwargs):
		for k, expected in kwargs.items():
			self.assertIn(k, data)
			got = data.get(k)
			if isinstance(got, list):
				values = [g['content'] for g in got]
				self.assertIn(expected, values)
			else:
				value = got['content']
				self.assertEqual(value, expected)

	def verify_property(self, data, property, **kwargs):
		for k, expected in kwargs.items():
			self.assertIn(k, data)
			got = data.get(k)
			if isinstance(got, list):
				values = [g[property] for g in got]
				self.assertIn(expected, values)
			else:
				value = got[property]
				self.assertEqual(value, expected)

	def get_classification_labels(self, data):
		cl = data.get('classified_as', [])
		for c in cl:
			clabel = c['_label']
			yield clabel
		
	def get_typed_referrers(self, data):
		return self.get_typed_content('referred_to_by', data)
		
	def get_typed_identifiers(self, data):
		return self.get_typed_content('identified_by', data)
		
	def get_typed_content(self, prop, data):
		identified_by = data.get(prop, [])
		identifiers = defaultdict(set)
		for i in identified_by:
			content = i['content']
			for clabel in self.get_classification_labels(i):
				identifiers[clabel].add(content)
		for k in identifiers.keys():
			if len(identifiers[k]) == 1:
				identifiers[k] = identifiers[k].pop()
		return dict(identifiers)
		
	def verify_place_hierarchy(self, places, place, expected_names):
		while place:
			expected = expected_names.pop(0)
			self.verify_content(place, identified_by=expected)
			place = place.get('part_of', [])
			if place:
				i = place[0]['id']
				place = places.get(i)
		self.assertEqual(len(expected_names), 0)

##########################################################################################

class KnoedlerTestPipeline(KnoedlerPipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, data, **kwargs):
		self.uid_tag_prefix	= 'tag:getty.edu,2019:digital:pipeline:TESTS:REPLACE-WITH-UUID#'
		super().__init__(input_path, data, **kwargs)
		self.writer = writer

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		services.update({
			'problematic_records': {},
			'location_codes': {},
		})
		return services

	def run(self, **options):
		vocab.conceptual_only_parts()
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		services = self.get_services(**options)
		super().run(services=services, **options)


class TestKnoedlerPipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		settings.pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', str(pathlib.Path('data/common')))
		settings.pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', str(pathlib.Path('data')))
# 		os.environ['GETTY_PIPELINE_SERVICE_FILES_PATH'] = str(pathlib.Path('data/knoedler'))

		# os.environ['GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH'] = 'data/common'
		self.data = {
			'header_file': 'tests/data/knoedler/knoedler_0.csv',
			'files_pattern': 'knoedler.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		data = self.data.copy()
		
		tests_path = Path(f'tests/data/knoedler/{test_name}')
		files = list(tests_path.rglob('knoedler_ar*'))
		
		if files:
			data['files_pattern'] = str(tests_path / 'knoedler_ar*')

		writer = TestWriter()
		pipeline = KnoedlerTestPipeline(
				writer,
				input_path,
				data=data,
				models=MODELS,
				limit=100,
				debug=True
		)
		pipeline.run()
		return writer.processed_output()

##########################################################################################

class PeopleTestPipeline(PeoplePipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, data, **kwargs):
		self.uid_tag_prefix	= 'tag:getty.edu,2019:digital:pipeline:TESTS:REPLACE-WITH-UUID#'
		super().__init__(input_path, data, **kwargs)
		self.writer = writer

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		services.update({
			'problematic_records': {},
			'location_codes': {},
		})
		return services

	def run(self, **options):
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		services = self.get_services(**options)
		super().run(services=services, **options)


class TestPeoplePipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		settings.pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', str(pathlib.Path('data/common')))
		settings.pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', str(pathlib.Path('data')))
# 		os.environ['GETTY_PIPELINE_SERVICE_FILES_PATH'] = str(pathlib.Path('data/people'))

		# os.environ['GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH'] = 'data/common'
		self.data = {
			'header_file': 'tests/data/people/people_authority_0.csv',
			'files_pattern': 'people_authority.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		data = self.data.copy()
		
		tests_path = Path(f'tests/data/people/{test_name}')
		files = list(tests_path.rglob('people_authority_ar*'))
		
		if files:
			data['files_pattern'] = str(tests_path / 'people_authority_ar*')

		writer = TestWriter()
		pipeline = PeopleTestPipeline(
				writer,
				input_path,
				data=data,
				models=MODELS,
				limit=100,
				debug=True
		)
		pipeline.run()
		return writer.processed_output()

##########################################################################################


class GoupilTestPipeline(GoupilPipeline):
	'''
	Test Goupil pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, data, **kwargs):
		self.uid_tag_prefix	= 'tag:getty.edu,2019:digital:pipeline:TESTS:REPLACE-WITH-UUID#'
		super().__init__(input_path, data, **kwargs)
		self.writer = writer

	def serializer_nodes_for_model(self, *args, model=None, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		nodes.append(Serializer(compact=False))
		nodes.append(self.writer)
		return nodes

	def get_services(self):
		services = super().get_services()
		# setup rest of needed services here
		return services

	def run(self, **options):
		vocab.conceptual_only_parts()
		vocab.add_linked_art_boundary_check()
		vocab.add_attribute_assignment_check()
		services = self.get_services(**options)
		super().run(services=services, **options)


class TestGoupilPipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Goupil pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		settings.pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', str(pathlib.Path('data/common')))
		settings.pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', str(pathlib.Path('data')))

		self.data = {
			'header_file': 'tests/data/goupil/goupil_0.csv',
			'files_pattern': 'goupil.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		data = self.data.copy()
		
		tests_path = Path(f'tests/data/goupil/{test_name}')
		
		files = list(tests_path.rglob('goupil_[!0]*'))
		headers = list(tests_path.rglob('goupil_0*'))
		
		if files:
			data['files_pattern'] = str(tests_path / 'goupil_[!0]*')
		
		if headers:
			data['header_file'] = str(headers[0])
		
		writer = TestWriter()
		pipeline = GoupilTestPipeline(
				writer,
				input_path,
				data=data,
				models=MODELS,
				limit=100,
				debug=True
		)
		pipeline.run()
		return writer.processed_output()


##########################################################################################

def classified_identifiers(data, key='identified_by'):
	classified_identifiers = {}
	identifiers = [(i['content'], i.get('classified_as', [])) for i in data.get(key, [])]
	for (content, classification) in identifiers:
		if len(classification):
			for cl in classification:
				label = cl['_label']
				classified_identifiers[label] = content
		else:
			classified_identifiers[None] = content
	return classified_identifiers

def classified_identifier_sets(data, key='identified_by'):
	classified_identifiers = defaultdict(set)
	identifiers = [(i.get('content'), i.get('classified_as', [])) for i in data.get(key, [])]
	for (content, classification) in identifiers:
		if content:
			if len(classification):
				for cl in classification:
					label = cl['_label']
					classified_identifiers[label].add(content)
			else:
				classified_identifiers[None].add(content)
	return classified_identifiers

def classification_sets(data, key='_label', classification_key='classified_as'):
	classification_set = set()
	classification = data.get(classification_key, [])
	if len(classification):
		for cl in classification:
			label = cl.get(key)
			classification_set.add(label)
	return classification_set

def classification_tree(data, key='_label'):
	tree = {}
	classification = data.get('classified_as', [])
	if len(classification):
		for cl in classification:
			label = cl[key]
			tree[label] = classification_tree(cl, key=key)
	return tree
