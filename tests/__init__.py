import os
import os.path
import hashlib
import json
import uuid
import pprint
import unittest
from pathlib import Path

from cromulent import model, reader
from cromulent.model import factory
from pipeline.util import CromObjectMerger
from pipeline.projects.provenance import ProvenancePipeline
from pipeline.projects.provenance.util import SalesTree
from pipeline.nodes.basic import Serializer, AddArchesModel

MODELS = {
	'Bidding': 'model-bidding',
	'AuctionOfLot': 'model-auction-of-lot',
	'Acquisition': 'model-acquisition',
	'Activity': 'model-activity',
	'Event': 'model-event',
	'Group': 'model-groups',
	'HumanMadeObject': 'model-object',
	'LinguisticObject': 'model-lo',
	'Person': 'model-person',
	'Place': 'model-place',
	'ProvenanceEntry': 'model-activity',
	'Production': 'model-production',
	'Set': 'model-set',
	'VisualItem': 'model-visual-item'
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


class ProvenanceTestPipeline(ProvenancePipeline):
	'''
	Test Provenance pipeline subclass that allows using a custom Writer.
	'''
	def __init__(self, writer, input_path, catalogs, auction_events, contents, **kwargs):
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
		services = self.get_services(**options)
		super().run(services=services, **options)

		counter = services['lot_counter']
		post_map = services['post_sale_map']
		self.generate_prev_post_sales_data(counter, post_map)

	def load_prev_post_sales_data(self):
		return {}

	def persist_prev_post_sales_data(self, post_sale_rewrite_map):
		self.prev_post_sales_map = post_sale_rewrite_map

	def load_sales_tree(self):
		return SalesTree()

	def persist_sales_tree(self, g):
		self.sales_tree = g


class TestProvenancePipelineOutput(unittest.TestCase):
	'''
	Parse test CSV data and run the Provenance pipeline with the in-memory TestWriter.
	Then verify that the serializations in the TestWriter object are what was expected.
	'''
	def setUp(self):
		self.catalogs = {
			'header_file': 'tests/data/pir/sales_catalogs_info_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		self.contents = {
			'header_file': 'tests/data/pir/sales_contents_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		self.auction_events = {
			'header_file': 'tests/data/pir/sales_descriptions_0.csv',
			'files_pattern': 'tests/data/pir/empty.csv',
		}
		os.environ['QUIET'] = '1'

	def tearDown(self):
		pass

	def run_pipeline(self, test_name):
		input_path = os.getcwd()
		catalogs = self.catalogs.copy()
		events = self.auction_events.copy()
		contents = self.contents.copy()
		
		tests_path = Path(f'tests/data/pir/{test_name}')
		catalog_files = list(tests_path.rglob('sales_catalogs_info*'))
		event_files = list(tests_path.rglob('sales_descriptions*'))
		content_files = list(tests_path.rglob('sales_contents*'))
		
		if catalog_files:
			catalogs['files_pattern'] = str(tests_path / 'sales_catalogs_info*')

		if event_files:
			events['files_pattern'] = str(tests_path / 'sales_descriptions*')

		if content_files:
			contents['files_pattern'] = str(tests_path / 'sales_contents*')
		
		writer = TestWriter()
		pipeline = ProvenanceTestPipeline(
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
