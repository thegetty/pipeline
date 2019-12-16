import sys
import pathlib
import pprint
import itertools
import json
import warnings
from collections import defaultdict

import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

import bonobo
import settings

import pipeline.execution
from cromulent import model, vocab
from pipeline.nodes.basic import \
			AddArchesModel, \
			Serializer

class StaticInstanceHolder:
	'''
	This class wraps a dict that holds a set of crom objects, categorized by model name.
	
	Access to those objects is recorded, and at the end of a pipeline run, just those
	objects that were accessed can be returned (to be serialized). This helps to avoid
	serializing objects that are not relevant to a specific pipeline run (e.g. defined
	for use in another dataset).
	'''
	def __init__(self, instances):
		self.instances = instances
		self.used = set()

	def get_instance(self, model, name):
		self.used.add((model, name))
		return self.instances[model][name]

	def used_instances(self):
		used = defaultdict(dict)
		for model, name in self.used:
			used[model][name] = self.instances[model][name]
		return used

class PipelineBase:
	def __init__(self, project_name, *, helper):
		self.project_name = project_name
		self.input_path = None
		self.helper = helper
		self.static_instances = StaticInstanceHolder(self.setup_static_instances())
		helper.add_services(self.get_services())

	def setup_static_instances(self):
		'''
		These are instances that are used statically in the code. For example, when we
		provide attribution of an identifier to Getty, or use a Lugt number, we need to
		serialize the related Group or Person record for that attribution, even if it does
		not appear in the source data.
		'''
		GETTY_GRI_URI = self.helper.make_proj_uri('ORGANIZATION', 'LOCATION-CODE', 'JPGM')
		lugt_ulan = 500321736
		gri_ulan = 500115990
		LUGT_URI = self.helper.make_proj_uri('PERSON', 'ULAN', lugt_ulan)
		gri = model.Group(ident=GETTY_GRI_URI, label='Getty Research Institute')
		gri.identified_by = vocab.PrimaryName(ident='', content='Getty Research Institute')
		gri.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{gri_ulan}')
		lugt = model.Person(ident=LUGT_URI, label='Frits Lugt')
		lugt.identified_by = vocab.PrimaryName(ident='', content='Frits Lugt')
		lugt.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{lugt_ulan}')
		return {
			'Group': {
				'gri': gri
			},
			'Person': {
				'lugt': lugt
			}
		}

	def _service_from_path(self, file):
		if file.suffix == '.json':
			with open(file, 'r') as f:
				return json.load(f)
		elif file.suffix == '.sqlite':
			s = URL(drivername='sqlite', database=file.absolute())
			e = create_engine(s)
			return e

	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = {
			'trace_counter': itertools.count(),
			f'fs.data.{self.project_name}': bonobo.open_fs(self.input_path)
		}

		common_path = pathlib.Path(settings.pipeline_common_service_files_path)
		print(f'Common path: {common_path}')
		for file in common_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				services[file.stem] = service

		proj_path = pathlib.Path(settings.pipeline_project_service_files_path(self.project_name))
		print(f'Project path: {proj_path}')
		for file in proj_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				if file.stem in services:
					warnings.warn(f'*** Project is overloading a shared service file: {file}')
				services[file.stem] = service

		return services

	def serializer_nodes_for_model(self, model=None, *args, **kwargs):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		if self.debug:
			nodes.append(Serializer(compact=False))
		else:
			nodes.append(Serializer(compact=True))
		return nodes

	def add_serialization_chain(self, graph, input_node, model=None, *args, **kwargs):
		'''Add serialization of the passed transformer node to the bonobo graph.'''
		nodes = self.serializer_nodes_for_model(*args, model=model, **kwargs)
		if nodes:
			graph.add_chain(*nodes, _input=input_node)
		else:
			sys.stderr.write('*** No serialization chain defined\n')

	def run_graph(self, graph, *, services):
		if True:
			bonobo.run(graph, services=services)
		else:
			e = pipeline.execution.GraphExecutor(graph, services)
			e.run()

class UtilityHelper:
	def __init__(self, project_name):
		self.project_name = project_name
		self.proj_prefix = f'tag:getty.edu,2019:digital:pipeline:{project_name}:REPLACE-WITH-UUID#'
		self.shared_prefix = f'project_nametag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID#'

	def add_services(self, services):
		'''
		Called from PipelineBase.__init__, this is used for dependency
		injection of services data.
		
		This would ordinarily go in UtilityHelper.__init__, but this object
		is constructed before the pipeline object which is used to produce
		the services dict. So the pipeline object injects the data in *its*
		constructor instead.
		'''
		self.services = services

	def make_proj_uri(self, *values):
		'''Convert a set of identifying `values` into a URI'''
		if values:
			suffix = ','.join([urllib.parse.quote(str(v)) for v in values])
			return self.proj_prefix + suffix
		else:
			suffix = str(uuid.uuid4())
			return self.proj_prefix + suffix

	def make_shared_uri(self, *values):
		'''Convert a set of identifying `values` into a URI'''
		if values:
			suffix = ','.join([urllib.parse.quote(str(v)) for v in values])
			return self.shared_prefix + suffix
		else:
			suffix = str(uuid.uuid4())
			return self.shared_prefix + suffix
