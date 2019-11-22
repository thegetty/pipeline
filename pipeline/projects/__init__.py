import sys
import pathlib
import itertools
import json
import warnings

from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

import bonobo
import settings

import pipeline.execution
from pipeline.nodes.basic import \
			AddArchesModel, \
			Serializer

class PipelineBase:
	def __init__(self):
		self.project_name = None
		self.input_path = None

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
