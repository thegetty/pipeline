import sys
import pathlib
import itertools
import json
import bonobo
import settings
from sqlalchemy import create_engine

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
			print(f'SQLITE SERVICE: {file.stem}')
			return create_engine(f'sqlite://%s' % (file.absolute(),))

	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = {
			'trace_counter': itertools.count(),
			f'fs.data.{self.project_name}': bonobo.open_fs(self.input_path)
		}

		common_path = pathlib.Path(settings.pipeline_common_service_files_path)
		for file in common_path.rglob('*'):
			service = self._service_from_path(file)
			if service:
				services[file.stem] = service

		proj_path = pathlib.Path(settings.pipeline_project_service_files_path(self.project_name))
		for file in proj_path.rglob('*'):
			if file.stem in services:
				print(f'*** Project is overloading a shared service JSON file: {file.stem}')
			service = self._service_from_path(file)
			if service:
				services[file.stem] = service

		return services

	def serializer_nodes_for_model(self, model=None):
		nodes = []
		if model:
			nodes.append(AddArchesModel(model=model))
		if self.debug:
			nodes.append(Serializer(compact=False))
		else:
			nodes.append(Serializer(compact=True))
		return nodes

	def add_serialization_chain(self, graph, input_node, model=None):
		'''Add serialization of the passed transformer node to the bonobo graph.'''
		nodes = self.serializer_nodes_for_model(model=model)
		if nodes:
			graph.add_chain(*nodes, _input=input_node)
		else:
			sys.stderr.write('*** No serialization chain defined\n')
