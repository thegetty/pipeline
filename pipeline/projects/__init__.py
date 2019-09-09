import sys
import pathlib
import itertools
import json
import bonobo
import settings
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL

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
					print(f'*** Project is overloading a shared service file: {file}')
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

	def run_graph(self, graph, *, services):
		bonobo.run(graph, services=services)

##########################################################################################
##########################################################################################
##########################################################################################

from bonobo.execution.contexts.graph import BaseGraphExecutionContext
from bonobo.execution import events
from bonobo.constants import BEGIN, EMPTY, END
from queue import Empty
from bonobo.errors import InactiveReadableError
from bonobo.util.statistics import WithStatistics
from bonobo.execution.contexts.base import BaseContext

class SimpleGraphExecutionContext(BaseGraphExecutionContext):
	def __init__(self, *args, **kwargs):
		super(SimpleGraphExecutionContext, self).__init__(*args, **kwargs)
		for i, node in enumerate(self):
			print(f'[{i}] {node!r}')

	def start(self, starter=None):
		super(SimpleGraphExecutionContext, self).start()

		self.register_plugins()
		self.dispatch(events.START)
# 		self.tick(pause=False)

		for node in self.nodes:
			print(f'starting node {node}')
			if starter is None:
				node.start()
			else:
				starter(node)

		self.dispatch(events.STARTED)

# 	def tick(self, pause=True):
# 		self.dispatch(events.TICK)
# 		if pause:
# 			sleep(self.TICK_PERIOD)

	def stop(self, stopper=None):
		super(SimpleGraphExecutionContext, self).stop()

		self.dispatch(events.STOP)
		for node_context in self.nodes:
			if stopper is None:
				node_context.stop()
			else:
				stopper(node_context)
# 		self.tick(pause=False)
		self.dispatch(events.STOPPED)
		self.unregister_plugins()

	def kill(self):
		super(SimpleGraphExecutionContext, self).kill()

		self.dispatch(events.KILL)
		for node_context in self.nodes:
			node_context.kill()
# 		self.tick()

	def write(self, *messages):
		"""Push a list of messages in the inputs of this graph's inputs, matching the output of special node "BEGIN" in
		our graph."""

		for i in self.graph.outputs_of(BEGIN):
			for message in messages:
				self[i].write(message)

	def loop(self):
		nodes = set(node for node in self.nodes if node.should_loop)
		while self.should_loop and len(nodes):
# 			self.tick(pause=False)
			for node in list(nodes):
				try:
					node.step()
				except Empty:
					continue
				except InactiveReadableError:
					nodes.discard(node)

	def run_until_complete(self):
		self.write(BEGIN, EMPTY, END)
		self.loop()

##########################################################################################
##########################################################################################
##########################################################################################


import bonobo.execution.strategies
from bonobo.execution.strategies.base import Strategy
from bonobo.execution.strategies.naive import NaiveStrategy
from bonobo.execution.contexts.graph import GraphExecutionContext
class MyExecutionStrategy(NaiveStrategy):
	def execute(self, graph, **kwargs):
		print(f'======= MyExecutionStrategy.execute()')
# 		context = GraphExecutionContext(graph, **kwargs)
		context = SimpleGraphExecutionContext(graph, **kwargs)
		with context:
			print(f'context: {context}')
			context.run_until_complete()
		return context

bonobo.execution.strategies.STRATEGIES['__getty'] = MyExecutionStrategy
bonobo.execution.strategies.DEFAULT_STRATEGY = '__getty'
