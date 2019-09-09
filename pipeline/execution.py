from bonobo.config import use, Option, Service, Configurable
from bonobo.constants import BEGIN, NOT_MODIFIED
from bonobo.util import get_name, isconfigurabletype, isconfigurable
import inspect
import bonobo
import types
import pprint
from functools import partial

class GraphExecutor(object):
	def __init__(self, graph, services):
		self.graph = graph
		self.services = services.copy()
		self.service_bindings = {}
		self.runtime_bindings = {}
		for ix in graph.topologically_sorted_indexes:
			node = graph[ix]
			name = get_name(node)
			options = dict(getattr(node, '__options__', {}))
			self.service_bindings[ix] = []
			self.runtime_bindings[ix] = []
			for k, v in options.items():
				if isinstance(v, Service):
					s = services.get(k)
					if s is not None:
						self.service_bindings[ix].append((k, s))
					else:
						self.runtime_bindings[ix].append(k)
# 			print(f'[{name}] static services: {self.service_bindings[ix]}')
# 			print(f'[{name}] runtime services: {self.runtime_bindings[ix]}')
# 		for i in self.graph.outputs_of(BEGIN):
# 			self.print_tree(i)

	def run(self):
		g = self.graph
		for i in g.outputs_of(BEGIN):
			self.run_node(i, None, level=0)
# 		raise Exception

	def node(self, i):
		g = self.graph
		node = g[i]
		return node.wrapped

	def run_node(self, i, input, level=0):
		g = self.graph
		node = g[i]
		name = get_name(node)
		indent = '  ' * level
		services = {k: v for k, v in self.service_bindings[i] if v is not None}
		for k in self.runtime_bindings[i]:
			s = getattr(node, k)
			services[k] = self.services[s]

		if services:
			# print(f'*** binding services: {services}')
			node = partial(node, **services)

# 		if services:
# 			print(f'{indent}{name} {services.keys()}')
# 		else:
# 			print(f'{indent}{name}')
		
		try:
			# print(f'calling {node!r}({input})')
			if input is None:
				result = node()
			else:
				result = node(input)

			if result == NOT_MODIFIED:
				result = input

			if isinstance(result, types.GeneratorType):
				#print('RESULT IS A GENERATOR')
				for r in result:
					#print(f'[{name}] =gen=> {r}')
					for j in g.outputs_of(i):
						self.run_node(j, r, level=level+1)
			else:
				#print(f'RESULT IS {result}')
				#print(f'[{name}] =ret=> {result}')
				for j in g.outputs_of(i):
					self.run_node(j, result, level=level+1)
		except Exception as e:
			print(f'**** ERROR: {e!r}')

	def print_tree(self, i, level=0):
		g = self.graph
		node = g[i]
		name = get_name(node)
		indent = '  ' * level
		print(f'{indent}{name}')
		for j in g.outputs_of(i):
			self.print_tree(j, level=level+1)
