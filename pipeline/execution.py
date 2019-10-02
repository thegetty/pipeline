import os
# import sys
import types
from functools import partial
import time
from collections import Counter, defaultdict, namedtuple

from bonobo.config import use, Option, Service, Configurable
from bonobo.constants import BEGIN, NOT_MODIFIED
from bonobo.util import get_name, isconfigurabletype, isconfigurable
import settings

class GraphExecutor(object):
	'''
	Run a bonobo graph sequentially on a single thread, allowing easier debugging
	and profiling.
	'''
	def __init__(self, graph, services):
		file = open(os.path.join(settings.output_file_path, 'pipeline.counters'), 'wt', buffering=1)
# 		if not file:
# 			file = sys.stdout
		self.file = file
		self.counters_in = defaultdict(int)
		self.counters_out = defaultdict(int)
		self.timers = defaultdict(float)
		self.start_time = time.time()
		self.next_emit_time = self.start_time + 10.0
		self.graph = graph
		self.services = services.copy()
		self.service_bindings = {}
		self.runtime_bindings = {}
		for ix in graph.topologically_sorted_indexes:
			node = graph[ix]
# 			name = get_name(node)
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
		self.print_counts()
		print('================ DONE ================', file=self.file)

	def node(self, i):
		g = self.graph
		node = g[i]
		return node.wrapped

	def print_counts(self):
		file = self.file
		cur = time.time()
		if cur >= self.next_emit_time:
			self.next_emit_time += 10.0
			elapsed = cur - self.start_time
			keys = sorted(self.timers.keys(), key=lambda k: self.timers[k], reverse=True)
			print(f'============= %1.fs' % (elapsed,), file=file)
			for k in keys:
				elapsed = self.timers[k]
				j, level, name = k
				message = name
				message += f' [in={self.counters_in[j]}, out={self.counters_out[j]}]'
				print(f'%7.2f\t{message}' % (self.timers[k]), file=file)

	def tick_in(self, i, name, level):
		self.counters_in[i] += 1
		self.print_counts()

	def tick_out(self, i, name, level):
		self.counters_out[i] += 1

	def run_node(self, i, input, level=0):
		g = self.graph
		node = g[i]
		name = get_name(node)
		self.tick_in(i, name, level)
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
			start = time.time()
			if input is None:
				result = node()
			else:
				result = node(input)
			end = time.time()
			elapsed = end - start
			self.timers[(i, level, name)] += elapsed

			if result == NOT_MODIFIED:
				result = input

			if isinstance(result, types.GeneratorType):
				#print('RESULT IS A GENERATOR')
				for r in result:
					self.tick_out(i, name, level)
					#print(f'[{name}] =gen=> {r}')
					for j in g.outputs_of(i):
						self.run_node(j, r, level=level+1)
			else:
				self.tick_out(i, name, level)
				#print(f'RESULT IS {result}')
				#print(f'[{name}] =ret=> {result}')
				for j in g.outputs_of(i):
					self.run_node(j, result, level=level+1)
		except Exception as e:
			print(f'**** ERROR: {e!r}')
# 			raise

	def print_tree(self, i, level=0):
		g = self.graph
		node = g[i]
		name = get_name(node)
		indent = '  ' * level
		print(f'{indent}{name}')
		for j in g.outputs_of(i):
			self.print_tree(j, level=level+1)
