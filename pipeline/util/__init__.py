from threading import Lock
from contextlib import ContextDecorator

def identity(d):
	'''
	Simply yield the value that is passed as an argument.
	
	This is trivial, but necessary for use in constructing some bonobo graphs.
	For example, if two already instantiated graph chains need to be connected,
	one being used as input to the other, bonobo does not allow this:
	
	`graph.add_chain(_input=prefix.output, _output=suffix.input)`
	
	Instead, the `add_chain` call requires at least one graph node to be added. Hence:

	`graph.add_chain(identity, _input=prefix.output, _output=suffix.input)`
	'''
	yield d

class ExclusiveValue(ContextDecorator):
	_locks = {}
	lock = Lock()

	def __init__(self, wrapped):
		self._wrapped = wrapped

	def get_lock(self):
		_id = self._wrapped
		with ExclusiveValue.lock:
			if not _id in ExclusiveValue._locks:
				ExclusiveValue._locks[_id] = Lock()
		return ExclusiveValue._locks[_id]

	def __enter__(self):
		self.get_lock().acquire()
		return self._wrapped

	def __exit__(self, *exc):
		self.get_lock().release()
