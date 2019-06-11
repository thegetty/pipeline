import unittest
import pprint
from datetime import datetime
from pipeline.util import Dimension
import pipeline.util.cleaners

class TestDateCleaners(unittest.TestCase):
	'''
	Test the ability to recognize and parse various formats of dates.
	'''
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_parse_dimensions(self):
		'''
		Test the documented formats that `pipeline.util.cleaners.parse_dimensions` can parse
		and ensure that it returns the expected data.
		'''
		tests = {
			'''2 in''': [Dimension('2', 'inches', None)],
			'''2'8"''': [Dimension('2', 'feet', None), Dimension('8', 'inches', None)],
			'4cm': [Dimension('4', 'centimeters', None)],
			'1': None,
		}

		for value, expected in tests.items():
			dims = pipeline.util.cleaners.parse_dimensions(value)
			if expected is not None:
				self.assertIsInstance(dims, list)
				self.assertEqual(dims, expected, msg=f'dimensions: {value!r}')
			else:
				self.assertIsNone(dims)

	def test_dimension_cleaner(self):
		'''
		Test the documented formats that `pipeline.util.cleaners.dimensions_cleaner` can parse
		and ensure that it returns the expected data.
		'''
		tests = {
			'''2 in by 1 in''': ([Dimension('2', 'inches', None)], [Dimension('1', 'inches', None)]),
			'''2'2"h x 2'8"w''': ([Dimension('2', 'feet', 'height'), Dimension('2', 'inches', 'height')], [Dimension('2', 'feet', 'width'), Dimension('8', 'inches', 'width')]),
			'''1'3"x4cm h''': ([Dimension('1', 'feet', None), Dimension('3', 'inches', None)], [Dimension('4', 'centimeters', 'height')]),
			'''1'3" by 4"''': ([Dimension('1', 'feet', None), Dimension('3', 'inches', None)], [Dimension('4', 'inches', None)]),
			'1 by 4': None,
		}

		for value, expected in tests.items():
			dims = pipeline.util.cleaners.dimensions_cleaner(value)
			if expected is not None:
				self.assertIsInstance(dims, tuple)
# 				print('===== got:')
# 				pprint.pprint(dims)
# 				print('----- expected:')
# 				pprint.pprint(expected)
# 				print('=====')
				self.assertEqual(dims, expected, msg=f'dimensions: {value!r}')
			else:
				self.assertIsNone(dims)

if __name__ == '__main__':
	unittest.main()
