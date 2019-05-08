import unittest 
import sys
import os
import json
import pprint
from datetime import datetime
import extracters.cleaners

class TestDateCleaners(unittest.TestCase):
	'''
	Test the ability to recognize and parse various formats of dates.
	'''
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_date_cleaner(self):
		tests = {
			'1801': [datetime(1801,1,1), datetime(1802,1,1)],				# YYYY[?]
			'1802?': [datetime(1802,1,1), datetime(1803,1,1)],				# YYYY[?]
			'1803/02/03': [datetime(1803,2,3), datetime(1803,2,4)],			# YYYY/MM/DD # TODO: currently failing, but the upper range should be the next day
			'04/02/1804': [datetime(1804,2,4), datetime(1804,2,5)],			# DD/MM/YYYY # TODO: currently failing, but the upper range should be the next day
			'ca. 1806': [datetime(1801,1,1), datetime(1811,1,1)],			# ca. YYYY
			'aft. 1807': [datetime(1807,1,1), None],						# aft[er|.] YYYY
			'after 1808': [datetime(1808,1,1), None],						# aft[er|.] YYYY
			'bef. 1810': [None, datetime(1810,1,1)],						# bef[ore|.] YYYY
			'before 1811': [None, datetime(1811,1,1)],						# bef[ore|.] YYYY
			'1812.02.05': [datetime(1812,2,5), datetime(1812,2,6)],			# YYYY.MM.DD # TODO: currently failing, but the upper range should be the next day
			'1813/4': [datetime(1813,1,1), datetime(1815,1,1)],				# YYYY/(Y|YY|YYYY)
			'1814/21': [datetime(1814,1,1), datetime(1822,1,1)],			# YYYY/(Y|YY|YYYY)
			'1815/1901': [datetime(1815,1,1), datetime(1902,1,1)],			# YYYY/(Y|YY|YYYY)
			'1816-19': [datetime(1816,1,1), datetime(1820,1,1)],			# YYYY-YY
			'1830s': [datetime(1830,1,1), datetime(1840,1,1)],				# YYY0s
			'1841-': [datetime(1841,1,1), None],							# YYYY-
			'1850 Mar': [datetime(1850,3,1), datetime(1850,4,1)],			# YYYY Mon
			'1851 April 02': [datetime(1851,4,2), datetime(1851,4,3)],		# YYYY Month DD
		}
		
		for value, expected in tests.items():
			date_range = extracters.cleaners.date_cleaner(value)
			self.assertIsInstance(date_range, list)
			if expected is not None:
				self.assertEqual(date_range, expected, msg=f'date string: {value!r}')
