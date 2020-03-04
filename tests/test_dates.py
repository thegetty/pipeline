import unittest
from datetime import datetime
import pipeline.util.cleaners
from pipeline.util import implode_date, label_for_timespan_range

class TestDateCleaners(unittest.TestCase):
	'''
	Test the ability to recognize and parse various formats of dates.
	'''
	def setUp(self):
		pass

	def tearDown(self):
		pass

	def test_date_cleaner(self):
		'''
		Test the documented formats that `pipeline.util.cleaners.date_cleaner` can parse
		and ensure that it returns the expected data.
		'''
		tests = {
			'1801': [datetime(1801,1,1), datetime(1802,1,1)],				# YYYY[?]
			'1802?': [datetime(1802,1,1), datetime(1803,1,1)],				# YYYY[?]
			'1803/02/03': [datetime(1803,2,3), datetime(1803,2,4)],			# YYYY/MM/DD
			'04/02/1804': [datetime(1804,2,4), datetime(1804,2,5)],			# DD/MM/YYYY
			'ca. 1806': [datetime(1801,1,1), datetime(1811,1,1)],			# ca. YYYY
			'aft. 1807': [datetime(1807,1,1), None],						# aft[er|.] YYYY
			'after 1808': [datetime(1808,1,1), None],						# aft[er|.] YYYY
			'bef. 1810': [None, datetime(1810,1,1)],						# bef[ore|.] YYYY
			'before 1811': [None, datetime(1811,1,1)],						# bef[ore|.] YYYY
			'1812.02.05': [datetime(1812,2,5), datetime(1812,2,6)],			# YYYY.MM.DD
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
			date_range = pipeline.util.cleaners.date_cleaner(value)
			self.assertIsInstance(date_range, list)
			if expected is not None:
				self.assertEqual(date_range, expected, msg=f'date string: {value!r}')

	def test_implode_date(self):
		full_data = {'year': '2019', 'month': '04', 'day': '08'}
		self.assertEqual('2019-04-08', implode_date(full_data))
		self.assertEqual('2019-04-08', implode_date(full_data, clamp='begin'))
		self.assertEqual('2019-04-08', implode_date(full_data, clamp='end'))
		self.assertEqual('2019-04-09', implode_date(full_data, clamp='eoe'))

		prefixed_data = {'xxyear': '2019', 'xxmonth': '02'}
		self.assertEqual('2019-02', implode_date(prefixed_data, prefix='xx'))
		self.assertEqual('2019-02-01', implode_date(prefixed_data, prefix='xx', clamp='begin'))
		self.assertEqual('2019-02-28', implode_date(prefixed_data, prefix='xx', clamp='end'))
		self.assertEqual('2019-03-01', implode_date(prefixed_data, prefix='xx', clamp='eoe'))

		month_end_data = {'year': '2019', 'month': '02', 'day': '28'}
		self.assertEqual('2019-02-28', implode_date(month_end_data))
		self.assertEqual('2019-02-28', implode_date(month_end_data, clamp='begin'))
		self.assertEqual('2019-02-28', implode_date(month_end_data, clamp='end'))
		self.assertEqual('2019-03-01', implode_date(month_end_data, clamp='eoe'))

		year_end_data = {'year': '2019', 'month': '12', 'day': '31'}
		self.assertEqual('2019-12-31', implode_date(year_end_data))
		self.assertEqual('2019-12-31', implode_date(year_end_data, clamp='begin'))
		self.assertEqual('2019-12-31', implode_date(year_end_data, clamp='end'))
		self.assertEqual('2020-01-01', implode_date(year_end_data, clamp='eoe'))

		bad_month_data = {'day': '00', 'mo': '00', 'year': '1781'}
		self.assertEqual('1781', implode_date(bad_month_data, prefix=''))
		self.assertEqual('1781-01-01', implode_date(bad_month_data, prefix='', clamp='begin'))
		self.assertEqual('1781-12-31', implode_date(bad_month_data, prefix='', clamp='end'))
		self.assertEqual('1782-01-01', implode_date(bad_month_data, prefix='', clamp='eoe'))

		bad_day_data = {'day': '00', 'mo': '09', 'year': '1781'}
		self.assertEqual('1781-09', implode_date(bad_day_data, prefix=''))
		self.assertEqual('1781-09-01', implode_date(bad_day_data, prefix='', clamp='begin'))
		self.assertEqual('1781-09-30', implode_date(bad_day_data, prefix='', clamp='end'))
		self.assertEqual('1781-10-01', implode_date(bad_day_data, prefix='', clamp='eoe'))

	def test_label_for_timespan_range(self):
		# single year
		self.assertEqual(label_for_timespan_range('2004-01-01', '2004-12-31', inclusive=True), '2004')
		self.assertEqual(label_for_timespan_range('2004-01-01', '2005-01-01', inclusive=False), '2004')

		# single month
		self.assertEqual(label_for_timespan_range('2004-02-01', '2004-02-29', inclusive=True), '2004-02')
		self.assertEqual(label_for_timespan_range('2004-02-01', '2004-03-01', inclusive=False), '2004-02')

		# single day
		self.assertEqual(label_for_timespan_range('2004-02-03', '2004-02-03', inclusive=True), '2004-02-03')
		self.assertEqual(label_for_timespan_range('2004-02-03', '2004-02-04', inclusive=False), '2004-02-03')

		# multi-day
		self.assertEqual(label_for_timespan_range('2004-02-25', '2004-03-02', inclusive=True), '2004-02-25 to 2004-03-02')
		self.assertEqual(label_for_timespan_range('2004-02-25', '2004-03-02', inclusive=False), '2004-02-25 to 2004-03-01')

		# truncated dates, lexical year
		self.assertEqual(label_for_timespan_range('2004', '2005', inclusive=True), '2004 to 2005')
		with self.assertRaises(Exception):
			label_for_timespan_range('2004', '2005', inclusive=False)

		# truncated dates, lexical year-month
		self.assertEqual(label_for_timespan_range('2004-03', '2004-04', inclusive=True), '2004-03 to 2004-04')
		with self.assertRaises(Exception):
			label_for_timespan_range('2004-03', '2004-04', inclusive=False)

if __name__ == '__main__':
	unittest.main()
