import warnings
import locale
import re
import calendar
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta
# from pipeline.util import Dimension
import urllib.parse

CIRCA = 5 # years
CIRCA_D = timedelta(days=365*CIRCA)

share_re = re.compile("([0-9]+)/([0-9]+)")

_COUNTRY_NAMES = {
	'Algeria': 'Algeria',
	'Argentina': 'Argentina',
	'Armenia': 'Armenia',
	'Australia': 'Australia',
	'Austria': 'Austria',
	'Oesterreich': 'Austria',
	'Österreich': 'Austria',
	'Belgium': 'Belgium',
	'België': 'Belgium',
	'Belgique': 'Belgium',
	'Brazil': 'Brazil',
	'Brasil': 'Brazil',
	'Canada': 'Canada',
	'Czech Republic': 'Czech Republic',
	'Ceska Republika': 'Czech Republic',
	'Ceská Republika': 'Czech Republic',
	'Céska republika': 'Czech Republic',
	'Céska Republika': 'Czech Republic',
	'Cuba': 'Cuba',
	'Denmark': 'Denmark',
	'Danmark': 'Denmark',
	'Germany': 'Germany',
	'Deutschalnd': 'Germany',
	'Deutschland': 'Germany',
	'Duetschland': 'Germany',
	'Ireland': 'Ireland',
	'Eire': 'Ireland',
	'England': 'England',
	'Spain': 'Spain',
	'España': 'Spain',
	'Espana': 'Spain',
	'Espagne': 'Spain',
	'France': 'France',
	'Great Britain': 'Great Britain',
	'Hungary': 'Hungary',
	'Magyarorszag': 'Hungary',
	'Magyarország': 'Hungary',
	'India': 'India',
	'Israel': 'Israel',
	'Italy': 'Italy',
	'Italia': 'Italy',
	'Japan': 'Japan',
	'Latvija': 'Latvia',
	'Liechtenstein': 'Liechtenstein',
	'Luxembourg': 'Luxembourg',
	'México': 'Mexico',
	'Mexico': 'Mexico',
	'Netherlands': 'Netherlands',
	'Nederland': 'Netherlands',
	'New Zealand': 'New Zealand',
	'Norway': 'Norway',
	'Norge': 'Norway',
	'Poland': 'Poland',
	'Polska': 'Poland',
	'Portugal': 'Portugal',
	'Romania': 'Romania',
	'Russia': 'Russia',
	'Rossiya': 'Russia',
	'Switzerland': 'Switzerland',
	'Schweiz': 'Switzerland',
	'Suisse': 'Switzerland',
	'Scotland': 'Scotland',
	'Slovakia': 'Slovakia',
	'South Africa': 'South Africa',
	'Finland': 'Finland',
	'Suomen': 'Finland',
	'Sweden': 'Sweden',
	'Sverige': 'Sweden',
	'United Kingdom': 'United Kingdom',
	'UK': 'United Kingdom',
	'Ukraine': 'Ukraine',
	'Ukraïna': 'Ukraine',
	'United States of America': 'USA',
	'USA': 'USA',
	'US': 'USA',
	'Wales': 'Wales',
}

# These are the current countries found in the PIR data
_COUNTRIES = set(_COUNTRY_NAMES.keys())

_US_STATES = {
	'AK': 'Alaska',
	'AL': 'Alabama',
	'AR': 'Arkansas',
	'AS': 'American Samoa',
	'AZ': 'Arizona',
	'CA': 'California',
	'CO': 'Colorado',
	'CT': 'Connecticut',
	'DC': 'District of Columbia',
	'D.C.': 'District of Columbia', # Some of the data uses syntax with dots for DC
	'DE': 'Delaware',
	'FL': 'Florida',
	'GA': 'Georgia',
	'GU': 'Guam',
	'HI': 'Hawaii',
	'IA': 'Iowa',
	'ID': 'Idaho',
	'IL': 'Illinois',
	'IN': 'Indiana',
	'KS': 'Kansas',
	'KY': 'Kentucky',
	'LA': 'Louisiana',
	'MA': 'Massachusetts',
	'MD': 'Maryland',
	'ME': 'Maine',
	'MI': 'Michigan',
	'MN': 'Minnesota',
	'MO': 'Missouri',
	'MP': 'Northern Mariana Islands',
	'MS': 'Mississippi',
	'MT': 'Montana',
	'NA': 'National',
	'NC': 'North Carolina',
	'ND': 'North Dakota',
	'NE': 'Nebraska',
	'NH': 'New Hampshire',
	'NJ': 'New Jersey',
	'NM': 'New Mexico',
	'NV': 'Nevada',
	'NY': 'New York',
	'OH': 'Ohio',
	'OK': 'Oklahoma',
	'OR': 'Oregon',
	'PA': 'Pennsylvania',
	'PR': 'Puerto Rico',
	'RI': 'Rhode Island',
	'SC': 'South Carolina',
	'SD': 'South Dakota',
	'TN': 'Tennessee',
	'TX': 'Texas',
	'UT': 'Utah',
	'VA': 'Virginia',
	'VI': 'Virgin Islands',
	'VT': 'Vermont',
	'WA': 'Washington',
	'WI': 'Wisconsin',
	'WV': 'West Virginia',
	'WY': 'Wyoming',
}

def _parse_us_location(parts, *, uri_base):
	try:
		city_name, state_name, country_name = parts
	except ValueError:
		return None
	state_type = None
	city_type = None
	state_uri = None
	city_uri = None
	if state_name in _US_STATES or state_name in _US_STATES.values():
		state_name = _US_STATES.get(state_name, state_name)
		state_uri = f'{uri_base}PLACE,COUNTRY-' + urllib.parse.quote(country_name) + ',' + urllib.parse.quote(state_name)
		city_uri = state_uri + ',' + urllib.parse.quote(city_name)
		state_type = 'State'
		city_type = 'City'
	else:
		# Not a recognized state, so fall back to just a general place
		state_type = 'Place'
		city_type = 'Place'

	country = {
		'type': 'Country',
		'name': country_name,
		'uri': f'{uri_base}PLACE,COUNTRY-' + urllib.parse.quote(country_name),
	}

	state = {
		'type': state_type,
		'name': state_name,
		'uri': state_uri,
		'part_of': country
	}

	city = {
		'type': city_type,
		'name': city_name,
		'uri': city_uri,
		'part_of': state,
	}

	for current in (city, state, country):
		for p in ('part_of', 'uri'):
			if p in current and not current[p]:
				del current[p]

	return city

def _parse_uk_location(parts, *, uri_base):
	country_name = 'United Kingdom'
	if len(parts) == 3 and parts[-2] == 'England':
		place_name = parts[0]
		return {
			# The first component of the triple isn't always a city in UK data
			# (e.g. "Burton Constable, England, UK" or "Castle Howard, England, UK")
			# so do not assert a type for this level of the place hierarchy.
			'name': place_name,
			'part_of': {
				'type': 'Country',
				'name': country_name,
				'uri': f'{uri_base}PLACE,COUNTRY-' + urllib.parse.quote(country_name),
			}
		}
	return None

_COUNTRY_HANDLERS = {
	'USA': _parse_us_location,
	'United Kingdom': _parse_uk_location,
}

def parse_location_name(value, uri_base=None):
	'''
	Parses a string like 'Los Angeles, CA, USA' or 'Genève, Schweiz'
	and returns a structure that can be passed to `pipeline.linkedart.make_la_place`, or
	`None` if the string cannot be parsed.
	'''
	parts = value.split(', ')
	return parse_location(*parts, uri_base=uri_base)

def parse_location(*parts, uri_base=None, types=None):
	'''
	Takes a list of hierarchical place names, and returns a structure that can be passed
	to `pipeline.linkedart.make_la_place`.

	If the iterable `types` is given, it supplies the type names of the associated names
	(e.g. `('City', 'Country')`). Otherwise, heuristics are used to guide the parsing.
	'''
	value = ', '.join(parts)
	if uri_base is None:
		uri_base = 'tag:getty.edu,2019:digital:REPLACE-WITH-UUID:pipeline#'

	if types:
		current = None
		uri_parts = []
		for t, name in zip(reversed(types), reversed(parts)):
			uri = None
			if t.upper() in ('COUNTRY', 'STATE', 'PROVINCE'):
				uri_parts.append(t.upper())
				uri_parts.append(urllib.parse.quote(name))
				uri = f'{uri_base}PLACE,' + '-'.join(uri_parts)
			current = {
				'type': t,
				'name': name,
				'uri': uri,
				'part_of': current
			}
			for p in ('part_of', 'uri'):
				if not current[p]:
					del current[p]
		return current

	current = None
	country_name = re.sub(r'[.].*$', '', parts[-1])
	country_type = None
	if country_name in _COUNTRIES:
		country_type = 'Country'
		country_name = _COUNTRY_NAMES.get(country_name, country_name)
	elif country_name in _COUNTRY_NAMES.values():
		# to avoid creationing country mappings like { 'Japan' : 'Japan' } if the above search fails,
		# before giving up try to find the value within the values of the mapping object,
		# and if it exists, use the country_name variable value as is
		country_type = 'Country'
	else:
		warnings.warn(f'*** Expecting country name, but found unexpected value: {country_name!r}')
		# not a recognized place name format; assert a generic Place with the associated value as a name
		return {'name': value}

# 	if country_name in _COUNTRY_HANDLERS:
# 		_parts = list(parts[:-1]) + [country_name]
# 		loc = _COUNTRY_HANDLERS[country_name](_parts, uri_base=uri_base)
# 		if loc:
# 			return loc

	current = {
		'type': country_type,
		'name': country_name,
		'uri': f'{uri_base}PLACE,COUNTRY-' + urllib.parse.quote(country_name),
	}

	if len(parts) == 2:
		city_name = parts[0]
		current = {
			'type': 'City',
			'name': city_name,
			'part_of': current
		}
	else:
		for v in reversed(parts[:-1]):
			current = {
				'type': 'Place',
				'name': v,
				'part_of': current
			}

	return current

def share_parse(value):
	if value is None:
		return None
	else:
		m = share_re.match(value)
		if m:
			(t,b) = m.groups()
			return float(t) / float(b)
		else:
			print("Could not parse raw share: %s" % value)
			return None


def ymd_to_datetime(year, month, day, which="begin"):
	if not isinstance(year, int):
		try:
			year = int(year)
		except:
			# print("DATE CLEAN: year is %r; returning None" % year)
			return None

	if not isinstance(month, int):
		try:
			month = int(month)
		except:
			# print("DATE CLEAN: month is %r; continuing with %s" % (month, "earliest" if which=="begin" else "latest"))
			month = None

	if not isinstance(day, int):
		try:
			day = int(day)
		except:
			day = None

	if not month or month > 12 or month < 1:
		if which == "begin":
			month = 1
		else:
			month = 12

	maxday = calendar.monthrange(year, month)[1]
	if not day or day > maxday or day < 1:
		if which == "begin":
			day = 1
		else:
			# number of days in month
			day = maxday

	ystr = "%04d" % abs(year)
	if year < 0:
		ystr = "-" + ystr

	if which == "begin":
		return "%s-%02d-%02dT00:00:00" % (ystr, month, day)
	else:
		return "%s-%02d-%02dT23:59:59" % (ystr, month, day)



def date_parse(value, delim):
	# parse a / or - or . date or range

	bits = value.split(delim)
	if len(bits) == 2:
		# YYYY/ range
		b1 = bits[0].strip()
		b2 = bits[1].strip()
		if len(b2) < 3 :
			b2 = "%s%s" % (b1[:len(b1)-len(b2)], b2)
		elif len(b2) > 4:
			print("Bad range: %s" % value)
			return None
		try:
			return [datetime(int(b1),1,1), datetime(int(b2)+1,1,1)]
		except:
			print("Broken delim: %s" % value)
			return None
	elif len(bits) == 3:
		# YYYY/MM/DD or YY/YY/YYYY or DD.MM.YYYY or YYYY.MM.DD
		m = int(bits[1])
		if len(bits[0]) == 4:
			y = int(bits[0])
			d = int(bits[2])
		else:
			y = int(bits[2])
			d = int(bits[0])
		if m == 0:
			m = 1
		if d == 0:
			d = 1
		if m > 12:
			# swap them
			d, m = m, d
		try:
			yearmonthday = datetime(y,m,d)
			return [yearmonthday, yearmonthday+timedelta(days=1)]
		except:
			print("Bad // value: %s" % value)
	else:
		print("broken / date: %s" % value)
	return None



def date_cleaner(value):

	# FORMATS:

	# CCth
	# YYYY[?]
	# YYYY/MM/DD
	# DD/MM/YYYY
	# ca. YYYY
	# aft[er|.] YYYY
	# bef[ore|.] YYYY
	# YYYY.MM.DD
	# YYYY/(Y|YY|YYYY)
	# YYYY-YY
	# YYY0s
	# YYYY-
	# YYYY Mon
	# YYYY Month DD

	if value:
		value = value.replace("?",'')
		value = value.replace('est', '')
		value = value.replace("()", '')
		value = value.replace(' or ', '/')
		value = value.strip()
		value = value.replace('by ', 'bef.')
		value = value.replace('c.', 'ca.')
		value = value.replace('CA.', 'ca.')
		value = value.replace('af.', 'aft.')

	if not value:
		return None

	elif value.startswith("|"):
		# Broken? null it out
		return None

	elif len(value) == 4 and value.isdigit():
		# year only
		return [datetime(int(value),1,1), datetime(int(value)+1,1,1)]

	elif value.startswith('v.'):
		value = value[2:].strip()
		return None

	elif value.endswith('s'):
		# 1950s
		if len(value) == 5 and value[:4].isdigit():
			y = int(value[:4])
			return [datetime(y,1,1), datetime(y+10,1,1)]
		else:
			warnings.warn("Bad YYYYs date: %s" % value)
			return None

	elif len(value) == 5 and value[:4].isdigit() and value.endswith('-'):
		y = int(value[:4])
		return [datetime(y,1,1), None]

	elif value.startswith("ca"):
		# circa x
		value = value[3:].strip()
		if len(value) == 4 and value.isdigit():
			y = int(value)
			return [datetime(y-CIRCA,1,1), datetime(y+CIRCA,1,1)]
		else:
			# Try and parse it
			if value.find('/') > -1:
				val = date_parse(value, '/')
			elif value.find('-') > -1:
				val = date_parse(value, '-')
			else:
				val = None

			if not val:
				warnings.warn("bad circa: %s" % value)
				return None

			val[0] -= CIRCA_D
			val[1] += CIRCA_D
			return val

	elif value.startswith('aft'):
		# after x
		value = value.replace('aft.', '')
		value = value.replace('after ', '')
		value = value.strip()
		try:
			y = int(value)
		except:
			warnings.warn("Bad aft value: %s" % value)
			return None
		return [datetime(y,1,1), datetime(y+CIRCA+1,1,1)] # GRI guideline says that 'after 1900' really means (1900 or later)

	elif value.startswith('bef'):
		value = value.replace('bef.', '')
		value = value.replace('before ', '')
		value = value.strip()
		y = int(value)
		return [datetime(y-CIRCA,1,1), datetime(y+1,1,1)] # GRI guideline says that 'before 1900' really means (up to and including 1900)

	elif len(value) <= 4 and (value.endswith('st') or value.endswith('nd') or value.endswith('rd') or value.endswith('th')):
		century = value[:len(value)-2]
		try:
			c = int(century)
		except:
			warnings.warn("Bad century value: %s" % century)
			print(f'{value!r}')
			return None
		year = (c-1) * 100
		start, end = year, year + 100
		if start == 0:
			start = 1
		return [datetime(start,1,1), datetime(end,1,1)]

	elif value.find('/') > -1:
		# year/year or year/month/date
		# 1885/90
		# 07/02/1897
		return date_parse(value, '/')

	elif value.find('.') > -1:
		return date_parse(value, '.')

	elif value.find('-') > -1:
		return date_parse(value, '-')

	elif value.find(';') > -1:
		return date_parse(value, ';')

	else:
		with c_locale(), suppress(ValueError):
			yearmonthday = datetime.strptime(value, '%Y %B %d')
			if yearmonthday:
				return [yearmonthday, yearmonthday+timedelta(days=1)]

		with c_locale(), suppress(ValueError):
			yearmonth = datetime.strptime(value, '%Y %b')
			if yearmonth:
				year = yearmonth.year
				month = yearmonth.month
				maxday = calendar.monthrange(year, month)[1]
				d = datetime(year, month, 1)
				r = [d, d+timedelta(days=maxday)]
				return r

		warnings.warn(f'fell through to: {value!r}')
		return None


@contextmanager
def c_locale():
	l = locale.getlocale()
	locale.setlocale(locale.LC_ALL, 'C')
	try:
		yield
	finally:
		locale.setlocale(locale.LC_ALL, l)

def test_date_cleaner():
	import sqlite3
	c = sqlite3.connect('/Users/rsanderson/Development/getty/provenance/matt/gpi.sqlite')
	res = c.execute("SELECT DISTINCT person_birth_date from gpi_people")
	x = 0
	for d in res:
		date_cleaner(d[0])
		x += 1
	res = c.execute("SELECT DISTINCT person_death_date from gpi_people")
	for d in res:
		date_cleaner(d[0])
		x += 1

	print("Tried %s dates" % x)

def test_share_parser():
	import sqlite3
	c = sqlite3.connect('/Users/rsanderson/Development/getty/pipeline/data/raw_gpi.sqlite')
	res = c.execute("SELECT DISTINCT joint_own_sh_1 FROM raw_knoedler")
	x = 0
	for s in res:
		x += 1
		# print(share_parse(s[0]))
	res = c.execute("SELECT DISTINCT joint_own_sh_2 FROM raw_knoedler")
	for s in res:
		x += 1
		# print(share_parse(s[0]))
	print("Tried %s shares" % x)

if __name__ == "__main__":
	# test_date_cleaner()
	test_share_parser()
