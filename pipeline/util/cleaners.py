import pprint
import locale
import re
import calendar
from contextlib import contextmanager, suppress
from datetime import datetime, timedelta
from pipeline.util import Dimension
import urllib.parse

CIRCA = 5 # years
CIRCA_D = timedelta(days=365*CIRCA)

share_re = re.compile("([0-9]+)/([0-9]+)")

number_pattern = r'((?:\d+\s+\d+/\d+)|(?:\d+(?:[.,]\d+)?))'
unit_pattern = r'''('|"|d[.]?|duymen|pouces?|inches|inch|in[.]?|pieds?|v[.]?|voeten|feet|foot|ft[.]?|cm)'''
dimension_pattern = f'({number_pattern}\\s*(?:{unit_pattern})?)'
dimension_re = re.compile(f'\\s*{dimension_pattern}')

simple_width_height_pattern = r'(?:\s*((?<!\w)[wh]|width|height))?'
simple_dimensions_pattern_x1 = ''\
	f'(?P<d1>(?:{dimension_pattern}\\s*)+)'\
	f'(?P<d1w>{simple_width_height_pattern})'
simple_dimensions_re_x1 = re.compile(simple_dimensions_pattern_x1)
simple_dimensions_pattern_x2 = ''\
	f'(?P<d1>(?:{dimension_pattern}\\s*)+)'\
	f'(?P<d1w>{simple_width_height_pattern})'\
	r'(?:,)?\s*(x|by)'\
	f'(?P<d2>(?:\\s*{dimension_pattern})+)'\
	f'(?P<d2w>{simple_width_height_pattern})'
simple_dimensions_re_x2 = re.compile(simple_dimensions_pattern_x2)

# Haut 14 pouces, large 10 pouces
french_dimensions_pattern = f'[Hh]aut(?:eur)? (?P<d1>(?:{dimension_pattern}\\s*)+), [Ll]arge(?:ur)? (?P<d2>(?:{dimension_pattern}\\s*)+)'
french_dimensions_re = re.compile(french_dimensions_pattern)

# Hoog. 1 v. 6 d., Breed 2 v. 3 d.
dutch_dimensions_pattern = f'(?P<d1w>[Hh]oogh?[.]?|[Bb]reedt?) (?P<d1>(?:{dimension_pattern}\\s*)+), (?P<d2w>[Hh]oogh?[.]?|[Bb]reedt?) (?P<d2>(?:{dimension_pattern}\\s*)+)'
dutch_dimensions_re = re.compile(dutch_dimensions_pattern)

def _canonical_value(value):
	value = value.replace(',', '.')
	value = value.replace(' 1/4', '.25')
	value = value.replace(' 1/2', '.5')
	value = value.replace(' 3/4', '.75')
	if '/' in value:
		return None
	if value.startswith('.'):
		value = f'0{value}'
	return value

def _canonical_unit(value):
	if value is None:
		return None
	value = value.lower()
	if 'in' in value or value in ('pouces', 'pouce', 'duymen', 'd.', 'd') or value == '"':
		return 'inches'
	elif 'ft' in value or value in ('pieds', 'pied', 'feet', 'foot', 'voeten', 'v.', 'v') or value == "'":
		return 'feet'
	elif 'cm' in value:
		return 'cm'
	return None

def _canonical_which(value):
	if not value:
		return None
	value = value.strip().lower()
	if value.startswith('w'):
		return 'width'
	elif value.startswith('h'):
		return 'height'
	print(f'*** Unknown which dimension: {value}')
	return None

def parse_simple_dimensions(value, which=None):
	'''
	Parse the supplied string for dimensions (value + unit), and return a list of
	`Dimension`s, optionally setting the `which` property to the supplied value.

	Examples:

	1 cm
	2ft
	5 pieds
	'''
	if value is None:
		return None
	value = value.strip()
	dims = []
# 	print(f'DIMENSION: {value}')
	for m in re.finditer(dimension_re, value):
		# print(f'--> match {m}')
		v = _canonical_value(m.group(2))
		if not v:
			print(f'*** failed to canonicalize dimension value: {m.group(2)}')
			return None
		unit_value = m.group(3)
		u = _canonical_unit(unit_value)
		if unit_value and not u:
			print(f'*** not a recognized unit: {unit_value}')
		which = _canonical_which(which)
		d = Dimension(value=v, unit=u, which=which)
		dims.append(d)
	if not dims:
		return None
	return dims

def normalized_dimension_object(dimensions):
	'''
	Normalizes the given `dimensions`, or returns `None` is normalization fails.

	Returns a tuple of the normalized data, and a label which preserves the original
	set of dimensions.

	For example, the input:

		[
			Dimension(value='10', unit='feet', which=None),
			Dimension(value='3', unit='inches', which=None),
		]

	results in the output:

		(
			Dimension(value='123.0', unit='inches', which=None),
			"10 feet, 3 inches"
		)
	'''
	nd = normalize_dimension(dimensions)
	if not nd:
		return None
	labels = []
	for d in dimensions:
		if d.unit == 'inches':
			labels.append(f'{d.value} inches')
		elif d.unit == 'feet':
			labels.append(f'{d.value} feet')
		elif d.unit == 'cm':
			labels.append(f'{d.value} cm')
		elif d.unit is None:
			labels.append(f'{d.value}')
		else:
			print(f'*** unrecognized unit: {d.unit}')
			return None
	label = ', '.join(labels)
	return nd, label

def normalize_dimension(dimensions):
	'''
	Given a list of `Dimension`s, normalize them into a single Dimension (e.g. values in
	both feet and inches become a single dimension of inches).

	If the values cannot be sensibly combined (e.g. inches + centimeters), returns `None`.
	'''
	unknown = 0
	inches = 0
	cm = 0
	which = None
	for d in dimensions:
		which = d.which
		if d.unit == 'inches':
			inches += float(d.value)
		elif d.unit == 'feet':
			inches += 12 * float(d.value)
		elif d.unit == 'cm':
			cm += float(d.value)
		elif d.unit is None:
			unknown += float(d.value)
		else:
			print(f'*** unrecognized unit: {d.unit}')
			return None
	used_systems = 0
	for v in (inches, cm, unknown):
		if v:
			used_systems += 1
	if used_systems != 1:
		print(f'*** dimension used a mix of unit systems (metric, imperial, and/or unknown): {dimensions}')
		return None
	elif inches:
		return Dimension(value=str(inches), unit='inches', which=which)
	elif cm:
		return Dimension(value=str(cm), unit='cm', which=which)
	else:
		return Dimension(value=str(cm), unit=None, which=which)

def dimensions_cleaner(value):
	'''
	Attempt to parse a set of dimensions from the given string.

	Returns a tuple of `pipeline.util.Dimension` objects if parsing succeeds,
	None otherwise.
	'''
	if value is None:
		return None
	cleaners = [
		simple_dimensions_cleaner_x2,
		french_dimensions_cleaner_x2,
		dutch_dimensions_cleaner_x2,
		simple_dimensions_cleaner_x1
	]
	for f in cleaners:
		d = f(value)
		if d:
			return d
	return None

def french_dimensions_cleaner_x2(value):
	'''Attempt to parse 2 dimensions from a French-formatted string.'''
	# Haut 14 pouces, large 10 pouces

	m = french_dimensions_re.match(value)
	if m:
		d = m.groupdict()
		d1 = parse_simple_dimensions(d['d1'], 'h')
		d2 = parse_simple_dimensions(d['d2'], 'w')
		if d1 and d2:
			return (d1, d2)
		else:
			print(f'd1: {d1} {d["d1"]} h')
			print(f'd2: {d2} {d["d2"]} w')
			print(f'*** Failed to parse dimensions: {value}')
	return None

def dutch_dimensions_cleaner_x2(value):
	'''Attempt to parse 2 dimensions from a Dutch-formatted string.'''
	# Hoog. 1 v. 6 d., Breed 2 v. 3 d.
	# Breedt 6 v., hoog 3 v

	m = dutch_dimensions_re.match(value)
	if m:
		d = m.groupdict()
		h = 'h'
		w = 'w'
		if 'breed' in d['d1w'].lower():
			h, w = w, h

		d1 = parse_simple_dimensions(d['d1'], h)
		d2 = parse_simple_dimensions(d['d2'], w)
		if d1 and d2:
			return (d1, d2)
		else:
			print(f'd1: {d1} {d["d1"]} h')
			print(f'd2: {d2} {d["d2"]} w')
			print(f'*** Failed to parse dimensions: {value}')
	return None

def simple_dimensions_cleaner_x1(value):
	'''Attempt to parse 1 dimension from a string.'''
	# 1 cm
	# 1' 2"
	# 1 ft. 2 in. h

	m = simple_dimensions_re_x1.match(value)
	if m:
		d = m.groupdict()
		d1 = parse_simple_dimensions(d['d1'], d['d1w'])
		if d1:
			return (d1,)
	return None

def simple_dimensions_cleaner_x2(value):
	'''Attempt to parse 2 dimensions from a string.'''
	# 1 cm x 2 in
	# 1' 2" by 3 cm
	# 1 ft. 2 in. h by 3 cm w

	m = simple_dimensions_re_x2.match(value)
	if m:
		d = m.groupdict()
		d1 = parse_simple_dimensions(d['d1'], d['d1w'])
		d2 = parse_simple_dimensions(d['d2'], d['d2w'])
		if d1 and d2:
			return (d1, d2)
		else:
			print(f'd1: {d1} {d["d1"]} {d["d1w"]}')
			print(f'd2: {d2} {d["d2"]} {d["d2w"]}')
			print(f'*** Failed to parse dimensions: {value}')
	return None

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
	'Espagne': 'Spain',
	'Espana': 'Spain',
	'España': 'Spain',
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
	'USA': 'United States of America',
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

def parse_location_name(value, uri_base=None):
	'''
	Parses a string like 'Los Angeles, CA, USA' or 'Genève, Schweiz'
	and returns a structure that can be passed to `pipeline.linkedart.make_la_place`, or
	`None` if the string cannot be parsed.
	'''
	if uri_base is None:
		uri_base = 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID#'
	current = None
	parts = value.split(', ')
	country_name = parts[-1]
	if country_name in _COUNTRIES:
		country_name = _COUNTRY_NAMES.get(country_name, country_name)
	else:
		print(f'*** Expecting country name, but found unexpected value: {country_name!r}')
		# not a recognized place name format; assert a generic Place with the associated value as a name
		return {'name': value}

	# TODO: figure out how to use consistent URIs for countries, or uniquely identifying pairs (city, state, 'US')
	if len(parts) == 2:
		city_name, country_name = parts
		city = {
			'type': 'City',
			'name': city_name,
			'part_of': {
				'type': 'Country',
				'name': country_name,
				'uri': f'{uri_base}PLACE-COUNTRY-' + urllib.parse.quote(country_name),
			}
		}
		current = city
	elif len(parts) == 3 and parts[-1] in ('USA', 'US'):
		city_name, state_name, _ = parts
		country_name = 'United States of America'
		state_type = 'State'
		state_uri = None
		city_uri = None
		if len(state_name) == 2:
			try:
				state_name = _US_STATES[state_name]
				state_uri = f'{uri_base}PLACE-COUNTRY-' + urllib.parse.quote(country_name) + '-STATE-' + urllib.parse.quote(state_name)
				city_uri = state_uri + '-CITY-' + urllib.parse.quote(city_name)
			except:
				# Not a recognized state, so fall back to just a general place
				state_type = 'Place'
		city = {
			'type': 'City',
			'name': city_name,
			'uri': city_uri,
			'part_of': {
				'type': state_type,
				'name': state_name,
				'uri': state_uri,
				'part_of': {
					'type': 'Country',
					'name': country_name,
					'uri': f'{uri_base}PLACE-COUNTRY-' + urllib.parse.quote(country_name),
				}
			}
		}
		current = city
	elif len(parts) == 3 and parts[-1] == 'UK':
		country_name = 'United Kingdom'
		if len(parts) == 3 and parts[-2] == 'England':
			place_name = parts[0]
			place = {
				# The first component of the triple isn't always a city in UK data
				# (e.g. "Burton Constable, England, UK" or "Castle Howard, England, UK")
				# so do not assert a type for this level of the place hierarchy.
				'name': place_name,
				'part_of': {
					'type': 'Country',
					'name': country_name,
					'uri': f'{uri_base}PLACE-COUNTRY-' + urllib.parse.quote(country_name),
				}
			}
			current = place
	else:
		current = {
			'type': 'Specific Place',
			'name': value
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
			print("DATE CLEAN: year is %r; returning None" % year)
			return None

	if not isinstance(month, int):
		try:
			month = int(month)
		except:
			print("DATE CLEAN: month is %r; continuing with %s" % (month, "earliest" if which=="begin" else "latest"))
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
		return value

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
			print("Bad YYYYs date: %s" % value)
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
				print("bad circa: %s" % value)
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
			print("Bad aft value: %s" % value)
			return None
		return [datetime(y,1,1), None]

	elif value.startswith('bef'):
		value = value.replace('bef.', '')
		value = value.replace('before ', '')
		value = value.strip()
		y = int(value)
		return [None, datetime(y,1,1)]

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

		print("fell through to: %s" % value)
		return value


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
