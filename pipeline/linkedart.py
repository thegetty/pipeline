from contextlib import suppress
import warnings
import urllib.parse
import calendar

from cromulent import model, vocab
from cromulent.model import factory
from cromulent.extract import extract_physical_dimensions
from pipeline.util.cleaners import ymd_to_datetime

factory.auto_id_type = 'uuid'
vocab.add_art_setter()

def add_crom_data(data: dict, what=None):
	data['_CROM_FACTORY'] = factory
	data['_LOD_OBJECT'] = what
	return data

def get_crom_object(data: dict):
	if data is None:
		return None
	return data.get('_LOD_OBJECT')

def remove_crom_object(data: dict):
	with suppress(KeyError):
		del data['_LOD_OBJECT']
		del data['_CROM_FACTORY']
	return data

class MakeLinkedArtRecord:
	def set_properties(self, data, thing):
		'''
		The following keys in `data` are handled to set properties on `thing`:

		`referred_to_by`
		`identifiers`
		`names` -	An array of arrays of one or two elements. The first element of each
					array is a name string, and is set as the value of a `model.Name` for
					`thing`. If there is a `dict` second element, its contents are used to
					assert properties of the name. An array associated with the key
					`'referred_to_by'` will be used to assert that the `LinguisticObject`s
					(or `dict`s representing a `LinguisticObject`) refer to the name.

		Example data:

		{
			'names': [
				['J. Paul Getty'],
				[
					'Getty',
					{
						'referred_to_by': [
							{'uri': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#K-ROW-1-2-3'},
							model.LinguisticObject(ident='tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#K-ROW-1-7-10'),
						]
					}
				]
			]
		}
		'''
		for notedata in data.get('referred_to_by', []):
			if isinstance(notedata, tuple):
				content, itype = notedata
				if itype is not None:
					if isinstance(itype, type):
						note = itype(content=content)
					elif isinstance(itype, object):
						note = itype
						note.content = content
					else:
						note = vocab.Note(content=content)
						note.classified_as = itype
			elif isinstance(notedata, model.BaseResource):
				note = notedata
			elif isinstance(notedata, str):
				note = vocab.Note(content=notedata)
			else:
				note = notedata
			thing.referred_to_by = note

		for identifier in data.get('identifiers', []):
			if isinstance(identifier, tuple):
				content, itype = identifier
				if itype is not None:
					if isinstance(itype, type):
						ident = itype(ident='', content=content)
						if not content:
							warnings.warn(f'Setting empty identifier on {thing.id}')
					elif isinstance(itype, object):
						ident = itype
						ident.content = content
						if not content:
							warnings.warn(f'Setting empty identifier on {thing.id}')
					else:
						ident = model.Identifier(ident='')
						if not content:
							warnings.warn(f'Setting empty identifier on {thing.id}')
						ident.content = content
						ident.classified_as = itype
			else:
				ident = identifier
# 				c = ident.content
			thing.identified_by = ident

		if not hasattr(thing, '_label') and 'label' in data:
			setattr(thing, '_label', data['label'])

		for namedata in data.get('names', []):
			# namedata should take the form of:
			# ["A. Name"]
			# ["A. Name", {'referred_to_by': [{'uri': 'URI-OF-LINGUISTIC_OBJECT'}, model.LinguisticObject()]}]
			if isinstance(namedata, tuple):
				name, *properties = namedata
			else:
				name = namedata
				properties = {}
			n = set_la_name(thing, name)
			self.set_lo_properties(n, *properties)

	def set_lo_properties(self, n, *properties):
		for props in properties:
			assert isinstance(props, dict)
			for ref in props.get('referred_to_by', []):
				if isinstance(ref, dict):
					if 'uri' in ref:
						l = model.LinguisticObject(ident=ref['uri'])
					elif 'uuid' in data:
						l = model.LinguisticObject(ident="urn:uuid:%s" % ref['uuid'])
					else:
						raise Exception(f'MakeLinkedArtRecord call attempt to set name {name} with a non-identified reference: {ref}')
				elif isinstance(ref, object):
					l = ref
				else:
					raise Exception(f'MakeLinkedArtRecord call attempt to set name {name} with an unrecognized reference type: {ref}')
				n.referred_to_by = l

	def __call__(self, data: dict):
		if '_LOD_OBJECT' in data:
			thing = data['_LOD_OBJECT']
		else:
			otype = data['object_type']
			if 'uri' in data:
				thing = otype(ident=data['uri'])
			elif 'uuid' in data:
				thing = otype(ident="urn:uuid:%s" % data['uuid'])
			else:
				raise Exception('MakeLinkedArtRecord called with a dictionary with neither uuid or uri member')

		self.set_properties(data, thing)

		return add_crom_data(data=data, what=thing)

def set_la_name(thing, value, title_type=None, set_label=False):
	if value is None:
		return None
	if isinstance(value, tuple):
		label, language = value
	else:
		label = value
		language = None
	if set_label:
		thing._label = label
	name = model.Name(ident='', content=label)
	if title_type is not None:
		name.classified_as = title_type
	if not label:
		warnings.warn(f'Setting empty name on {thing.id}')
	thing.identified_by = name
	if language is not None:
		name.language = language
	return name

class MakeLinkedArtLinguisticObject(MakeLinkedArtRecord):
	# TODO: document the expected format of data['translations']
	# TODO: document the expected format of data['identifiers']
	def set_properties(self, data, thing):
		super().set_properties(data, thing)

		# TODO: this whole title_type thing isn't right. most of the identifiers below aren't titles
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
		name = None
		if 'label' in data:
			name = set_la_name(thing, data['label'], title_type, set_label=True)

		for author in data.get('created_by', []):
			thing.created_by = author

		for a in data.get('used_for', []):
			thing.used_for = a

		for a in data.get('about', []):
			thing.about = a

		for c in data.get('classified_as', []):
			thing.classified_as = c

		for t in data.get('translations', []):
			n = set_la_name(thing, t, title_type)
			if name is not None:
				n.translation_of = name

		for content, itype, notes in data.get('qualified_identifiers', []):
			ident = itype(content=content)
			if not content:
				warnings.warn(f'Setting empty identifier on {thing.id}')
			thing.identified_by = ident
			for n in notes:
				ident.referred_to_by = n

		code_type = None # TODO: is there a model.Type value for this sort of code?
		for c in data.get('classifications', []):
			if isinstance(c, model.Type):
				classification = c
			else:
				cid, label = c
				name = model.Name()
				name.classified_as = title_type
				name.content = label

				classification = model.Type(label=label)
				if not label:
					warnings.warn(f'Setting empty name on {classification.id}')
				classification.identified_by = name

				code = model.Identifier()
				code.classified_as = code_type
				if not cid:
					warnings.warn(f'Setting empty identifier on {code.id}')
				code.content = cid
				classification.identified_by = code
			thing.about = classification

		for c in data.get('indexing', []):
			if isinstance(c, tuple):
				cid, label = c
				name = model.Name()
				name.classified_as = title_type
				name.content = label

				indexing = model.Type(label=label)
				if not label:
					warnings.warn(f'Setting empty name on {indexing.id}')
				indexing.identified_by = name

				code = model.Identifier()
				code.classified_as = code_type
				code.content = cid
				if not cid:
					warnings.warn(f'Setting empty identifier on {code.id}')
				indexing.identified_by = code
			else:
				indexing = c
			thing.about = indexing

		parents = data.get('part_of', [])
		for parent_data in parents:
			parent = get_crom_object(parent_data)
			thing.part_of = parent

		children = data.get('part', [])
		for child_data in children:
			child = get_crom_object(child_data)
			thing.part = child

		for carrier in data.get('carried_by', []):
			hmo = get_crom_object(carrier)
			thing.carried_by = hmo

		for dimension in data.get('dimensions', []):
			thing.dimension = dimension

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.LinguisticObject
		return super().__call__(data)


class MakeLinkedArtHumanMadeObject(MakeLinkedArtRecord):
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title') # TODO: is this the right aat URI?
		if 'label' in data:
			set_la_name(thing, data['label'], title_type, set_label=True)

		if 'title' in data:
			# TODO: This needs to be a PrimaryName, not a Name classified as a Title
			title = data['title']
			if isinstance(title, str):
				set_la_name(thing, title, title_type, set_label=True)
			elif isinstance(title, (list, tuple)):
				value, *properties = title
				n = set_la_name(thing, value, title_type, set_label=True)
				n.classified_as = title_type
				self.set_lo_properties(n, *properties)
				thing.identified_by = n

		parents = data.get('part_of', [])
		for parent_data in parents:
			parent = get_crom_object(parent_data)
			thing.part_of = parent

		for carried in data.get('carries', []):
			lo = get_crom_object(carried)
			thing.carries = lo

		for coll in data.get('member_of', []):
			thing.member_of = coll

		for annotation in data.get('annotations', []):
			a = model.Annotation(ident='', content=annotation)
			thing.carries = a


class MakeLinkedArtAbstract(MakeLinkedArtLinguisticObject):
	pass

class MakeLinkedArtAgent(MakeLinkedArtRecord):
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))
			if ulan:
				thing.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{ulan}')

		if 'name' in data:
			title_type = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
			name = data['name']
			if isinstance(name, str):
				set_la_name(thing, name, title_type, set_label=True)
			elif isinstance(name, (list, tuple)):
				value, *properties = name
				n = model.Name(ident='', content=value)
				n.classified_as = title_type
				self.set_lo_properties(n, *properties)
				thing.identified_by = n

		# Locations are names of residence places (P74 -> E53)
		# XXX FIXME: Places are their own model
		if 'places' in data:
			for p in data['places']:
				if isinstance(p, model.Place):
					pl = p
				elif isinstance(p, dict):
					pl = get_crom_object(p)
				else:
					pl = model.Place(ident='', label=p)
				#pl._label = p['label']
				#nm = model.Name()
				#nm.content = p['label']
				#pl.identified_by = nm
				#for s in p['sources']:
				#		l = model.LinguisticObject(ident="urn:uuid:%s" % s[1])
					# l._label = _row_label(s[2], s[3], s[4])
				#	pl.referred_to_by = l
				thing.residence = pl


class MakeLinkedArtOrganization(MakeLinkedArtAgent):
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		with suppress(KeyError):
			thing._label = str(data['label'])

		for event in data.get('events', []):
			thing.carried_out = event

		for n in data.get('nationality', []):
			thing.classified_as = n

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.Group
		return super().__call__(data)

class MakeLinkedArtAuctionHouseOrganization(MakeLinkedArtOrganization):
	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = vocab.AuctionHouseOrg
		return super().__call__(data)


# XXX Reconcile with provenance.timespan_from_outer_bounds
def make_ymd_timespan(data: dict, start_prefix="", end_prefix="", label=""):
	y = f'{start_prefix}year'
	m = f'{start_prefix}month'
	d = f'{start_prefix}day'
	y2 = f'{end_prefix}year'
	m2 = f'{end_prefix}month'
	d2 = f'{end_prefix}day'	

	t = model.TimeSpan(ident='')
	if not label:
		label = ymd_to_label(data[y], data[m], data[d])
		if y != y2:
			lbl2 = ymd_to_label(data[y2], data[m2], data[d2])
			label = f'{label} to {lbl2}'
	t._label = label
	if not label:
		warnings.warn(f'Setting empty name on {t.id}')
	t.identified_by = model.Name(ident='', content=label)
	t.begin_of_the_begin = ymd_to_datetime(data[y], data[m], data[d])
	t.end_of_the_end = ymd_to_datetime(data[y2], data[m2], data[d2], which="end")
	return t

def ymd_to_label(year, month, day):
	# Return monthname day year
	if not year:
		return "Unknown"
	if not month:
		return str(year)
	if not isinstance(month, int):
		try:
			month = int(month)
			month_name = calendar.month_name[month]
		except:
			# Assume it's already a name of a month
			month_name = month
	else:
		month_name = calendar.month_name[month]
	if day:
		return f'{month_name} {day}, {year}'
	else:
		return f'{month_name} {year}'


class MakeLinkedArtPerson(MakeLinkedArtAgent):
	def set_properties(self, data, who):
		super().set_properties(data, who)
		with suppress(KeyError):
			who._label = str(data['label'])

		for ns in ['aat_nationality_1', 'aat_nationality_2','aat_nationality_3']:
			# add nationality
			n = data.get(ns)
			# XXX Strip out antique / modern anonymous as a nationality
			if n:
				if int(n) in [300310546,300264736]:
					break
				natl = vocab.Nationality(ident="http://vocab.getty.edu/aat/%s" % n)
				who.classified_as = natl
				natl._label = str(data[ns+'_label'])
			else:
				break
		for n in data.get('nationality', []):
			if isinstance(n, model.BaseResource):
				who.classified_as = n

		# nationality field can contain other information, but not useful.
		# XXX Intentionally ignored but validate with GRI

		if data.get('active_early') or data.get('active_late'):
			act = vocab.Active()
			ts = model.TimeSpan(ident='')
			if data['active_early']:
				ts.begin_of_the_begin = "%s-01-01:00:00:00Z" % (data['active_early'],)
				ts.end_of_the_begin = "%s-01-01:00:00:00Z" % (data['active_early']+1,)
			if data['active_late']:
				ts.begin_of_the_end = "%s-01-01:00:00:00Z" % (data['active_late'],)
				ts.end_of_the_end = "%s-01-01:00:00:00Z" % (data['active_late']+1,)
			ts._label = "%s-%s" % (data['active_early'], data['active_late'])
			act.timespan = ts
			who.carried_out = act

		for event in data.get('events', []):
			who.carried_out = event

		if data.get('birth'):
			b = model.Birth()
			ts = model.TimeSpan(ident='')
			if 'birth_clean' in data and data['birth_clean']:
				if data['birth_clean'][0]:
					ts.begin_of_the_begin = data['birth_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
				if data['birth_clean'][1]:
					ts.end_of_the_end = data['birth_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
			ts._label = data['birth']
			b.timespan = ts
			b._label = "Birth of %s" % who._label
			who.born = b

		if data.get('death'):
			d = model.Death()
			ts = model.TimeSpan(ident='')
			if 'death_clean' in data and data['death_clean']:
				if data['death_clean'][0]:
					ts.begin_of_the_begin = data['death_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
				if data['death_clean'][1]:
					ts.end_of_the_end = data['death_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
			ts._label = data['death']
			d.timespan = ts
			d._label = "Death of %s" % who._label
			who.died = d

		if 'contact_point' in data:
			for p in data['contact_point']:
				if isinstance(p, model.Identifier):
					pl = p
				elif isinstance(p, dict):
					pl = get_crom_object(p)
				else:
					pl = model.Name(ident='', content=p)
				who.contact_point = pl

		for uri in data.get('exact_match', []):
			who.exact_match = uri

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.Person
		return super().__call__(data)

class MakeLinkedArtPlace(MakeLinkedArtRecord):
	TYPES = {
		'city': vocab.instances['city'],
		'province': vocab.instances['province'],
		'state': vocab.instances['province'],
		'country': vocab.instances['nation'],
	}

	def __init__(self, base_uri=None, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.base_uri = base_uri

	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		type_name = data.get('type', 'place').lower()
		name = data.get('name')
		label = name
		parent_data = data.get('part_of')

		place_type = MakeLinkedArtPlace.TYPES.get(type_name)
		parent = None
		if parent_data:
			parent_data = self(parent_data)
			parent = get_crom_object(parent_data)
			if label:
				try:
					label = f'{label}, {parent._label}'
				except AttributeError:
					print('*** NO LABEL IN PARENT:' + factory.toString(parent, False))

		placeargs = {'label': label}
		if data.get('uri'):
			placeargs['ident'] = data['uri']

		if place_type:
			thing.classified_as = place_type
		if not name:
			warnings.warn(f'Place with missing name on {thing.id}')
		if parent:
			print(f'*** Setting parent on place object: {parent}')
			thing.part_of = parent

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.Place
		if self.base_uri and not data.get('uri'):
			data['uri'] = self.base_uri + urllib.parse.quote(data['name'])
		return super().__call__(data)

def make_la_place(data:dict, base_uri=None):
	'''
	Given a dictionary representing data about a place, construct a model.Place object,
	assign it as the crom data in the dictionary, and return the dictionary.

	The dictionary keys used to construct the place object are:

	- name
	- type (one of: 'City' or 'Country')
	- part_of (a recursive place dictionary)
	'''
	TYPES = {
		'city': vocab.instances['city'],
		'province': vocab.instances['province'],
		'state': vocab.instances['province'],
		'country': vocab.instances['nation'],
	}

	if data is None:
		return None
	type_name = data.get('type', 'place').lower()
	name = data['name']
	label = name
	parent_data = data.get('part_of')

	place_type = TYPES.get(type_name)
	parent = None
	if parent_data:
		parent_data = make_la_place(parent_data, base_uri=base_uri)
		parent = get_crom_object(parent_data)
		label = f'{label}, {parent._label}'

	placeargs = {'label': label}
	if data.get('uri'):
		placeargs['ident'] = data['uri']
	elif base_uri:
		data['uri'] = base_uri + urllib.parse.quote(label)
		placeargs['ident'] = data['uri']

	p = model.Place(**placeargs)
	if place_type:
		p.classified_as = place_type
	if name:
		p.identified_by = model.Name(ident='', content=name)
	else:
		warnings.warn(f'Place with missing name on {p.id}')
	if parent:
		p.part_of = parent
	return add_crom_data(data=data, what=p)

class PopulateObject:
	'''
	Shared functionality for project-specific bonobo node sub-classes to populate
	object records.
	'''
	@staticmethod
	def populate_object_statements(data:dict, default_unit=None):
		hmo = get_crom_object(data)
		sales_record = get_crom_object(data.get('_record'))

		format = data.get('format')
		if format:
			formatstmt = vocab.PhysicalStatement(ident='', content=format)
			if sales_record:
				formatstmt.referred_to_by = sales_record
			hmo.referred_to_by = formatstmt

		materials = data.get('materials')
		if materials:
			matstmt = vocab.MaterialStatement(ident='', content=materials)
			if sales_record:
				matstmt.referred_to_by = sales_record
			hmo.referred_to_by = matstmt

		dimstr = data.get('dimensions')
		if dimstr:
			dimstmt = vocab.DimensionStatement(ident='', content=dimstr)
			if sales_record:
				dimstmt.referred_to_by = sales_record
			hmo.referred_to_by = dimstmt
			for dim in extract_physical_dimensions(dimstr, default_unit=default_unit):
				if sales_record:
					dim.referred_to_by = sales_record
				hmo.dimension = dim
		else:
			pass
	# 		print(f'No dimension data was parsed from the dimension statement: {dimstr}')

