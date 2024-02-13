from contextlib import suppress
import warnings
import urllib.parse
import calendar
import json

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

def get_crom_objects(data: list):
	r = list()
	for d in data:
		r.append(d.get('_LOD_OBJECT'))
	return r


def remove_crom_object(data: dict):
	with suppress(KeyError):
		del data['_LOD_OBJECT']
		del data['_CROM_FACTORY']
	return data

class MakeLinkedArtRecord:
	def set_referred_to_by(self, data, thing):
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

	def set_properties(self, data, thing):
		'''
		The following keys in `data` are handled to set properties on `thing`:

		`referred_to_by`
		`identifiers`
		`names` -	An array of arrays of one or two elements. The first element of each
					array is a name string, and is set as the value of a `model.Name` for
					`thing`. If there is a `dict` second element, its contents are used to
					assert properties of the name:
					
					- An array associated with the key `'referred_to_by'` will be used to
					  assert that the `LinguisticObject`s (or `dict`s representing a
					  `LinguisticObject`) refer to the name.
					- A value associated with the key `'classified_as'` (either a
					  `model.Type` or a cromulent vocab class) will be asserted as the
					  classification of the `model.Name`.

		Example data:

		{
			'names': [
				['J. Paul Getty'],
				[
					'Getty',
					{
						'classified_as': model.Type(ident='http://vocab.getty.edu/aat/300404670', label='Primary Name'),
		# or:			'classified_as': vocab.PrimaryName,
						'referred_to_by': [
							{'uri': 'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#K-ROW-1-2-3'},
							model.LinguisticObject(ident='tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#K-ROW-1-7-10'),
						]
					}
				]
			]
		}
		'''
		self.set_referred_to_by(data, thing)

		for c in data.get('classified_as', []):
			thing.classified_as = c

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
				properties = []
			name_kwargs = {}
			for props in properties:
				if 'classified_as' in props:
					cl = props['classified_as']
					del props['classified_as']
					name_kwargs['title_type'] = cl

			n = set_la_name(thing, name, **name_kwargs)
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
			otypes = otype if isinstance(otype, list) else [otype]
			kwargs = {}
			if 'uri' in data:
				kwargs['ident'] = data['uri']
			elif 'uuid' in data:
				kwargs['ident'] = "urn:uuid:%s" % data['uuid']
			else:
				raise Exception('MakeLinkedArtRecord called with a dictionary with neither uuid or uri member')
			thing = vocab.make_multitype_obj(*otypes, **kwargs)

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
		if not label:
			warnings.warn(f'Setting empty label on {thing.id}')
		thing._label = label
	name = model.Name(ident='', content=label)
	if title_type is not None:
		if isinstance(title_type, model.Type):
			name.classified_as = title_type
		else:
			vocab.add_classification(name, title_type)
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

		if 'content' in data:
			thing.content = data['content']
		
		if 'also_found_on' in data:
			thing._validate_profile = False
			thing.features_are_also_found_on = data['also_found_on']

		for author in data.get('created_by', []):
			thing.created_by = author

		for a in data.get('used_for', []):
			thing.used_for = a

		for a in data.get('about', []):
			thing.about = a

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
		if 'object_type' not in data or data['object_type'] == []:
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
			if name:
				if isinstance(name, str):
					set_la_name(thing, name, title_type, set_label=True)
				elif isinstance(name, (list, tuple)):
					value, *properties = name
					n = model.Name(ident='', content=value)
					n.classified_as = title_type
					self.set_lo_properties(n, *properties)
					thing.identified_by = n

		for uri in data.get('exact_match', []):
			thing.exact_match = uri
		# import pdb; pdb.set_trace()
		for sdata in data.get('sojourns', []):
			if 'active_city' not in sdata:
				label = sdata.get('label', 'Sojourn activity')
				stype = sdata.get('type', model.Activity)
				act = stype(ident='', label=label)
				ts = get_crom_object(sdata.get('timespan'))
				if 'tgn' in sdata:
					place = sdata['tgn']
				else:
					place = get_crom_object(sdata.get('place'))
				act.timespan = ts
				# import pdb; pdb.set_trace()
				act.took_place_at = place
				thing.carried_out = act
				self.set_referred_to_by(sdata, act)

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
		# import pdb; pdb.set_trace()
		# iterate events only if we want professional activity block to exist (if there is active city). Else, we don't need events, so delete them.
		if 'active_city_date' in data:
			for event in data.get('events', []):
				
				for sdata in data.get('sojourns', []):
					if 'active_city' in sdata:
						if 'Professional activity' in event.__dict__['_label']:
							if 'tgn' in sdata:
								place = sdata['tgn']
							else:
								place = get_crom_object(sdata.get('place'))
							event.took_place_at = place
					
				thing.carried_out = event
		else:
			data['events'] = []
		# here that we have populated possible professional activity events, 
		# iterate all events and if not one event has 'took place at', meaning
		# there is not active city in the data, so group should not have block 'professional activity'
		
		for n in data.get('nationality', []):
			thing.classified_as = n

		if data.get('formation'):
			b = model.Formation()
			ts = model.TimeSpan(ident='')
			if 'formation_clean' in data and data['formation_clean']:
				if data['formation_clean'][0]:
					ts.begin_of_the_begin = data['formation_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
				if data['formation_clean'][1]:
					ts.end_of_the_end = data['formation_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
			verbatim = data['formation']
			ts._label = verbatim
			ts.identified_by = model.Name(ident='', content=verbatim)
			b.timespan = ts
			b._label = "Formation of %s" % thing._label
			thing.formed_by = b

		if data.get('dissolution'):
			d = model.Dissolution()
			ts = model.TimeSpan(ident='')
			if 'dissolution_clean' in data and data['dissolution_clean']:
				if data['dissolution_clean'][0]:
					ts.begin_of_the_begin = data['dissolution_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
				if data['dissolution_clean'][1]:
					ts.end_of_the_end = data['dissolution_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
			verbatim = data['dissolution']
			ts._label = verbatim
			ts.identified_by = model.Name(ident='', content=verbatim)
			d.timespan = ts
			d._label = "Dissolution of %s" % thing._label
			thing.dissolved_by = d

	def __call__(self, data: dict):
		if 'object_type' not in data or data['object_type'] == []:
			data['object_type'] = model.Group
		return super().__call__(data)

class MakeLinkedArtAuctionHouseOrganization(MakeLinkedArtOrganization):
	def __call__(self, data: dict):
		if 'object_type' not in data or data['object_type'] == []:
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

		for n in data.get('occupation', []):
			if isinstance(n, model.BaseResource):
				# import pdb; pdb.set_trace()
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
			# import pdb; pdb.set_trace()			
			# MAYBE HERE ITERATE DATA[SOJOURNS] AND TAKE PLACE 

			for sdata in data.get('sojourns', []):
				if 'active_city' in sdata:
					if 'Professional activity' in event.__dict__['_label']:
						if 'tgn' in sdata:
							place = sdata['tgn']
						else:
							place = get_crom_object(sdata.get('place'))
						event.took_place_at = place

			who.carried_out = event

		if data.get('birth'):
			b = model.Birth()
			ts = model.TimeSpan(ident='')
			if 'birth_clean' in data and data['birth_clean']:
				if data['birth_clean'][0]:
					ts.begin_of_the_begin = data['birth_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
				if data['birth_clean'][1]:
					ts.end_of_the_end = data['birth_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
			verbatim = data['birth']
			ts._label = verbatim
			ts.identified_by = model.Name(ident='', content=verbatim)
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
			verbatim = data['death']
			ts._label = verbatim
			ts.identified_by = model.Name(ident='', content=verbatim)
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
					pl = model.Identifier(ident='', content=p)
				who.contact_point = pl

	def __call__(self, data: dict):
		if 'object_type' not in data or data['object_type'] == []:
			data['object_type'] = model.Person
		return super().__call__(data)

class MakeLinkedArtPlace(MakeLinkedArtRecord):
	TYPES = {
		'city': vocab.instances['city'],
		'province': vocab.instances['province'],
		'state': vocab.instances['province'],
		'country': vocab.instances['nation'],
		'address': vocab.instances['address']
	}

	def __init__(self, base_uri=None, *args, **kwargs):
		super().__init__(*args, **kwargs)
		self.base_uri = base_uri

	def set_properties(self, data, thing):
		name = data.get('name')
		data.setdefault('names', [name])
		super().set_properties(data, thing)

		type_name = data.get('type', 'place').lower()
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
			# print(f'*** Setting parent on place object: {parent}')
			thing.part_of = parent

	def __call__(self, data: dict):
		if 'object_type' not in data or data['object_type'] == []:
			data['object_type'] = model.Place
		if self.base_uri and not data.get('uri'):
			data['uri'] = self.base_uri + urllib.parse.quote(data['name'])
		return super().__call__(data)

def geo_json(lat, lon, label):
	return {
		"type" : "FeatureCollection",
		"features": [{
			"type": "Feature",
			"geometry": {
				"type": "Point",
				"coordinates": [float(lon), float(lat)]
			},
			"properties": {
				"name": label
			}
		}]
	}

def make_tgn_place(tgn_data: dict, uri_creator=None, tgn_lookup = {}):
	place_shared_uri = uri_creator(('PLACE', 'TGN-ID', tgn_data.get('tgn_id')))
	# import pdb; pdb.set_trace()
	if tgn_data is None:
		return None

	id = tgn_data.get("tgn_id")
	
	# if the place is the 'World'
	# we don't need to model places as being part of it
	# see: https://vocab.getty.edu/tgn/7029392
	if id == '7029392':
		return None

	label = tgn_data.get('place_label')
	url = tgn_data.get('permalink')
	
	type_label = tgn_data.get('place_type_label')
	type_identifier = tgn_data.get("place_type_preferred")
	
	lat = tgn_data.get('latitude')
	lon = tgn_data.get('longitude')
	
	part_of = tgn_data.get('part_of')
	
	if place_shared_uri:
		p = model.Place(ident=place_shared_uri, label=label)
	else:
		warnings.warn(f"No share-uri was created for place with TGN {id}")
		p = model.Place(ident='', label=label)
	
	p.identified_by = vocab.PrimaryName(ident='', content=label)

	p.classified_as = model.Type(ident=type_identifier, label=type_label)
	
	if lat and lon:
		p.defined_by = json.dumps(geo_json(lat, lon, label))

	p.exact_match = model.BaseResource(ident=f"http://vocab.getty.edu/tgn/{id}")
	
	page = vocab.WebPage(ident=url, label=url)
	page._validate_range = False

	# battling with crom in the following lines
	# for the classification of a web page to be serialized
	# we have to assign it a dummy Digital Object acccess_point first 
	# and then remove it and also to no assing any identifier to the page itself!
	page.access_point = [model.DigitalObject(ident=url, label=url)]
	page.access_point = None
	
	p.referred_to_by = page
	if part_of:
		p.part_of = make_tgn_place(tgn_lookup[part_of], uri_creator, tgn_lookup)
	
	return p
	
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
		'address': vocab.instances['address']
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
	identifiers = data.get('identifiers')
	if identifiers:
		for identifier in identifiers:
			p.identified_by  = identifier
	elif name:
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
	def populate_object_statements(data:dict, default_unit=None, strip_comments=False):
		hmo = get_crom_object(data)
		record = data.get('_record')
		
		if not record:
			record = data.get('_records')
		
		if isinstance(record, list):
			sales_record = get_crom_objects(record)
		else:
			sales_record = get_crom_object(record)

		format = data.get('format')
		if format:
			formatstmt = vocab.PhysicalStatement(ident='', content=format)
			if sales_record:
				if isinstance(sales_record, list):
					for record in sales_record:
						formatstmt.referred_to_by = record
				else: 
					formatstmt.referred_to_by = sales_record
			hmo.referred_to_by = formatstmt

		materials = data.get('materials')
		if materials:
			matstmt = vocab.MaterialStatement(ident='', content=materials)
			if sales_record:
				if isinstance(sales_record, list):
					for record in sales_record:
						matstmt.referred_to_by = record
				else: 
					matstmt.referred_to_by = sales_record
			hmo.referred_to_by = matstmt

		dimstr = data.get('dimensions')

		if dimstr:
			dimstmt = vocab.DimensionStatement(ident='', content=dimstr)
			if sales_record:
				if isinstance(sales_record, list):
					for record in sales_record:
						dimstmt.referred_to_by = record
				else: 
					dimstmt.referred_to_by = sales_record
			hmo.referred_to_by = dimstmt
			if strip_comments:
				import re;
				dimstr = re.sub(r"\[.*\]", '', dimstr).strip()
			dimstr = dimstr.replace("X", "x")
			for dim in extract_physical_dimensions(dimstr, default_unit=default_unit):
				if sales_record:
					if isinstance(sales_record, list):
						for record in sales_record:
							dim.referred_to_by = record
					else: 
						dim.referred_to_by = sales_record
				hmo.dimension = dim
		else:
			pass
	# 		print(f'No dimension data was parsed from the dimension statement: {dimstr}')

