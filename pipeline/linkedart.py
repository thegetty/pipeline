import pprint
from contextlib import suppress

from cromulent import model, vocab
from cromulent.model import factory
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

class MakeLinkedArtRecord:
	def set_properties(self, data, thing):
		for identifier in data.get('identifiers', []):
			if isinstance(identifier, tuple):
				content, itype = identifier
				if itype is not None:
					if isinstance(itype, type):
						ident = itype(content=content)
					elif isinstance(itype, object):
						ident = itype
						ident.content = content
					else:
						ident = model.Identifier()
						ident.content = content
						ident.classified_as = itype
			else:
				ident = identifier
			thing.identified_by = ident

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
	name = model.Name()
	if title_type is not None:
		name.classified_as = title_type
	name.content = label
	thing.identified_by = name
	if language is not None:
		name.language = language
	return name

class MakeLinkedArtLinguisticObject(MakeLinkedArtRecord):
	# TODO: document the expected format of data['translations']
	# TODO: document the expected format of data['identifiers']
	# TODO: document the expected format of data['names']
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300055726', label='Title') # TODO: is this the right aat URI?
		name = None
		if 'label' in data:
			name = set_la_name(thing, data['label'], title_type, set_label=True)

		for t in data.get('translations', []):
			n = set_la_name(thing, t, title_type)
			if name is not None:
				n.translation_of = name

		for content, itype, notes in data.get('qualified_identifiers', []):
			ident = itype(content=content)
			thing.identified_by = ident
			for n in notes:
				ident.referred_to_by = n

		for name in data.get('names', []):
			n = set_la_name(thing, name[0])
			for ref in name[1:]:
				l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
				# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
				n.referred_to_by = l

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
				classification.identified_by = name

				code = model.Identifier()
				code.classified_as = code_type
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
				indexing.identified_by = name

				code = model.Identifier()
				code.classified_as = code_type
				code.content = cid
				indexing.identified_by = code
			else:
				indexing = c
			thing.about = indexing


class MakeLinkedArtHumanMadeObject(MakeLinkedArtRecord):
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300055726', label='Title') # TODO: is this the right aat URI?
		if 'label' in data:
			set_la_name(thing, data['label'], title_type, set_label=True)

		if 'title' in data:
			# TODO: This needs to be a PrimaryName, not a Name classified as a Title
			set_la_name(thing, data['title'], title_type, set_label=True)

		for coll in data.get('member_of', []):
			thing.member_of = coll

		for identifier in data.get('identifiers', []):
			if isinstance(identifier, tuple):
				content, itype = identifier
				if itype is not None:
					if isinstance(itype, type):
						ident = itype(content=content)
					elif isinstance(itype, object):
						ident = itype
						ident.content = content
					else:
						ident = model.Identifier()
						ident.content = content
						ident.classified_as = itype
			else:
				ident = identifier
			thing.identified_by = ident

		for name in data.get('names', []):
			n = set_la_name(thing, name[0])
			for ref in name[1:]:
				l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
				# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
				n.referred_to_by = l

		for annotation in data.get('annotations', []):
			a = model.Annotation()
			a.content = content
			thing.carries = a


class MakeLinkedArtAbstract(MakeLinkedArtLinguisticObject):
	pass

class MakeLinkedArtOrganization(MakeLinkedArtRecord):
	# TODO: document the expected format of data['names']
	def set_properties(self, data, thing):
		super().set_properties(data, thing)
		with suppress(KeyError):
			thing._label = str(data['label'])

		ulan = data.get('ulan')
		if ulan:
			thing.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{ulan}')

		if 'events' in data:
			for event in data['events']:
				thing.carried_out = event

		for name in data.get('names', []):
			n = set_la_name(thing, name[0])
			for ref in name[1:]:
				l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
				# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
				n.referred_to_by = l
# 			thing.identified_by = n

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.Group
		return super().__call__(data)

class MakeLinkedArtAuctionHouseOrganization(MakeLinkedArtOrganization):
	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = vocab.AuctionHouseOrg
		return super().__call__(data)

def make_la_person(data: dict):
	uri = data.get('uri')
	who = model.Person(ident=uri)
	who._label = str(data['label'])

	ulan = data.get('ulan')
	if ulan:
		who.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{ulan}')

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

	# nationality field can contain other information, but not useful.
	# XXX Intentionally ignored but validate with GRI

	if data.get('active_early') or data.get('active_late'):
		act = vocab.Active()
		ts = model.TimeSpan()
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
		ts = model.TimeSpan()
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
		ts = model.TimeSpan()
		if 'death_clean' in data and data['death_clean']:
			if data['death_clean'][0]:
				ts.begin_of_the_begin = data['death_clean'][0].strftime("%Y-%m-%dT%H:%M:%SZ")
			if data['death_clean'][1]:
				ts.end_of_the_end = data['death_clean'][1].strftime("%Y-%m-%dT%H:%M:%SZ")
		ts._label = data['death']
		d.timespan = ts
		d._label = "Death of %s" % who._label
		who.died = d

	# Add names
	for name in data.get('names', []):
		n = model.Name(ident='', content=name[0])
		for ref in name[1:]:
			l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
			# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
			n.referred_to_by = l
		who.identified_by = n

	for identifier in data.get('identifiers', []):
		if isinstance(identifier, tuple):
			content, itype = identifier
			if itype is not None:
				if isinstance(itype, type):
					ident = itype(content=content)
				elif isinstance(itype, object):
					ident = itype
					ident.content = content
				else:
					ident = model.Identifier()
					ident.content = content
					ident.classified_as = itype
		else:
			ident = identifier
		who.identified_by = ident

	# Locations are names of residence places (P74 -> E53)
	# XXX FIXME: Places are their own model
	if 'places' in data:
		for p in data['places']:
			pl = model.Place()
			#pl._label = p['label']
			#nm = model.Name()
			#nm.content = p['label']
			#pl.identified_by = nm
			#for s in p['sources']:
			#		l = model.LinguisticObject(ident="urn:uuid:%s" % s[1])
				# l._label = _row_label(s[2], s[3], s[4])
			#	pl.referred_to_by = l
			who.residence = pl

	for uri in data.get('exact_match', []):
		who.exact_match = uri

	return add_crom_data(data=data, what=who)

def make_la_place(data: dict):
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

	type = TYPES.get(type_name)
	parent = None
	if parent_data:
		parent_data = make_la_place(parent_data)
		parent = get_crom_object(parent_data)
		label = f'{label}, {parent._label}'
		
	placeargs = {'label': label}
	if data.get('uri'):
		placeargs['ident'] = data['uri']
	p = model.Place(**placeargs)
	if type:
		p.classified_as = type
	p.identified_by = model.Name(ident='', content=name)
	if parent:
		p.part_of = parent
	return add_crom_data(data=data, what=p)
