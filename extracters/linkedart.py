import pprint

from cromulent import model, vocab
from cromulent.model import factory
factory.auto_id_type = 'uuid'
vocab.add_art_setter()

def add_crom_data(data: dict, what=None):
	data['_CROM_FACTORY'] = factory
	data['_LOD_OBJECT'] = what
	return data

class MakeLinkedArtRecord:
	def set_properties(self, data, object):
		pass

	def __call__(self, data: dict):
		if '_LOD_OBJECT' in data:
			object = data['_LOD_OBJECT']
		else:
			otype = data['object_type']
			object = otype(ident="urn:uuid:%s" % data['uuid'])
		self.set_properties(data, object)

		return add_crom_data(data=data, what=object)

def set_la_name(object, value, title_type=None, set_label=False):
	if value is None:
		return
	if type(value) == tuple:
		label, language = value
	else:
		label = value
		language = None
	if set_label:
		object._label = label
	name = model.Name()
	if title_type is not None:
		name.classified_as = title_type
	name.content = label
	object.identified_by = name
	if language is not None:
		name.language = language
	return name

class MakeLinkedArtLinguisticObject(MakeLinkedArtRecord):
	# TODO: document the expected format of data['translations']
	# TODO: document the expected format of data['identifiers']
	# TODO: document the expected format of data['names']
	def set_properties(self, data, object):
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300055726', label='Title') # TODO: is this the right aat URI?
		name = None
		if 'label' in data:
			name = set_la_name(object, data['label'], title_type, set_label=True)

		for t in data.get('translations', []):
			n = set_la_name(object, t, title_type)
			if name is not None:
				n.translation_of = name

		for id, itype in data.get('identifiers', []):
			if itype is None:
				itype = vocab.Identifier
			if type(itype) == type:
				ident = itype(content=id)
				ident.content = id # TODO: This shouldn't be needed, but the crom instantiation above ignores it
			else:
				ident = model.Identifier()
				ident.content = id
				ident.classified_as = itype
			object.identified_by = ident

		for name in data.get('names', []):
			n = set_la_name(object, name[0])
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
			object.about = classification

		for c in data.get('indexing', []):
			if type(c) == tuple:
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
			object.about = indexing


class MakeLinkedArtAbstract(MakeLinkedArtLinguisticObject):
	pass

class MakeLinkedArtOrganization(MakeLinkedArtRecord):
	# TODO: document the expected format of data['names']
	def set_properties(self, data, object):
		object._label = str(data['label'])

		if 'events' in data:
			for event in data['events']:
				object.carried_out = event

		for name in data.get('names', []):
			n = set_la_name(object, name[0])
			for ref in name[1:]:
				l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
				# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
				n.referred_to_by = l
# 			object.identified_by = n

	def __call__(self, data: dict):
		if 'object_type' not in data:
			data['object_type'] = model.Group
		return super().__call__(data)
