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

class MakeLinkedArtLinguisticObject(MakeLinkedArtRecord):
	# TODO: document the expected format of data['translations']
	def set_properties(self, data, object):
		title_type = model.Type(ident='http://vocab.getty.edu/aat/300055726', label='Title') # TODO: is this the right aat URI?
		name = None
		if 'label' in data:
			object._label = data['label']
			name = model.Name()
			name.classified_as = title_type
			name.content = data['label']
			object.identified_by = name

		for t in data.get('translations', []):
			title = model.Name()
			title.classified_as = title_type
			if name is not None:
				title.translation_of = name
			object.identified_by = title

class MakeLinkedArtAbstract(MakeLinkedArtLinguisticObject):
	# TODO: document the expected format of data['identifiers']
	def set_properties(self, data, object):
		super().set_properties(data, object)
		for id, type in data.get('identifiers', []):
			ident = model.Identifier()
			ident.content = id
			if type is not None:
				ident.classified_as = type
			object.identified_by = ident

class MakeLinkedArtOrganization(MakeLinkedArtRecord):
	# TODO: document the expected format of data['names']
	def set_properties(self, data, object):
		object._label = str(data['label'])

		if 'events' in data:
			for event in data['events']:
				object.carried_out = event

		for name in data.get('names', []):
			n = model.Name()
			n.content = name[0]
			for ref in name[1:]:
				l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
				# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
				n.referred_to_by = l
			object.identified_by = n

		for id, type in data.get('identifiers', []):
			ident = model.Identifier()
			ident.content = id
			if type is not None:
				ident.classified_as = type
			object.identified_by = ident

	def __call__(self, data: dict):
		data['object_type'] = model.Group
		return super().__call__(data)
