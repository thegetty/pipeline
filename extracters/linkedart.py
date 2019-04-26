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
	def set_properties(self, data, object):
		if data.get('label'):
			object._label = data['label']
			title_type = model.Type(ident='http://vocab.getty.edu/aat/300055726', label='Title') # TODO: is this the right aat URI?
			name = model.Name()
			name.classified_as = title_type
			name.content = data['label']
			object.identified_by = name
		
		for t in data.get('translations', []):
			title = model.Name()
			title.classified_as = title_type
			title.translation_of = name
			object.identified_by = title

class MakeLinkedArtAbstract(MakeLinkedArtLinguisticObject):
	def set_properties(self, data, object):
		super().set_properties(data, object)
		for id, type in data.get('identifiers', []):
			ident = model.Identifier()
			ident.content = id
			if type is not None:
				ident.classified_as = type
			object.identified_by = ident

def make_la_organization(data: dict):
	# TODO: turn this into a MakeLinkedArtRecord subclass
	# TODO: - figure out how to construct the model.Group in the subclass without data having either an _LOD_OBJECT or object_type key
	who = model.Group(ident="urn:uuid:%s" % data['uuid'])
	who._label = str(data['label'])

	if data.get('events'):
		for event in data['events']:
			who.carried_out = event

	for name in data.get('names', []):
		n = model.Name()
		n.content = name[0]
		for ref in name[1:]:
			l = model.LinguisticObject(ident="urn:uuid:%s" % ref[1])
			# l._label = _row_label(ref[2][0], ref[2][1], ref[2][2])
			n.referred_to_by = l
		who.identified_by = n

	for id, type in data.get('identifiers', []):
		ident = model.Identifier()
		ident.content = id
		if type is not None:
			ident.classified_as = type
		who.identified_by = ident

	return add_crom_data(data=data, what=who)
