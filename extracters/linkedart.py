from cromulent import model, vocab
from cromulent.model import factory
factory.auto_id_type = 'uuid'
vocab.add_art_setter()

def add_crom_data(data: dict, what=None):
	data['_CROM_FACTORY'] = factory
	data['_LOD_OBJECT'] = what
	return data

def make_la_record(data: dict):
	otype = data['object_type']
	object = otype(ident="urn:uuid:%s" % data['uuid'])
	object._label = data['label']
	name = model.Name()
	name.content = data['label']
	object.identified_by = name
	for t in data.get('translations', []):
		title = model.Name()
		title.translation_of = name
		object.identified_by = title
	
	return add_crom_data(data=data, what=object)

def make_la_abstract(data: dict):
	a = data['_LOD_OBJECT']
	for id, type in data.get('identifiers', []):
		ident = model.Identifier()
		ident.content = id
		if type is not None:
			ident.classified_as = type
		a.identified_by = ident
	return add_crom_data(data=data, what=a)

def make_la_organization(data: dict):
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
