import warnings

from bonobo.config import Configurable, Option

from cromulent import model, vocab
from cromulent.model import factory
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtPlace, \
			get_crom_object

class ModelPlace(Configurable):
	helper = Option(required=True)

	def model_concept_group(self, record, data):
		record.setdefault('classified_as', [])
		if data.get('country_name'):
			country = data['country_name']
# 			print(f'*** COUNTRY: {country}')
			record['country'] = {
				'uri': self.helper.named_place_uri(country),
				'label': country,
				'name': country,
				'type': 'country'
			}
		place_type = data.get('place_type')
		cl = self.helper.place_classification(place_type)
		if cl:
			record['classified_as'].append(cl)
# 			print(f'{data["gaia_auth_id"]}: {place_type}: {cl}')

	@staticmethod
	def model_term_group(record, data):
		record.setdefault('identifiers', [])
		name = data.get('term_name')
		state = data.get('state_province')
# 		print(f'*** PLACE: {name}')

		country = data.get('country', {}).get('label')
		if state:
			parent = data.get('country')
# 			print(f'*** STATE: {state}')
			state = {
				'name': state,
				'label': state,
				'type': 'state'
			}
			if country:
				state['uri'] = self.helper.named_place_uri(country, state),
				state['label'] = f'{state}, {country}'
			record['state'] = state

		if name:
			record.setdefault('label', name)
			record.setdefault('name', name)
			record['identifiers'].append(vocab.Name(ident='', content=name))

	def model_place(self, data):
		country = data.get('country')
		state = data.get('state')
		if country:
			data['part_of'] = country
		if state:
			state['part_of'] = data.get('part_of')
			data['part_of'] = state
		pid = data['concept_group']['gaia_auth_id']
		data.setdefault('label', f'Place ({pid})')
		place_base = self.helper.make_proj_uri('Place:')
		mlap = MakeLinkedArtPlace(base_uri=place_base)
		mlap(data)
		place = get_crom_object(data)
# 		print('===================>')
# 		print(factory.toString(place, False))

	def add_uri(self, data):
		cg = data.get('concept_group', {})
		pid = cg['gaia_auth_id']
		terms = _as_list(data.get('term_group', []))
		names = [t.get('term_name') for t in terms if t.get('term_type') == 'main']
		name = names[0] if names else None
		place_type = cg.get('place_type')

		country = data.get('country')
		state = data.get('state')
		names = []
		if country:
			names.append(country['label'])
		if state:
			names.append(state['label'])
		names.append(name)

		data['uri'] = self.helper.place_uri(pid, *names, place_type=place_type)

	def __call__(self, data):
		pid = data['concept_group']['gaia_auth_id']
		data.setdefault('identifiers', [])
		data['identifiers'].append(self.helper.gci_number_id(pid, id_class=vocab.SystemNumber))

		self.model_concept_group(data, data['concept_group'])
		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)

		self.add_uri(data)
		self.model_place(data)
		return data
