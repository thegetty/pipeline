import pprint
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

	@staticmethod
	def model_concept_group(record, data):
		if data.get('country_name'):
			country = data['country_name']
			print(f'*** COUNTRY: {country}')
			record['country'] = {
				'label': country,
				'name': country,
				'type': 'country'
			}

	@staticmethod
	def model_term_group(record, data):
		record.setdefault('identifiers', [])
		name = data.get('term_name')
		state = data.get('state_province')
# 		print(f'*** PLACE: {name}')

		country = data.get('country', {}).get('label')
		if state:
			print(f'*** STATE: {state}')
			parent = data.get('country')
			state = {
				'name': state,
				'label': state,
				'type': 'state'
			}
			if country:
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
# 		print(f'MAKE PLACE: {pprint.pformat(data)}')
		mlap(data)
		place = get_crom_object(data)
# 		print('===================>')
# 		print(factory.toString(place, False))

	def __call__(self, data):
		pid = data['concept_group']['gaia_auth_id']

		self.model_concept_group(data, data['concept_group'])
		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)

		data['uri'] = self.helper.place_uri(pid)
		self.model_place(data)
		return data
