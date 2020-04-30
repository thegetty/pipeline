import pprint
import warnings

from bonobo.config import Configurable, Service, Option

from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtPerson, \
			make_la_place, \
			get_crom_object, \
			add_crom_data

class ModelPlace(Configurable):
	helper = Option(required=True)

	def model_term_group(self, record, data):
		record.setdefault('identifiers', [])
		name = data.get('term_name')

		if name:
			record.setdefault('label', name)
			record.setdefault('name', name)
			record['identifiers'].append(vocab.Name(ident='', content=name))

	def model_place(self, data):
		pid = data['concept_group']['gaia_auth_id']
		data.setdefault('label', f'Place ({pid})')
		make_la_place(data)

	def __call__(self, data):
		pid = data['concept_group']['gaia_auth_id']
		data['uri'] = self.helper.place_uri(pid)
		
		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)
		
		self.model_place(data)
		return data
