import pprint
import warnings

from bonobo.config import Configurable, Service, Option

from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPerson, \
			get_crom_object, \
			add_crom_data

class ModelCorp(Configurable):
	helper = Option(required=True)

	def model_term_group(self, record, data):
		record.setdefault('identifiers', [])
		name = data['corp_name']
		
		record.setdefault('label', name)
		record['identifiers'].append(vocab.PrimaryName(ident='', content=name))

	def model_place(self, data):
		mlao = MakeLinkedArtOrganization()
		mlao(data)

	def __call__(self, data):
		cid = data['concept_group']['gaia_auth_id']
		data['uri'] = self.helper.corporate_body_uri(cid)
		
		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)
		
		self.model_place(data)
		return data
