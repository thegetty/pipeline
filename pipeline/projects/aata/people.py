import pprint
import warnings

from bonobo.config import Configurable, Option

from pipeline.projects.aata.modeling import ModelBase
from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtPerson, \
			get_crom_object, \
			add_crom_data

class ModelPerson(ModelBase):
	def model_concept_group(self, record, data):
		record.setdefault('identifiers', [])
		record.setdefault('nationality', [])

		gaia_id = data['gaia_auth_id']
		record['uri'] = self.helper.person_uri(gaia_id)
		record['identifiers'].append(self.helper.gci_number_id(gaia_id))

		n = data.get('nationality')
		if n:
			record['nationality'].append(n)

	@staticmethod
	def model_term_group(record, data):
		record.setdefault('identifiers', [])

		pname = data.get('primary_name', '')
		sname = data.get('secondary_name', '')
		qual = data.get('name_qualifier')
		if pname and sname:
			name = f'{sname} {pname}'
		elif pname or sname:
			name = ''.join([pname, sname])
		else:
			return

		if qual:
			name += f' {qual}'

		record.setdefault('label', name)
		record['identifiers'].append(vocab.Name(ident='', content=name))

	def model_relationship_group(self, record, data):
		# TODO
		pass

	@staticmethod
	def model_person(data):
		mlap = MakeLinkedArtPerson()
		mlap(data)

	def __call__(self, data):
		self.model_concept_group(data, data['concept_group'])
		pid = data['concept_group']['gaia_auth_id']

		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)
		for rg in _as_list(data.get('gaia_auth_relationship_group')):
			self.model_relationship_group(data, rg)
		for mg in _as_list(data.get('exact_match_group')):
			self.model_exact_match_group(data, mg)
		for wg in _as_list(data.get('warrant_group')):
			self.model_warrant_group(data, wg)

		data.setdefault('label', f'Person ({pid})')
		self.model_person(data)

		return data
