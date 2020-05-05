import re
import pprint
import warnings

from bonobo.config import Configurable, Option

from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPlace, \
			get_crom_object, \
			add_crom_data

class ModelCorp(Configurable):
	helper = Option(required=True)

	def model_concept_group(self, record, data):
		record.setdefault('referred_to_by', [])
		record.setdefault('identifiers', [])
		record.setdefault('_places', []) # for extraction/serialization by the pipeline
		record.setdefault('places', []) # for pipeline.linkedart modeling code

		gaia_id = data['gaia_auth_id']
		snote = data.get('scope_note')
		inote = data.get('internal_note')
		snfnote = data.get('source_not_found_note')
		locations = _as_list(data.get('location', []))

		record['uri'] = self.helper.corporate_body_uri(gaia_id)
		record['identifiers'].append(self.helper.gci_number_id(gaia_id))

		if snote:
			record['referred_to_by'].append(vocab.Note(ident='', content=snote))
		if inote:
			record['referred_to_by'].append(vocab.InternalNote(ident='', content=inote))
		if snfnote:
			record['referred_to_by'].append(vocab.InternalNote(ident='', content=snfnote))

		mlap = MakeLinkedArtPlace()
		for loc in locations:
			geog_id = loc.get('gaia_geog_id')
			if geog_id:
				geog_uri = self.helper.place_uri(geog_id)
				geog_data = {
					'uri': geog_uri,
					'identifiers': [self.helper.gci_number_id(geog_id)],
				}
				geog_name = loc.get('location_string')
				if geog_name:
					geog_data['label'] = geog_name
					geog_data['name'] = geog_name
				mlap(geog_data)
				record['places'].append(geog_data)
				record['_places'].append(geog_data)

	@staticmethod
	def model_term_group(record, data):
		record.setdefault('identifiers', [])
		term_type = data['term_type']
		cl = model.Name
		if term_type == 'main':
			cl = vocab.PrimaryName
		elif term_type == 'in-house':
			# TODO: flag as no-display
			pass
		name = data['corp_name']

		record.setdefault('label', name)
		record['identifiers'].append(cl(ident='', content=name))

	def model_exact_match_group(self, record, data):
		record.setdefault('exact_match', [])

		brief_name = data.get('resource_brief_name')
		full_name = data.get('resource_full_name')
		rid = data.get('resource_id')
		if brief_name and rid:
			uri = self.helper.exact_match_uri(brief_name, rid)
			if uri:
				exact_match = model.BaseResource(ident=uri)
				record['exact_match'].append(exact_match)

	def model_warrant_group(self, record, data):
		record.setdefault('referred_to_by', [])
		record.setdefault('exact_match', [])

		brief_name = data.get('resource_brief_name')
		description = data.get('warrant_description')
		note = data.get('warrant_note')
		if brief_name and description:
			if brief_name == 'Unknown' and description.startswith('Bib Record #'):
				return # ignore internal references
		
		if brief_name == 'LCSH' and note:
			lcsh_pattern = re.compile(r'((nb|nr|no|ns|sh|n)(\d+))')
			match = lcsh_pattern.search(note)
			if match:
				lcid = match.group(1)
				lcuri = f'http://id.loc.gov/authorities/names/{lcid}'
				exact_match = model.BaseResource(ident=lcuri)
				record['exact_match'].append(exact_match)
				return

		if description and note:
			n = vocab.BibliographyStatement(ident='', content=note)
			n.identified_by = model.Name(ident='', content=description)
			record['referred_to_by'].append(n)
		elif description:
			n = vocab.BibliographyStatement(ident='', content=description)
			record['referred_to_by'].append(n)
		elif note:
			n = vocab.BibliographyStatement(ident='', content=note)
			record['referred_to_by'].append(n)

	@staticmethod
	def model_place(data):
		mlao = MakeLinkedArtOrganization()
		mlao(data)

	def __call__(self, data):
		self.model_concept_group(data, data['concept_group'])
		for tg in _as_list(data.get('term_group')):
			self.model_term_group(data, tg)
		for mg in _as_list(data.get('exact_match_group')):
			self.model_exact_match_group(data, mg)
		for wg in _as_list(data.get('warrant_group')):
			self.model_warrant_group(data, wg)

		self.model_place(data)
		return data
