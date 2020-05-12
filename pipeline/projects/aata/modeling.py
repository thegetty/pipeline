import pprint
import warnings

import re
import warnings
from cromulent import model, vocab
from bonobo.config import Configurable, Option

class ModelBase(Configurable):
	helper = Option(required=True)

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
		warnings.warn('model warrant group')
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

