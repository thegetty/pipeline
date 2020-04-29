import pprint
import warnings

from bonobo.config import Configurable, Service, Option

from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtPerson, \
			get_crom_object, \
			add_crom_data

class ModelJournal(Configurable):
	helper = Option(required=True)

	def model_record_desc_group(self, record, data):
		record.setdefault('referred_to_by', [])
		record.setdefault('identifiers', [])

		jid = data['record_id']
		inote = data.get('internal_note')
		snote = data.get('source_note')
		record['identifiers'].append(self.helper.gci_number_id(jid))
		
		if inote:
			record['referred_to_by'].append(vocab.Note(ident='', content=inote))
		if snote:
			record['referred_to_by'].append(vocab.Note(ident='', content=snote))

	def model_journal_group(self, record, data):
		if not data:
			return
		record.setdefault('identifiers', [])
		record.setdefault('referred_to_by', [])
		record.setdefault('language', [])

		title = data.get('title')
		title_translated = data.get('title_translated')
		variant_titles = _as_list(data.get('variant_title'))
		related_titles = _as_list(data.get('related_title'))
		lang_docs = _as_list(data['lang_doc'])
		frequency = data.get('frequency')
		start_year = data.get('start_year')
		cease_year = data.get('cease_year')
		issn = data.get('issn')
		coden = data.get('coden')
		if title:
			record['label'] = title
			record['identifiers'].append(vocab.PrimaryName(ident='', content=title))
		if title_translated:
			record['identifiers'].append(vocab.TranslatedTitle(ident='', content=title))
		for lang in lang_docs:
			l = self.helper.language_object_from_code(lang)
			if l:
				record['language'].append(l)
		if frequency:
			record['referred_to_by'].append(vocab.Note(ident='', content=frequency))

		# TODO: start_year
		# TODO: cease_year

		if issn:
			record['identifiers'].append(vocab.IssnIdentifier(ident='', content=issn))

		if coden:
			record['identifiers'].append(vocab.CodenIdentifier(ident='', content=coden))

	def model_publisher_group(self, record, data):
		# TODO:
		# publisher_group/gaia_corp_id
		# publisher_group/gaia_geog_id
		pass

	def model_sponsor_group(self, record, data):
		# TODO:
		# sponsor_group/ ???
		pass

	def model_issue_group(self, record, data):
		# TODO:
		# /issue_group
		# issue_group/issue_id
		# issue_group/title
		# issue_group/title_translated
		# issue_group/date/display_date
		# issue_group/date/sort_year
		# issue_group/volume
		# issue_group/number
		# issue_group/note
		pass

	def model_journal(self, data):
		data['object_type'] = vocab.JournalText
		mlalo = MakeLinkedArtLinguisticObject()
		mlalo(data)

	def __call__(self, data):
		jid = data['record_desc_group']['record_id']
		data['uri'] = self.helper.journal_uri(jid)
		
		self.model_record_desc_group(data, data['record_desc_group'])
		self.model_journal_group(data, data.get('journal_group'))
		for ig in _as_list(data.get('issue_group')):
			self.model_issue_group(data, ig)
		for pg in _as_list(data.get('publisher_group')):
			self.model_publisher_group(data, pg)
		for sg in _as_list(data.get('sponsor_group')):
			self.model_sponsor_group(data, sg)

		data.setdefault('label', f'Journal ({jid})')
		self.model_journal(data)
		return data
