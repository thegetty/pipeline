import pprint
import warnings

from bonobo.config import Configurable, Service, Option

from cromulent import model, vocab
from cromulent.model import factory
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtPerson, \
			MakeLinkedArtOrganization, \
			get_crom_object, \
			add_crom_data

class ModelArticle(Configurable):
	helper = Option(required=True)
	language_code_map = Service('language_code_map')

	def model_record_desc_group(self, record, data):
		code = data['doc_type']['doc_code']
		cls = self.helper.document_type_class(code)
		record['object_type'] = cls

	def model_record_id_group(self, record, data):
		record.setdefault('identifiers', [])
		record.setdefault('part_of', [])

		rid = data['record_id']
		aata_ids = _as_list(data.get('aata_id'))
		cid = data.get('collective_rec_id')

		record['identifiers'] += [vocab.LocalNumber(ident='', content=aid) for aid in aata_ids]
		record['identifiers'] += [vocab.LocalNumber(ident='', content=rid)]

		if cid:
			uri = self.helper.article_uri(cid)
			parent = {'uri': uri}
			make_la_lo = MakeLinkedArtLinguisticObject()
			make_la_lo(parent)
			record['part_of'].append(parent)

	def model_authorship_group(self, record, data):
		if not data:
			return
		record.setdefault('_people', [])
		record.setdefault('created_by', [])
		authors = _as_list(data.get('primary_author'))

		mlap = MakeLinkedArtPerson()
		mlao = MakeLinkedArtOrganization()

		ordered_data = []
		article_label = record['label']
		creation_id = record['uri'] + '-Creation'
		creation = model.Creation(ident=creation_id, label=f'Creation of {article_label}')
		for a in authors:
			gaia_id = a['gaia_authority_id']
			gaia_type = a['gaia_authority_type']
			name = a['author_name']
			roles = _as_list(a['author_role'])
			order = a['author_order']

			ordered_data.append((order, name))

			p = {
				'label': name,
				'name': name,
			}

			if gaia_type == 'Person':
				uri = self.helper.person_uri(gaia_id)
				p['uri'] = uri
				mlap(p)
			elif gaia_type == 'Corp':
				uri = self.helper.corporate_body_uri(gaia_id)
				p['uri'] = uri
				mlao(p)
			else:
				raise Exception(f'Unexpected type of authorship record: {gaia_type}')
# 				uri = self.helper.make_proj_uri(gaia_type, 'GAIA', gaia_id)

			record['_people'].append(p)

			for role in roles:
				part = model.Creation(ident='', label=f'{role} Creation sub-event')
				part.carried_out_by = get_crom_object(p)
				cl = self.helper.role_type(role)
				if cl:
					part.classified_as = cl
				creation.part = part

		ordered_authors = [p[1] for p in sorted(ordered_data)]
		order_string = self.helper.ordered_author_string(ordered_authors)
		creation.referred_to_by = vocab.Note(ident='', content=order_string)
		record['created_by'].append(creation)

	def model_title_group(self, record, data):
		record.setdefault('identifiers', [])

		primary = data['primary']
		title = primary.get('title')
		translated = primary.get('title_translated')
		variants = _as_list(primary.get('title_variant'))

		if title:
			record['label'] = title
			if 'title' in record:
				raise Exception(f'existing title!')
			record['title'] = title
		if translated:
			record['identifiers'].append(vocab.TranslatedTitle(ident='', content=translated))
		for v in variants:
			record['identifiers'].append(vocab.Title(ident='', content=v))

	def model_imprint_group(self, record, data):
		if not data:
			return
		record.setdefault('referred_to_by', [])
		record.setdefault('used_for', [])
		record.setdefault('part_of', [])
		record.setdefault('_activities', [])
		record.setdefault('_groups', [])
		record.setdefault('_places', [])
		record.setdefault('identifiers', [])

		edition = data.get('edition')
		series_number = data.get('series_number')
		doi = data.get('doi')
		coden = data.get('coden')
		website = data.get('website_address')
		publishers = _as_list(data.get('publisher'))
		distributors = _as_list(data.get('distributor'))
		journal = data.get('journal_info')
			# imprint_group/journal_info/aata_journal_id
			# imprint_group/journal_info/aata_issue_id
		degree = data.get('thesis_degree')
		tr = data.get('technical_report_number')

		if edition:
			record['referred_to_by'].append(vocab.EditionStatement(ident='', content=edition))

		if series_number:
			record['referred_to_by'].append(vocab.Note(ident='', content=series_number)) # TODO: classify this Note

		if doi:
			record['identifiers'].append(vocab.DoiIdentifier(ident='', content=doi))

		if coden:
			record['identifiers'].append(vocab.CodenIdentifier(ident='', content=coden))

		if website:
			record['referred_to_by'].append(vocab.Note(ident='', content=website))

		article_label = record['label']
		for i, publisher in enumerate(publishers):
			corp_id = publisher.get('gaia_corp_id')
			geog_id = publisher.get('publisher_location', {}).get('gaia_geog_id')
			a_uri = record['uri'] + f'-pub-{i}'
			a = vocab.Publishing(ident=a_uri, label=f'Publishing of {article_label}')
			if corp_id:
				uri = self.helper.corporate_body_uri(corp_id)
				g = model.Group(ident=uri)
				a.carried_out_by = g
				record['_groups'].append(add_crom_data({}, g))
			if geog_id:
				uri = self.helper.place_uri(geog_id)
				p = model.Place(ident=uri)
				a.took_place_at = p
				record['_places'].append(add_crom_data({}, p))
			record['used_for'].append(a)
# 			record['_activities'].append(add_crom_data({}, a))

		for i, distributor in enumerate(distributors):
			corp_id = distributor.get('gaia_corp_id')
			geog_id = distributor.get('distributor_location', {}).get('gaia_geog_id')
			a_uri = record['uri'] + f'-dist-{i}'
			a = vocab.Distributing(ident=a_uri, label=f'Distribution of {article_label}')
			if corp_id:
				uri = self.helper.corporate_body_uri(corp_id)
				g = model.Group(ident=uri)
				a.carried_out_by = g
				record['_groups'].append(add_crom_data({}, g))
			if geog_id:
				uri = self.helper.place_uri(geog_id)
				p = model.Place(ident=uri)
				a.took_place_at = p
				record['_places'].append(add_crom_data({}, p))
			record['used_for'].append(a)
# 			record['_activities'].append(add_crom_data({}, a))

		if journal:
			journal_id = journal.get('aata_journal_id')
			issue_id = journal.get('aata_issue_id')
			issue_uri = self.helper.issue_uri(journal_id, issue_id)
			issue = vocab.IssueText(ident=issue_uri)
			record['part_of'].append(add_crom_data({'uri': issue_uri}, issue))

		if degree:
			record['referred_to_by'].append(vocab.Note(ident='', content=degree))

		if tr:
			record['identifiers'].append(model.Identifier(ident='', content=tr)) # TODO: classify this Identifier

	@staticmethod
	def model_physical_desc_group(record, data):
		if not data:
			return
		record.setdefault('referred_to_by', [])

		pages = data.get('pages')
		collation = data.get('collation')
		illustrations = data.get('illustrations')
		medium = data.get('electronic_medium_type')

		if pages:
			record['referred_to_by'].append(vocab.PaginationStatement(ident='', content=pages))

		if collation:
			record['referred_to_by'].append(vocab.Description(ident='', content=collation))

		if illustrations:
			record['referred_to_by'].append(vocab.IllustruationStatement(ident='', content=illustrations))

		if medium:
			record['referred_to_by'].append(vocab.PhysicalStatement(ident='', content=medium))

	def model_notes_group(self, record, data):
		if not data:
			return
		record.setdefault('_declared_languages', set())
		record.setdefault('language', [])
		record.setdefault('identifiers', [])
		record.setdefault('referred_to_by', [])

		lang_docs = _as_list(data.get('lang_doc'))
		lang_summaries = _as_list(data.get('lang_summary'))
		isbns = _as_list(data.get('isbn'))
		issns = _as_list(data.get('issn'))
		citation_note = data.get('citation_note')
		inotes = _as_list(data.get('internal_note'))

		for lang in lang_summaries:
			record['_declared_languages'].add(lang)

		for lang in lang_docs:
			record['_declared_languages'].add(lang)
			l = self.helper.language_object_from_code(lang)
			if l:
				record['language'].append(l)

		for isbn in isbns:
			num = isbn.get('isbn_number')
			q = isbn.get('isbn_qualifier')
			if num:
				i = vocab.IsbnIdentifier(ident='', content=num)
				if q:
					i.referred_to_by = vocab.Note(ident='', content=q)
				record['identifiers'].append(i)

		for issn in issns:
			i = vocab.IssnIdentifier(ident='', content=issn)
			record['identifiers'].append(i)

		if citation_note:
			record['referred_to_by'].append(vocab.Citation(ident='', content=citation_note))

		for inote in inotes:
			record['referred_to_by'].append(vocab.Note(ident='', content=inote['note']))

	@staticmethod
	def model_abstract_group(record, data):
		if not data:
			return
		record.setdefault('referred_to_by', [])

		a = data.get('abstract')
		if a:
			record['referred_to_by'].append(vocab.Abstract(ident='', content=a))

	def model_classification_group(self, record, data):
		record.setdefault('classified_as', [])

		code = data['class_code']
		name = data['class_name']
		uri = self.helper.make_proj_uri('Classification', code)
		t = model.Type(ident=uri, label=name)
		record['classified_as'].append(t)

	@staticmethod
	def model_index_group(record, data):
		record.setdefault('indexing', [])

		term = data['index_term']
		opids = _as_list(data.get('other_persistent_id'))
		for opid in opids:
			eid = opid['external_id']
			uri = f'http://vocab.getty.edu/aat/{eid}'
			t = model.Type(ident=uri, label=term)
			record['indexing'].append(t)

	def add_title(self, data):
		'''
		Special handling is given to modeling of the title (PrimaryName) of the article.
		If we can detect that it is written in a language that matches one of the
		languages asserted for either the document or summaries, then we assert that
		as the language of the title Linguistic Object.
		'''
		if data.get('title'):
			title = data['title']
			restrict = data['_declared_languages']
			lang = self.helper.validated_string_language(title, restrict)
			pn = vocab.PrimaryName(ident='', content=title)
			if lang:
				pn.language = lang
			data['identifiers'].append(pn)

	@staticmethod
	def model_article(data):
		make_la_lo = MakeLinkedArtLinguisticObject()
		make_la_lo(data)

	def __call__(self, data, language_code_map):
		'''
		Given an XML element representing an AATA record, extract information about the
		"article" (this might be a book, chapter, journal article, etc.) including:

		* document type
		* titles and title translations
		* organizations and their role (e.g. publisher)
		* creators and thier role (e.g. author, editor)
		* abstracts
		* languages

		This information is returned in a single `dict`.
		'''

		rid = data['record_id_group']['record_id']
		data['uri'] = self.helper.make_proj_uri('Article', rid)

		self.model_title_group(data, data['title_group'])
		data.setdefault('label', f'Article ({rid})') # this should get overridden in model_title_group)
		self.model_record_desc_group(data, data['record_desc_group'])
		self.model_record_id_group(data, data['record_id_group'])
		self.model_authorship_group(data, data.get('authorship_group'))
		self.model_imprint_group(data, data.get('imprint_group'))
		self.model_physical_desc_group(data, data.get('physical_desc_group'))
		self.model_notes_group(data, data.get('notes_group'))
		self.model_abstract_group(data, data.get('abstract_group'))
		for cg in _as_list(data.get('classification_group')):
			self.model_classification_group(data, cg)
		for ig in _as_list(data.get('index_group')):
			self.model_index_group(data, ig)

		self.add_title(data)
		self.model_article(data)

		return data
