import pprint
import warnings
from contextlib import suppress

from bonobo.config import Configurable, Service, Option

from cromulent import model, vocab
from pipeline.util import _as_list
from pipeline.linkedart import \
			MakeLinkedArtAbstract, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtPerson, \
			get_crom_object, \
			add_crom_data

class ModelSeries(Configurable):
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

	def model_series_group(self, record, data):
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

		if start_year:
			record['_publishing_start_year'] = start_year
		if cease_year:
			record['_publishing_cease_year'] = cease_year

		if issn:
			record['identifiers'].append(vocab.IssnIdentifier(ident='', content=issn))

		if coden:
			record['identifiers'].append(vocab.CodenIdentifier(ident='', content=coden))

	def model_publisher_group(self, record, data, seq):
		record.setdefault('_publishings', [])

		series_label = record['label']
		corp_id = data.get('gaia_corp_id')
		geog_id = data.get('gaia_geog_id')
		
		a_uri = record['uri'] + f'-pub-{seq}'
		cb_label = f' by CB{corp_id}' if corp_id else f' by publisher #{seq}'
		a = vocab.Publishing(ident=a_uri, label=f'Publishing of {series_label}' + cb_label)
		if corp_id:
			uri = self.helper.corporate_body_uri(corp_id)
			a.carried_out_by = model.Group(ident=uri)
		if geog_id:
			uri = self.helper.place_uri(geog_id)
			a.took_place_at = model.Place(ident=uri)
		record['_publishings'].append(a)

	def model_sponsor_group(self, record, data):
		# TODO:
		# sponsor_group/ ???
		pass

	def model_publishing(self, data):
		series_label = data['label']
		a_uri = data['uri'] + f'-pub'
		a = vocab.Publishing(ident=a_uri, label=f'Publishing of {series_label}')
		start_year = data.get('_publishing_start_year')
		cease_year = data.get('_publishing_cease_year')
		if start_year or cease_year:
			ts = model.TimeSpan(ident='')
			if start_year:
				with suppress(ValueError):
					year = int(start_year)
					ts.begin_of_the_begin = '%04d-01-01:00:00:00Z' % (year,)
			if cease_year:
				with suppress(ValueError):
					year = int(cease_year)
					ts.end_of_the_end = '%04d-01-01:00:00:00Z' % (year+1,)
			a.timespan = ts

		publishings = data.get('_publishings', [])
		if publishings:
			if len(publishings) > 1:
				print(f'{len(publishings)} publishings of {series_label}')
			series_label = data['label']
			for sub in publishings:
				a.part = sub

		data['used_for'].append(a)

	def model_journal(self, data):
		data.setdefault('used_for', [])
		data['object_type'] = vocab.SeriesText

		self.model_publishing(data)

		mlalo = MakeLinkedArtLinguisticObject()
		mlalo(data)

	def __call__(self, data):
		jid = data['record_desc_group']['record_id']
		print(f'Series: {jid}')
		data['uri'] = self.helper.series_uri(jid)
		
		self.model_record_desc_group(data, data['record_desc_group'])
		self.model_series_group(data, data.get('series_group'))
		data.setdefault('label', f'Series ({jid})')

		for i, pg in enumerate(_as_list(data.get('publisher_group'))):
			self.model_publisher_group(data, pg, i)
		for sg in _as_list(data.get('sponsor_group')):
			self.model_sponsor_group(data, sg)

		self.model_journal(data)
		return data
