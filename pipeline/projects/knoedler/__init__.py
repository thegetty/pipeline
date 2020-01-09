import re
import os
import json
import sys
import warnings
import uuid
import csv
import pprint
import pathlib
import itertools
import datetime
from collections import Counter, defaultdict, namedtuple
from contextlib import suppress
import inspect
import urllib.parse

import time
import timeit
from sqlalchemy import create_engine
import dateutil.parser

import graphviz
import bonobo
from bonobo.config import use, Option, Service, Configurable
from bonobo.nodes import Limit
from bonobo.constants import NOT_MODIFIED

import settings

from cromulent import model, vocab
from cromulent.extract import extract_monetary_amount

from pipeline.projects import PipelineBase, UtilityHelper
from pipeline.util import \
			truncate_with_ellipsis, \
			implode_date, \
			timespan_from_outer_bounds, \
			GraphListSource, \
			CaseFoldingSet, \
			RecursiveExtractKeyedValue, \
			ExtractKeyedValue, \
			ExtractKeyedValues, \
			MatchingFiles, \
			strip_key_prefix
from pipeline.io.file import MergingFileWriter
from pipeline.io.memory import MergingMemoryWriter
# from pipeline.io.arches import ArchesWriter
import pipeline.linkedart
from pipeline.linkedart import \
			add_crom_data, \
			get_crom_object, \
			MakeLinkedArtRecord, \
			MakeLinkedArtLinguisticObject, \
			MakeLinkedArtHumanMadeObject, \
			MakeLinkedArtAuctionHouseOrganization, \
			MakeLinkedArtOrganization, \
			MakeLinkedArtPerson, \
			make_la_place
from pipeline.io.csv import CurriedCSVReader
from pipeline.nodes.basic import \
			AddFieldNames, \
			GroupRepeatingKeys, \
			GroupKeys, \
			AddArchesModel, \
			Serializer, \
			Trace
from pipeline.util.rewriting import rewrite_output_files, JSONValueRewriter

class PersonIdentity:
	'''
	Utility class to help assign records for people with properties such as `uri` and identifiers.
	'''
	def __init__(self, *, make_uri):
		self.make_uri = make_uri
		self.ignore_authnames = CaseFoldingSet(('NEW', 'NON-UNIQUE'))

	def acceptable_auth_name(self, auth_name):
		if not auth_name or auth_name in self.ignore_authnames:
			return False
		if '[' in auth_name:
			return False
		return True

	def uri_keys(self, data:dict, record_id=None):
		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(data.get('ulan'))

		auth_name = data.get('auth_name')
		auth_name_q = '?' in data.get('auth_nameq', '')

		if ulan:
			return ('PERSON', 'ULAN', ulan)
		elif self.acceptable_auth_name(auth_name):
			return ('PERSON', 'AUTHNAME', auth_name)
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			pi_rec_no = data['pi_record_no']
			if record_id:
				return ('PERSON', 'PI_REC_NO', pi_rec_no, record_id)
			else:
				warnings.warn(f'*** No record identifier given for person identified only by pi_record_number {pi_rec_no}')
				return ('PERSON', 'PI_REC_NO', pi_rec_no)

	def add_uri(self, data:dict, **kwargs):
		keys = self.uri_keys(data, **kwargs)
		data['uri_keys'] = keys
		data['uri'] = self.make_uri(*keys)

	def add_names(self, data:dict, referrer=None, role=None):
		'''
		Based on the presence of `auth_name` and/or `name` fields in `data`, sets the
		`label`, `names`, and `identifier` keys to appropriate strings/`model.Identifier`
		values.

		If the `role` string is given (e.g. 'artist'), also sets the `role_label` key
		to a value (e.g. 'artist “RUBENS, PETER PAUL”').
		'''
		auth_name = data.get('auth_name')
		role_label = None
		if self.acceptable_auth_name(auth_name):
			if role:
				role_label = f'{role} “{auth_name}”'
			data['label'] = auth_name
			pname = vocab.PrimaryName(ident='', content=auth_name) # NOTE: most of these are also vocab.SortName, but not 100%, so witholding that assertion for now
			if referrer:
				pname.referred_to_by = referrer
			data['identifiers'] = [pname]

		name = data.get('name')
		if name:
			if role and not role_label:
				role_label = f'{role} “{name}”'
			if referrer:
				data['names'] = [(name, {'referred_to_by': [referrer]})]
			else:
				data['names'] = [name]
			if 'label' not in data:
				data['label'] = name
		if 'label' not in data:
			data['label'] = '(Anonymous)'

		if role and not role_label:
			role_label = f'anonymous {role}'

		if role:
			data['role_label'] = role_label

class KnoedlerUtilityHelper(UtilityHelper):
	'''
	Project-specific code for accessing and interpreting sales data.
	'''
	def __init__(self, project_name, uid_prefix, static_instances=None):
		super().__init__(project_name)
		self.person_identity = PersonIdentity(make_uri=self.make_proj_uri)
		self.uid_tag_prefix = uid_prefix
		self.static_instances = static_instances
		self.csv_source_columns = ['pi_record_no']
		self.make_la_person = MakeLinkedArtPerson()

	def transaction_uri_for_record(self, data, incoming=False):
		'''
		Return a URI representing the procurement which the object (identified by the
		supplied data) is a part of. This may identify just the object being bought or
		sold or, in the case of multiple objects being bought for a single price, a
		single procurement that encompasses multiple object acquisitions.
		'''
		rec = data['book_record']
		book_id = rec['stock_book_no']
		page_id = rec['page_number']
		row_id = rec['row_number']
		hmo = get_crom_object(data['_object'])
		
		dir = 'In' if incoming else 'Out'
		price = data.get('knoedler_purchase') if incoming else data.get('price')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				print('MULTI OBJECT PURCHASE: ' + self.make_proj_uri('TX-MULTI', dir, n[12:]))
				return self.make_proj_uri('TX-MULTI', dir, n[12:])
		return self.make_proj_uri('TX', dir, book_id, page_id, row_id)

	@staticmethod
	def transaction_multiple_object_label(data, incoming=False):
		price = data.get('knoedler_purchase') if incoming else data.get('price')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				return n[12:]
		return None

	@staticmethod
	def transaction_contains_multiple_objects(data, incoming=False):
		'''
		Return `True` if the procurement related to the supplied data represents a
		transaction of multiple objects with a single payment, `False` otherwise.
		'''
		price = data.get('knoedler_purchase') if incoming else data.get('price')
		if price:
			n = price.get('note')
			if n and n.startswith('for numbers '):
				return True
		return False

	def copy_source_information(self, dst: dict, src: dict):
		for k in self.csv_source_columns:
			with suppress(KeyError):
				dst[k] = src[k]
		return dst

	def knoedler_number_id(self, content):
		k_id = vocab.LocalNumber(ident='', content=content)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Group', 'knoedler')
		k_id.assigned_by = assignment
		return k_id

	def make_uri(self, *values):
		'''Convert a set of identifying `values` into a URI'''
		if values:
			suffix = ','.join([urllib.parse.quote(str(v)) for v in values])
			return self.uid_tag_prefix + suffix
		else:
			suffix = str(uuid.uuid4())
			return self.uid_tag_prefix + suffix

	def add_person(self, data:dict, sales_record, rec_id):
		'''
		Add modeling data for people, based on properties of the supplied `data` dict.

		This function adds properties to `data` before calling
		`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
		'''
		pi = self.person_identity
		pi.add_uri(data, record_id=rec_id)
		pi.add_names(data, referrer=sales_record)

		self.make_la_person(data)
		return data



def add_crom_price(data, parent, services, add_citations=False):
	'''
	Add modeling data for `MonetaryAmount` based on properties of the supplied `data` dict.
	'''
	currencies = services['currencies']
	amnt = extract_monetary_amount(data, currency_mapping=currencies, add_citations=add_citations)
	if amnt:
		add_crom_data(data=data, what=amnt)
	return data


# TODO: copied from provenance.util; refactor
def filter_empty_person(data: dict, _):
	'''
	If all the values of the supplied dictionary are false (or false after int conversion
	for keys ending with 'ulan'), return `None`. Otherwise return the dictionary.
	'''
	set_flags = []
	for k, v in data.items():
		if k.endswith('ulan'):
			if v in ('', '0'):
				s = False
			else:
				s = True
		elif k in ('pi_record_no', 'star_rec_no'):
			s = False
		else:
			s = bool(v)
		set_flags.append(s)
	if any(set_flags):
		return data
	else:
		return None

def record_id(data):
	book = data['stock_book_no']
	page = data['page_number']
	row = data['row_number']
	return (book, page, row)

class AddPersonURI(Configurable):
	helper = Option(required=True)

	def __call__(self, data:dict):
		# TODO: move this into MakeLinkedArtPerson
		auth_name = data.get('authority')
		if data.get('ulan'):
			ulan = data['ulan']
			data['uri'] = self.helper.make_uri('PERSON', 'ULAN', ulan)
			data['ulan'] = ulan
		elif auth_name and '[' not in auth_name:
			data['uri'] = self.helper.make_uri('PERSON', 'AUTHNAME', auth_name)
			data['identifiers'] = [
				vocab.PrimaryName(ident='', content=auth_name)
			]
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			data['uri'] = self.helper.make_uri('PERSON', 'PI_REC_NO', data['parent_data']['pi_record_no'])

		return data

class AddGroupURI(Configurable):
	helper = Option(required=True)

	def __call__(self, data:dict):
		# TODO: move this into MakeLinkedArtOrganization
		auth_name = data.get('authority')
		if data.get('ulan'):
			ulan = data['ulan']
			key = f'GROUP-ULAN-{ulan}'
			data['uri'] = self.helper.make_uri('GROUP', 'ULAN', ulan)
			data['ulan'] = ulan
		elif auth_name and '[' not in auth_name:
			data['uri'] = self.helper.make_uri('GROUP', 'AUTHNAME', auth_name)
			data['identifiers'] = [
				vocab.PrimaryName(ident='', content=auth_name)
			]
		else:
			# not enough information to identify this person uniquely, so use the source location in the input file
			data['uri'] = self.helper.make_uri('GROUP', 'PI_REC_NO', data['pi_record_no'])

		return data

class AddBook(Configurable):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")

	def __call__(self, data:dict, make_la_lo, make_la_hmo):
		book = data['book_record']
		book_id, _, _ = record_id(book)
		data['_physical_book'] = {
			'uri': self.helper.make_uri('Book', book_id),
			'object_type': vocab.Book,
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [self.helper.knoedler_number_id(book_id)],
		}
		make_la_hmo(data['_physical_book'])

		data['_text_book'] = {
			'uri': self.helper.make_uri('Text', 'Book', book_id),
			'object_type': vocab.AccountBookText,
			'label': f'Knoedler Stock Book {book_id}',
			'identifiers': [self.helper.knoedler_number_id(book_id)],
			'carried_by': [data['_physical_book']]
		}
		make_la_lo(data['_text_book'])

		return data

class AddPage(Configurable):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	make_la_hmo = Service('make_la_hmo')
	static_instances = Option(default="static_instances")

	def __call__(self, data:dict, make_la_lo, make_la_hmo):
		book = data['book_record']
		book_id, page_id, _ = record_id(book)

		d = vocab.SequencePosition()
		d.value = page_id
		d.unit = vocab.instances['numbers']

		data['_physical_page'] = {
			'uri': self.helper.make_uri('Book', book_id, 'Page', page_id),
			'object_type': vocab.Page,
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}',
			'identifiers': [self.helper.knoedler_number_id(book_id)],
			'part_of': [data['_physical_book']],
		}
		make_la_hmo(data['_physical_page'])

		data['_text_page'] = {
			'uri': self.helper.make_uri('Text', 'Book', book_id, 'Page', page_id),
			'object_type': vocab.AccountBookText,
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}',
			'identifiers': [(page_id, vocab.LocalNumber(ident=''))],
			'part_of': [data['_text_book']],
			'carried_by': [data['_physical_page']],
			'dimensions': [d] # TODO: add dimension handling to MakeLinkedArtLinguisticObject
		}
		if data.get('heading'):
			# This is a transcription of the heading of the page
			# Meaning it is part of the page linguistic object
			data['_text_page']['heading'] = data['heading'] # TODO: add heading handling to MakeLinkedArtLinguisticObject
		if data.get('subheading'):
			# Transcription of the subheading of the page
			data['_text_page']['subheading'] = data['subheading'] # TODO: add subheading handling to MakeLinkedArtLinguisticObject
		make_la_lo(data['_text_page'])

		return data

class AddRow(Configurable):
	helper = Option(required=True)
	make_la_lo = Service('make_la_lo')
	static_instances = Option(default="static_instances")

	def __call__(self, data:dict, make_la_lo):
		book = data['book_record']
		book_id, page_id, row_id = record_id(book)

		d = vocab.SequencePosition()
		d.value = row_id
		d.unit = vocab.instances['numbers']

		notes = []
		# TODO: add attributed star record number to row as a LocalNumber
		for k in ('description', 'working_note', 'verbatim_notes'):
			if book.get(k):
				notes.append(vocab.Note(ident='', content=book[k]))

		data['_text_row'] = {
			'uri': self.helper.make_uri('Text', 'Book', book_id, 'Page', page_id, 'Row', row_id),
			'label': f'Knoedler Stock Book {book_id}, Page {page_id}, Row {row_id}',
			'identifiers': [(row_id, vocab.LocalNumber(ident=''))],
			'part_of': [data['_text_page']],
			'dimensions': [d], # TODO: add dimension handling to MakeLinkedArtLinguisticObject
			'referred_to_by': notes,
		}
		make_la_lo(data['_text_row'])

		return data

class AddArtists(Configurable):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')

	def __call__(self, data:dict, *, make_la_person):
		'''Add modeling for artists as people involved in the production of an object'''
		hmo = get_crom_object(data['_object'])

		try:
			hmo_label = f'{hmo._label}'
		except AttributeError:
			hmo_label = 'object'
		event_id = hmo.id + '-Production'
		event = model.Production(ident=event_id, label=f'Production event for {hmo_label}')
		hmo.produced_by = event

		artists = data.get('_artists', [])

		sales_record = get_crom_object(data['_text_row'])
		pi = self.helper.person_identity

		apuri = AddPersonURI(helper=self.helper)
		mlap = MakeLinkedArtPerson()
		for a in artists:
			apuri(a)
			mlap(a)

		for seq_no, a in enumerate(artists):
			pi.add_uri(a, record_id=f'artist-{seq_no+1}')
			pi.add_names(a, referrer=sales_record, role='artist')
			artist_label = a.get('role_label')
			make_la_person(a)
			person = get_crom_object(a)

			subprod_path = self.helper.make_uri_path(*a["uri_keys"])
			subevent_id = event_id + f'-{subprod_path}'
			subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {artist_label}')
			subevent.carried_out_by = person
			event.part = subevent
# 		data['_artists'] = [a for a in artists]
		return data

class PopulateKnoedlerObject(Configurable, pipeline.linkedart.PopulateObject):
	helper = Option(required=True)
	make_la_org = Service('make_la_org')
	vocab_type_map = Service('vocab_type_map')

	def __call__(self, data:dict, *, vocab_type_map, make_la_org):
		odata = data['object']
		title = odata['title']
		typestring = odata.get('object_type', '')
		identifiers = []
		
		apuri = AddPersonURI(helper=self.helper)
		mlap = MakeLinkedArtPerson()
		for a in data.get('_artists', []):
			apuri(a)
			mlap(a)

		data['_object'] = {
			'title': title,
			'identifiers': identifiers,
			'_record': data['_text_row'],
		}
		data['_object'].update({k:v for k,v in odata.items() if k in ('materials', 'dimensions')})

		try:
			knum = odata['knoedler_number']
			uri_key	= ('Object', knum)
			identifiers.append(self.helper.knoedler_number_id(knum))
		except:
			uri_key = ('Object', 'Internal', data['pi_record_no'])
		uri = self.helper.make_uri(*uri_key)
		data['_object']['uri'] = uri
		data['_object']['uri_key'] = uri_key

		if typestring in vocab_type_map:
			clsname = vocab_type_map.get(typestring, None)
			otype = getattr(vocab, clsname)
			data['_object']['object_type'] = otype
		else:
			data['_object']['object_type'] = model.HumanMadeObject

		add_group_uri = AddGroupURI(helper=self.helper)
		consigner = data['consigner']
		if consigner:
			add_group_uri(consigner)
			make_la_org(consigner)
			if 'no' in consigner:
				consigner_num = consigner['no']
				consigner_id = vocab.LocalNumber(ident='', label=f'Consigned number: {consigner_num}', content=consigner_num)
				assignment = model.AttributeAssignment(ident='')
				assignment.carried_out_by = get_crom_object(consigner)
				consigner_id.assigned_by = assignment
				data['_object']['identifiers'].append(consigner_id)
			data['_consigner'] = consigner

		mlao = MakeLinkedArtHumanMadeObject()
		mlao(data['_object'])

		self.populate_object_statements(data['_object'], default_unit='inches')
		return data

class TransactionSwitch:
	'''
	Wrap data values with an index on the transaction field so that different branches
	can be constructed for each transaction type (by using ExtractKeyedValue).
	'''
	def __call__(self, data:dict):
		rec = data['book_record']
		transaction = rec['transaction']
		if transaction in ('Sold', 'Destroyed', 'Stolen', 'Lost'):
			yield {transaction: data}
		else:
			warnings.warn(f'TODO: handle transaction type {transaction}')

class TransactionHandler:
	def _empty_tx(self, data, incoming=False):
		tx_uri = self.helper.transaction_uri_for_record(data, incoming)
		tx = vocab.Procurement(ident=tx_uri)
		return tx

	def _procurement(self, data, date_key, participants, purchase=None, incoming=False):
		date = implode_date(data[date_key])
		rec = data['book_record']
		pi_rec = data['pi_record_no']
		book_id, page_id, row_id = record_id(rec)
		sales_record = get_crom_object(data['_text_row'])

		hmo = get_crom_object(data['_object'])
		object_label = f'“{hmo._label}”'
		
		tx = self._empty_tx(data, incoming)
		tx_uri = tx.id

		dir = 'In' if incoming else 'Out'
		dir_label = 'Knoedler purchase' if incoming else 'Knoedler sale'
		tx_data = add_crom_data(data={'uri': tx_uri}, what=tx)
		self.set_date(tx, data, date_key)

		acq_id = self.helper.make_proj_uri('ACQ', dir, book_id, page_id, row_id)
		acq = model.Acquisition(ident=acq_id)
		if self.helper.transaction_contains_multiple_objects(data, incoming):
			multi_label = self.helper.transaction_multiple_object_label(data, incoming)
			tx._label = f'{dir_label} of multiple objects {multi_label} ({date})'
			acq._label = f'{dir_label} of {pi_rec} ({date})'
		else:
			tx._label = f'{dir_label} of {pi_rec} ({date})'
		acq.transferred_title_of = hmo

		amnt = get_crom_object(purchase)
		paym = None
		if amnt:
			payment_id = tx_uri + '-Payment'
			paym = model.Payment(ident=payment_id, label=f'Payment for {object_label}')
			paym.paid_amount = amnt
			tx.part = paym

		role = 'seller' if incoming else 'buyer'
		people = [
			self.helper.add_person(
				self.helper.copy_source_information(p, data),
				sales_record,
				f'{role}_{i+1}'
			) for i, p in enumerate(participants)
		]

		knoedler = self.helper.static_instances.get_instance('Group', 'knoedler')
		for person_data in people:
			person = get_crom_object(person_data)
			if incoming:
				if paym:
					paym.carried_out_by = person
					paym.paid_to = person
					paym.paid_from = knoedler
				tx_from, tx_to = person, knoedler
			else:
				if paym:
					paym.carried_out_by = knoedler
					paym.paid_to = knoedler
					paym.paid_from = person
				tx_from, tx_to = knoedler, person
			acq.transferred_title_from = tx_from
			acq.transferred_title_to = tx_to

		tx.part = acq

		for k in ('_procurements', '_people'):
			if k not in data:
				data[k] = []
		data['_procurements'].append(tx_data)
		data['_people'].extend(people)
		return tx

	def add_incoming_tx(self, data, make_la_person):
		purch = data.get('purchase')
		return self._procurement(data, 'entry_date', data['purchase_seller'], purch, True)

	def add_outgoing_tx(self, data, make_la_person):
		purch = data.get('purchase')
		return self._procurement(data, 'sale_date', data['sale_buyer'], purch, False)

	@staticmethod
	def set_date(event, data, date_key, date_key_prefix=''):
		'''Associate a timespan with the event.'''
		date = implode_date(data[date_key], date_key_prefix)
		if date:
			begin = implode_date(data[date_key], date_key_prefix, clamp='begin')
			end = implode_date(data[date_key], date_key_prefix, clamp='eoe')
			bounds = [begin, end]
		else:
			bounds = []
		if bounds:
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			event.timespan = ts

class ModelDestruction(Configurable, TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')

	def __call__(self, data:dict, make_la_person):
		rec = data['book_record']
		date = implode_date(data['sale_date'])
		hmo = get_crom_object(data['_object'])

		title = data['_object'].get('title')
		short_title = truncate_with_ellipsis(title, 100) or title
		dest_id = hmo.id + '-Destruction'
		d = model.Destruction(ident=dest_id, label=f'Destruction of “{short_title}”')
		if rec.get('verbatim_notes'):
			d.referred_to_by = vocab.Note(ident='', content=rec['verbatim_notes'])
		hmo.destroyed_by = d

		tx = self.add_incoming_tx(data, make_la_person)
		return data

class ModelTheftOrLoss(Configurable, TransactionHandler):
	helper = Option(required=True)
	make_la_person = Service('make_la_person')

	def __call__(self, data:dict, make_la_person):
		rec = data['book_record']
		pi_rec = data['pi_record_no']
		hmo = get_crom_object(data['_object'])
		tx = self.add_incoming_tx(data, make_la_person)
		tx_out = self._empty_tx(data, incoming=False)

		tx_type = rec['transaction']
		label_type = None
		if tx_type == 'Lost':
			label_type = 'Loss'
			transfer_class = vocab.Loss
		else:
			label_type = 'Theft'
			transfer_class = vocab.Theft

		tx_out._label = f'{label_type} of {pi_rec}'
		tx_out_data = add_crom_data(data={'uri': tx_out.id, 'label': tx_out._label}, what=tx_out)

		title = data['_object'].get('title')
		short_title = truncate_with_ellipsis(title, 100) or title
		theft_id = hmo.id + f'-{label_type}'
		notes = rec.get('verbatim_notes')
		if notes and 'Looted' in notes:
			transfer_class = vocab.Looting
		t = transfer_class(ident=theft_id, label=f'{label_type} of “{short_title}”')
		t.transferred_custody_from = self.helper.static_instances.get_instance('Group', 'knoedler')
		t.transferred_custody_of = hmo

		if notes:
			t.referred_to_by = vocab.Note(ident='', content=notes)

		tx_out.part = t
		
		data['_procurements'].append(tx_out_data)
		return data

class AddSaleAcquisitions(Configurable, TransactionHandler):
	'''
	Add Procurement/Acquisition modeling for a sold object. This includes an acquisition
	TO Knoedler from seller(s), and another acquisition FROM Knoedler to buyer(s).
	'''
	helper = Option(required=True)
	make_la_person = Service('make_la_person')

	def __call__(self, data:dict, make_la_person):
		in_tx = self.add_incoming_tx(data, make_la_person)
		out_tx = self.add_outgoing_tx(data, make_la_person)
		in_tx.ends_before_the_start_of = out_tx
		out_tx.starts_after_the_end_of = in_tx
		yield data

#mark - Knoedler Pipeline class

class KnoedlerPipeline(PipelineBase):
	'''Bonobo-based pipeline for transforming Knoedler data from CSV into JSON-LD.'''
	def __init__(self, input_path, data, **kwargs):
		self.uid_tag_prefix = f'tag:getty.edu,2019:digital:pipeline:knoedler:REPLACE-WITH-UUID#'
		project_name = 'knoedler'

		helper = KnoedlerUtilityHelper(project_name, self.uid_tag_prefix)
		super().__init__(project_name, helper=helper)
		helper.static_instances = self.static_instances
		
		self.graph = None
		self.models = kwargs.get('models', settings.arches_models)
		self.header_file = data['header_file']
		self.files_pattern = data['files_pattern']
		self.limit = kwargs.get('limit')
		self.debug = kwargs.get('debug', False)
		self.input_path = input_path

		fs = bonobo.open_fs(input_path)
		with fs.open(self.header_file, newline='') as csvfile:
			r = csv.reader(csvfile)
			self.headers = [v.lower() for v in next(r)]

	def setup_static_instances(self):
		instances = super().setup_static_instances()

		knoedler_ulan = 500304270
		knoedler_name = 'M. Knoedler & Co.'
		KNOEDLER_URI = self.helper.make_uri('ORGANIZATION', 'ULAN', str(knoedler_ulan))
		knoedler = model.Group(ident=KNOEDLER_URI, label=knoedler_name)
		knoedler.identified_by = vocab.PrimaryName(ident='', content=knoedler_name)
		knoedler.exact_match = model.BaseResource(ident=f'http://vocab.getty.edu/ulan/{knoedler_ulan}')

		instances['Group'].update({
			'knoedler': knoedler
		})
		return instances

	# Set up environment
	def get_services(self):
		'''Return a `dict` of named services available to the bonobo pipeline.'''
		services = super().get_services()
		services.update({
			# to avoid constructing new MakeLinkedArtPerson objects millions of times, this
			# is passed around as a service to the functions and classes that require it.
			'make_la_person': MakeLinkedArtPerson(),
			'make_la_lo': MakeLinkedArtLinguisticObject(),
			'make_la_hmo': MakeLinkedArtHumanMadeObject(),
			'make_la_org': MakeLinkedArtOrganization(),
		})
		return services

	def add_sales_chain(self, graph, records, services, serialize=True):
		'''Add transformation of sales records to the bonobo pipeline.'''
		sales_records = graph.add_chain(
# 			"star_record_no",
# 			"pi_record_no",
			GroupRepeatingKeys(
				drop_empty=True,
				mapping={
					'_artists': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('artist_', x),
						],
						'prefixes': (
							"artist_name",
							"artist_authority",
							"artist_nationality",
							"artist_attribution_mod",
							"artist_attribution_mod_auth",
							"star_rec_no",
							"artist_ulan")},
					'purchase_seller': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('purchase_seller_', x),
						],
						'prefixes': (
							"purchase_seller_name",
							"purchase_seller_loc",
							"purchase_seller_auth_name",
							"purchase_seller_auth_loc",
							"purchase_seller_auth_mod",
							"purchase_seller_ulan",
						)
					},
					'purchase_buyer': {
						'postprocess': [
							filter_empty_person,
							lambda x, _: strip_key_prefix('purchase_buyer_', x),
						],
						'prefixes': (
							"purchase_buyer_own",
							"purchase_buyer_share",
							"purchase_buyer_ulan",
						)
					},
					'prev_own': {
						'prefixes': (
							"prev_own",
							"prev_own_loc",
							"prev_own_ulan",
						)
					},
					'sale_buyer': {
						'prefixes': (
							"sale_buyer_name",
							"sale_buyer_loc",
							"sale_buyer_mod",
							"sale_buyer_auth_name",
							"sale_buyer_auth_addr",
							"sale_buyer_auth_mod",
							"sale_buyer_ulan",
						)
					}
				}
			),
			GroupKeys(mapping={
				'present_location': {
					'postprocess': lambda x, _: strip_key_prefix('present_loc_', x),
					'properties': (
						"present_loc_geog",
						"present_loc_inst",
						"present_loc_acc",
						"present_loc_note",
						"present_loc_ulan",
					)
				},
				'consigner': {
					'postprocess': lambda x, _: strip_key_prefix('consign_', x),
					'properties': (
						"consign_no",
						"consign_name",
						"consign_loc",
						"consign_ulan",
					)
				},
				'object': {
					'properties': (
						"knoedler_number",
						"title",
						"subject",
						"genre",
						"object_type",
						"materials",
						"dimensions",
					)
				},
				'sale_date': {
					'postprocess': lambda x, _: strip_key_prefix('sale_date_', x),
					'properties': (
						"sale_date_year",
						"sale_date_month",
						"sale_date_day",
					)
				},
				'entry_date': {
					'postprocess': lambda x, _: strip_key_prefix('entry_date_', x),
					'properties': (
						"entry_date_year",
						"entry_date_month",
						"entry_date_day",
					)
				},
				'purchase': {
					'postprocess': [lambda x, _: strip_key_prefix('purch_', x), lambda d, p: add_crom_price(d, p, services)],
					'properties': (
						"purch_amount",
						"purch_currency",
						"purch_note",
					)
				},
				'knoedler_purchase': {
					'postprocess': [
						lambda x, _: strip_key_prefix('knoedpurch_', x),
						lambda d, p: add_crom_price(d, p, services),
						lambda x, _: {'amount' if k == 'amt' else k:v for k,v in x.items()}
					],
					'properties': (
						"knoedpurch_amt",
						"knoedpurch_curr",
						"knoedpurch_note",
					)
				},
				'knoedler_share': {
					'properties': (
						"knoedshare_amt",
						"knoedshare_curr",
						"knoedshare_note",
					)
				},
				'price': {
					'properties': (
						"price_amount",
						"price_currency",
						"price_note",
					)
				},
				'book_record': {
					'properties': (
						"stock_book_no",
						"page_number",
						"row_number",
						"description",
						"folio",
						"link",
						"heading",
						"subheading",
						"verbatim_notes",
						"working_note",
						"transaction",
					)
				},
				'post_owner': {
					'properties': (
						"post_owner",
						"post_owner_ulan",
					)
				}
			}),
			_input=records.output
		)

		books = self.add_book_chain(graph, sales_records)
		pages = self.add_page_chain(graph, books)
		rows = self.add_row_chain(graph, pages)
		objects = self.add_object_chain(graph, rows)

		tx = graph.add_chain(
			TransactionSwitch(),
			_input=objects.output
		)

		sale = graph.add_chain(
			ExtractKeyedValue(key='Sold'),
			AddSaleAcquisitions(helper=self.helper),
			_input=tx.output
		)

		destruction = graph.add_chain(
			ExtractKeyedValue(key='Destroyed'),
			ModelDestruction(helper=self.helper),
			_input=tx.output
		)

		theft = graph.add_chain(
			ExtractKeyedValue(key='Stolen'),
			ModelTheftOrLoss(helper=self.helper),
			_input=tx.output
		)

		loss = graph.add_chain(
			ExtractKeyedValue(key='Lost'),
			ModelTheftOrLoss(helper=self.helper),
			_input=tx.output
		)

		for branch in (sale, destruction, theft, loss):
			procurement = graph.add_chain( ExtractKeyedValues(key='_procurements'), _input=branch.output )
			people = graph.add_chain( ExtractKeyedValues(key='_people'), _input=branch.output )
		
			if serialize:
				self.add_serialization_chain(graph, procurement.output, model=self.models['Procurement'])
				self.add_serialization_chain(graph, people.output, model=self.models['Person'])

		return tx

	def add_book_chain(self, graph, sales_records, serialize=True):
		books = graph.add_chain(
# 			add_book,
			AddBook(static_instances=self.static_instances, helper=self.helper),
			_input=sales_records.output
		)
		phys = graph.add_chain(
			ExtractKeyedValue(key='_physical_book'),
			_input=books.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_book'),
			_input=books.output
		)
		if serialize:
			self.add_serialization_chain(graph, phys.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return books

	def add_page_chain(self, graph, books, serialize=True):
		pages = graph.add_chain(
			AddPage(static_instances=self.static_instances, helper=self.helper),
			_input=books.output
		)
		phys = graph.add_chain(
			ExtractKeyedValue(key='_physical_page'),
			_input=pages.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_page'),
			_input=pages.output
		)
		if serialize:
			self.add_serialization_chain(graph, phys.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return pages

	def add_row_chain(self, graph, pages, serialize=True):
		rows = graph.add_chain(
			AddRow(static_instances=self.static_instances, helper=self.helper),
			_input=pages.output
		)
		text = graph.add_chain(
			ExtractKeyedValue(key='_text_row'),
			_input=rows.output
		)
		if serialize:
			self.add_serialization_chain(graph, text.output, model=self.models['LinguisticObject'])
		return rows

	def add_object_chain(self, graph, rows, serialize=True):
		objects = graph.add_chain(
			PopulateKnoedlerObject(helper=self.helper),
			
			AddArtists(helper=self.helper),
			_input=rows.output
		)
		hmos = graph.add_chain( ExtractKeyedValue(key='_object'), _input=objects.output )
		consigners = graph.add_chain( ExtractKeyedValue(key='_consigner'), _input=objects.output )
		artists = graph.add_chain(
			ExtractKeyedValues(key='_artists'),
			_input=objects.output
		)
		if serialize:
			self.add_serialization_chain(graph, hmos.output, model=self.models['HumanMadeObject'])
			self.add_serialization_chain(graph, consigners.output, model=self.models['Group'])
			self.add_serialization_chain(graph, artists.output, model=self.models['Person'])
		return objects

	def _construct_graph(self, services=None):
		'''
		Construct bonobo.Graph object(s) for the entire pipeline.
		'''
		g = bonobo.Graph()

		contents_records = g.add_chain(
			MatchingFiles(path='/', pattern=self.files_pattern, fs='fs.data.knoedler'),
			CurriedCSVReader(fs='fs.data.knoedler', limit=self.limit),
			AddFieldNames(field_names=self.headers),
		)
		sales = self.add_sales_chain(g, contents_records, services, serialize=True)

		self.graph = g

	def get_graph(self, **kwargs):
		'''Return a single bonobo.Graph object for the entire pipeline.'''
		if not self.graph:
			self._construct_graph(**kwargs)

		return self.graph

	def run(self, services=None, **options):
		'''Run the Knoedler bonobo pipeline.'''
		print(f'- Limiting to {self.limit} records per file', file=sys.stderr)
		if not services:
			services = self.get_services(**options)

		print('Running graph...', file=sys.stderr)
		graph = self.get_graph(services=services, **options)
		self.run_graph(graph, services=services)

		print('Serializing static instances...', file=sys.stderr)
		for model, instances in self.static_instances.used_instances().items():
			g = bonobo.Graph()
			nodes = self.serializer_nodes_for_model(model=self.models[model], use_memory_writer=False)
			values = instances.values()
			source = g.add_chain(GraphListSource(values))
			self.add_serialization_chain(g, source.output, model=self.models[model], use_memory_writer=False)
			self.run_graph(g, services={})


class KnoedlerFilePipeline(KnoedlerPipeline):
	'''
	Knoedler pipeline with serialization to files based on Arches model and resource UUID.

	If in `debug` mode, JSON serialization will use pretty-printing. Otherwise,
	serialization will be compact.
	'''
	def __init__(self, input_path, data, **kwargs):
		super().__init__(input_path, data, **kwargs)
		self.writers = []
		self.output_path = kwargs.get('output_path')

	def serializer_nodes_for_model(self, *args, model=None, use_memory_writer=True, **kwargs):
		nodes = []
		if self.debug:
			if use_memory_writer:
				w = MergingMemoryWriter(directory=self.output_path, partition_directories=True, compact=False, model=model)
			else:
				w = MergingFileWriter(directory=self.output_path, partition_directories=True, compact=False, model=model)
			nodes.append(w)
		else:
			if use_memory_writer:
				w = MergingMemoryWriter(directory=self.output_path, partition_directories=True, compact=True, model=model)
			else:
				w = MergingFileWriter(directory=self.output_path, partition_directories=True, compact=True, model=model)
			nodes.append(w)
		self.writers += nodes
		return nodes

	def run(self, **options):
		'''Run the Knoedler bonobo pipeline.'''
		start = timeit.default_timer()
		services = self.get_services(**options)
		super().run(services=services, **options)
		print(f'Pipeline runtime: {timeit.default_timer() - start}', file=sys.stderr)

		count = len(self.writers)
		for seq_no, w in enumerate(self.writers):
			print('[%d/%d] writers being flushed' % (seq_no+1, count))
			if isinstance(w, MergingMemoryWriter):
				w.flush()

		print('====================================================')
		print('Total runtime: ', timeit.default_timer() - start)
