import warnings
import pprint
import traceback
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab
from cromulent.model import factory

from pipeline.util import \
		timespan_before, \
		timespan_after
from pipeline.util.cleaners import parse_location_name
from pipeline.linkedart import add_crom_data, get_crom_object


class ProvenanceBase(Configurable):
	'''
	This is a base class providing common functionality in the handling of Provenance Entries.
	'''
	
	helper = Option(required=True)

	def add_person(self, data:dict, record, relative_id, **kwargs):
		'''
		Add modeling data for people, based on properties of the supplied `data` dict.

		This function adds properties to `data` before calling
		`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
		'''
		self.helper.add_person(data, record=record, relative_id=relative_id, **kwargs)
		return data

	def related_procurement(self, hmo, tx_label_args, current_tx=None, current_ts=None, buyer=None, seller=None, previous=False, ident=None, make_label=None, sales_record=None):
		'''
		Returns a new `vocab.ProvenanceEntry` object (and related acquisition) that is temporally
		related to the supplied procurement and associated data. The new procurement is for
		the given object, and has the given buyer and seller (both optional).

		If the `previous` flag is `True`, the new procurement is occurs before `current_tx`,
		and if the timespan `current_ts` is given, has temporal data to that effect. If
		`previous` is `False`, this relationship is reversed.
		
		The `make_label` argument, if supplied, is used as a function to generate the
		label for the provenance entry. Its arguments (generated in `handle_prev_post_owner`)
		are:
		
		  * helper: the helper object for the current pipeline
		  * sale_type: the sale type passed to `handle_prev_post_owner` (e.g. "Auction")
		  * transaction: the transaction type being handled (e.g. "Sold")
		  * rel: a string describing the relationship between this provenance entry and the object (e.g. "leading to the previous ownership of")
		  * N trailing arguments used that are the contents of the `lot_object_key` tuple passed to `handle_prev_post_owner`
		'''

		def _make_label_default(helper, sale_type, transaction, rel, *args):
			strs = [str(x) for x in args]
			return ', '.join(strs)
		if make_label is None:
			make_label = _make_label_default

		tx = vocab.ProvenanceEntry(ident=ident)
		if sales_record:
			tx.referred_to_by = sales_record
		tx_label = make_label(*tx_label_args)
		tx._label = tx_label
		tx.identified_by = model.Name(ident='', content=tx_label)

		if current_tx:
			if previous:
				tx.ends_before_the_start_of = current_tx
			else:
				tx.starts_after_the_end_of = current_tx
		modifier_label = 'Previous' if previous else 'Subsequent'
		try:
			pacq = model.Acquisition(ident='', label=f'{modifier_label} Acquisition of: “{hmo._label}”')
			pxfer = model.TransferOfCustody(ident='', label=f'{modifier_label} Transfer of Custody of: “{hmo._label}”')
		except AttributeError:
			pacq = model.Acquisition(ident='', label=f'{modifier_label} Acquisition')
			pxfer = model.TransferOfCustody(ident='', label=f'{modifier_label} Transfer of Custody')
		pacq.transferred_title_of = hmo
		pxfer.transferred_custody_of = hmo
		if buyer:
			pacq.transferred_title_to = buyer
			pxfer.transferred_custody_to = buyer
		if seller:
			pacq.transferred_title_from = seller
			pxfer.transferred_custody_from = seller

		tx.part = pacq
		tx.part = pxfer
		if current_ts:
			if previous:
				pacq.timespan = timespan_before(current_ts)
			else:
				pacq.timespan = timespan_after(current_ts)
		return tx, pacq

	def handle_prev_post_owner(self, data, hmo, tx_data, sale_type, lot_object_key, owner_record, record_id, rev, ts=None, make_label=None):
		current_tx = get_crom_object(tx_data)
		sales_record = get_crom_object(data.get('_record', data.get('_text_row')))
		if rev:
			rel = f'leading to the previous ownership of'
			source_label = 'Source of information on history of the object prior to the current sale.'
		else:
			rel = f'leading to the subsequent ownership of'
			source_label = 'Source of information on history of the object after the current sale.'
		owner_record.update({
			'pi_record_no': data['pi_record_no'],
			'ulan': owner_record.get('ulan', owner_record.get('own_ulan')),
		})
		self.add_person(owner_record, record=sales_record, relative_id=record_id, role='artist')
		owner = get_crom_object(owner_record)

		# TODO: handle other fields of owner_record: own_auth_d, own_auth_q, own_ques, own_so

		if owner_record.get('own_auth_l'):
			loc = owner_record['own_auth_l']
			current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
			place_data = self.helper.make_place(current)
			place = get_crom_object(place_data)
			owner.residence = place
			data['_owner_locations'].append(place_data)

		if owner_record.get('own_auth_p'):
			content = owner_record['own_auth_p']
			owner.referred_to_by = vocab.Note(ident='', content=content)

		data.setdefault('_other_owners', [])
		data['_other_owners'].append(owner_record)

		# The Provenance Entry URI must not share a prefix with the object URI, otherwise
		# we run the rist of provenance entries being accidentally merged during URI
		# reconciliation as part of the prev/post sale rewriting.
		tx_uri = self.helper.prepend_uri_key(hmo.id, f'PROV-{record_id}')
		tx_label_args = tuple([self.helper, sale_type, 'Sold', rel] + list(lot_object_key))
		tx, _ = self.related_procurement(hmo, tx_label_args, current_tx, ts, buyer=owner, previous=rev, ident=tx_uri, make_label=make_label, sales_record=sales_record)
		if owner_record.get('own_auth_e'):
			content = owner_record['own_auth_e']
			tx.referred_to_by = vocab.Note(ident='', content=content)

		own_info_source = owner_record.get('own_so')
		if own_info_source:
			note = vocab.SourceStatement(ident='', content=own_info_source, label=source_label)
			tx.referred_to_by = note

		ptx_data = tx_data.copy()
		data['_prov_entries'].append(add_crom_data(data=ptx_data, what=tx))

	def set_possible_attribute(self, obj, prop, data):
		value = get_crom_object(data)
		if not value:
			return
		uncertain = data.get('uncertain', False)
		if uncertain:
			assignment = vocab.PossibleAssignment(ident='')
			assignment.assigned_property = prop
			assignment.assigned = value
			obj.attributed_by = assignment
		else:
			setattr(obj, prop, value)
