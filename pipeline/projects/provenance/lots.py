import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab

import pipeline.execution
from pipeline.projects.provenance.util import object_key
from pipeline.util import \
		implode_date, \
		timespan_from_outer_bounds, \
		timespan_before, \
		timespan_after, \
		CaseFoldingSet
from pipeline.util.cleaners import parse_location_name
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object

#mark - Auction of Lot

class AddAuctionOfLot(Configurable):
	'''Add modeling data for the auction of a lot of objects.'''

	helper = Option(required=True)
	problematic_records = Service('problematic_records')
	auction_locations = Service('auction_locations')
	auction_houses = Service('auction_houses')
	non_auctions = Service('non_auctions')
	def __init__(self, *args, **kwargs):
		self.lot_cache = {}
		super().__init__(*args, **kwargs)

	@staticmethod
	def set_lot_auction_houses(lot, cno, auction_houses):
		'''Associate the auction house with the auction lot.'''
		houses = auction_houses.get(cno)
		if houses:
			for house in houses:
				lot.carried_out_by = house

	@staticmethod
	def set_lot_location(lot, cno, auction_locations):
		'''Associate the location with the auction lot.'''
		place = auction_locations.get(cno)
		if place:
			lot.took_place_at = place

	@staticmethod
	def set_lot_date(lot, auction_data):
		'''Associate a timespan with the auction lot.'''
		date = implode_date(auction_data, 'lot_sale_')
# 		dates = date_parse(date, delim='-')
# 		if dates:
		if date:
			begin = implode_date(auction_data, 'lot_sale_', clamp='begin')
			end = implode_date(auction_data, 'lot_sale_', clamp='eoe')
			bounds = [begin, end]
		else:
			bounds = []
		if bounds:
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			lot.timespan = ts

	def set_lot_notes(self, lot, auction_data):
		'''Associate notes with the auction lot.'''
		cno, lno, _ = object_key(auction_data)
		auction, _, _ = self.helper.auction_event_for_catalog_number(cno)
		notes = auction_data.get('lot_notes')
		if notes:
			note_id = lot.id + '-LotNotes'
			lot.referred_to_by = vocab.Note(ident=note_id, content=notes)
		if not lno:
			warnings.warn(f'Setting empty identifier on {lot.id}')
		lno = str(lno)
		lot.identified_by = vocab.LotNumber(ident='', content=lno)
		lot.part_of = auction

	def set_lot_objects(self, lot, cno, lno, data):
		'''Associate the set of objects with the auction lot.'''
		coll = vocab.AuctionLotSet(ident=f'{data["uri"]}-Set')
		shared_lot_number = self.helper.shared_lot_number_from_lno(lno)
		coll._label = f'Auction Lot {cno} {shared_lot_number}'
		est_price = data.get('estimated_price')
		if est_price:
			coll.dimension = get_crom_object(est_price)
		start_price = data.get('start_price')
		if start_price:
			coll.dimension = get_crom_object(start_price)

		lot.used_specific_object = coll
		data['_lot_object_set'] = add_crom_data(data={}, what=coll)

	def __call__(self, data, non_auctions, auction_houses, auction_locations, problematic_records):
		'''Add modeling data for the auction of a lot of objects.'''
		ask_price = data.get('ask_price', {}).get('ask_price')
		if ask_price:
			# if there is an asking price/currency, it's a direct sale, not an auction;
			# filter these out from subsequent modeling of auction lots.
			return

		self.helper.copy_source_information(data['_object'], data)

		auction_data = data['auction_of_lot']
		try:
			lot_object_key = object_key(auction_data)
		except Exception as e:
			warnings.warn(f'Failed to compute lot object key from data {auction_data} ({e})')
			pprint.pprint({k: v for k, v in data.items() if v != ''})
			raise
		cno, lno, date = lot_object_key
		if cno in non_auctions:
			# the records in this sales catalog do not represent auction sales, so should
			# be skipped.
			return

		shared_lot_number = self.helper.shared_lot_number_from_lno(lno)
		uid, uri = self.helper.shared_lot_number_ids(cno, lno, date)
		data['uid'] = uid
		data['uri'] = uri

		lot = vocab.Auction(ident=data['uri'])
		lot_id = f'{cno} {shared_lot_number} ({date})'
		lot_object_id = f'{cno} {lno} ({date})'
		lot_label = f'Auction of Lot {lot_id}'
		lot._label = lot_label
		data['lot_id'] = lot_id
		data['lot_object_id'] = lot_object_id

		for problem_key, problem in problematic_records.get('lots', []):
			# TODO: this is inefficient, but will probably be OK so long as the number
			#       of problematic records is small. We do it this way because we can't
			#       represent a tuple directly as a JSON dict key, and we don't want to
			#       have to do post-processing on the services JSON files after loading.
			if tuple(problem_key) == lot_object_key:
				note = model.LinguisticObject(ident='', content=problem)
				note.classified_as = vocab.instances["brief text"]
				note.classified_as = model.Type(
					ident=self.helper.problematic_record_uri,
					label='Problematic Record'
				)
				lot.referred_to_by = note

		self.set_lot_auction_houses(lot, cno, auction_houses)
		self.set_lot_location(lot, cno, auction_locations)
		self.set_lot_date(lot, auction_data)
		self.set_lot_notes(lot, auction_data)
		self.set_lot_objects(lot, cno, lno, data)

		tx_uri = self.helper.transaction_uri_for_lot(auction_data, data.get('price', []))
		lots = self.helper.lots_in_transaction(auction_data, data.get('price', []))
		multi = self.helper.transaction_contains_multiple_lots(auction_data, data.get('price', []))
		tx = vocab.Procurement(ident=tx_uri)
		tx._label = f'Procurement of Lot {cno} {lots} ({date})'
		lot.caused = tx
		tx_data = {'uri': tx_uri}

		if multi:
			tx_data['multi_lot_tx'] = lots
		with suppress(AttributeError):
			tx_data['_date'] = lot.timespan
		data['_procurement_data'] = add_crom_data(data=tx_data, what=tx)

		add_crom_data(data=data, what=lot)
		yield data

class AddAcquisitionOrBidding(Configurable):
	helper = Option(required=True)
	buy_sell_modifiers = Service('buy_sell_modifiers')
	make_la_person = Service('make_la_person')
	transaction_types = Service('transaction_types')

	@staticmethod
	def related_procurement(current_tx, hmo, current_ts=None, buyer=None, seller=None, previous=False):
		'''
		Returns a new `vocab.Procurement` object (and related acquisition) that is temporally
		related to the supplied procurement and associated data. The new procurement is for
		the given object, and has the given buyer and seller (both optional).

		If the `previous` flag is `True`, the new procurement is occurs before `current_tx`,
		and if the timespan `current_ts` is given, has temporal data to that effect. If
		`previous` is `False`, this relationship is reversed.
		'''
		tx = vocab.Procurement()
		if current_tx:
			if previous:
				tx.ends_before_the_start_of = current_tx
			else:
				tx.starts_after_the_end_of = current_tx
		modifier_label = 'Previous' if previous else 'Subsequent'
		try:
			pacq = model.Acquisition(ident='', label=f'{modifier_label} Acquisition of: “{hmo._label}”')
		except AttributeError:
			pacq = model.Acquisition(ident='', label=f'{modifier_label} Acquisition')
		pacq.transferred_title_of = hmo
		if buyer:
			pacq.transferred_title_to = buyer
		if seller:
			pacq.transferred_title_from = seller
		tx.part = pacq
		if current_ts:
			if previous:
				pacq.timespan = timespan_before(current_ts)
			else:
				pacq.timespan = timespan_after(current_ts)
		return tx

	def final_owner_procurement(self, final_owner, current_tx, hmo, current_ts):
		tx = self.related_procurement(current_tx, hmo, current_ts, buyer=final_owner)
		try:
			object_label = hmo._label
			tx._label = f'Procurement leading to the currently known location of “{object_label}”'
		except AttributeError:
			tx._label = f'Procurement leading to the currently known location of object'
		return tx

	def add_acquisition(self, data, buyers, sellers, buy_sell_modifiers, make_la_person=None):
		'''Add modeling of an acquisition as a transfer of title from the seller to the buyer'''
		sales_record = get_crom_object(data['_record'])
		hmo = get_crom_object(data)
		parent = data['parent_data']
	# 	transaction = parent['transaction']
		prices = parent['price']
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)
		data['buyer'] = buyers
		data['seller'] = sellers

		acq_label = None
		try:
			object_label = f'“{hmo._label}”'
			acq_label = f'Acquisition of {cno} {lno} ({date}): {object_label}'
		except AttributeError:
			object_label = '(object)'
			acq_label = f'Acquisition of {cno} {lno} ({date})'
		amnts = [get_crom_object(p) for p in prices]

	# 	if not prices:
	# 		print(f'*** No price data found for {transaction} transaction')

		tx_data = parent['_procurement_data']
		current_tx = get_crom_object(tx_data)
		payment_id = current_tx.id + '-Payment'

		acq_id = hmo.id + '-Acquisition'
		acq = model.Acquisition(ident=acq_id, label=acq_label)
		acq.transferred_title_of = hmo

		multi = tx_data.get('multi_lot_tx')
		paym_label = f'multiple lots {multi}' if multi else object_label
		paym = model.Payment(ident=payment_id, label=f'Payment for {paym_label}')

		THROUGH = set(buy_sell_modifiers['through'])
		FOR = set(buy_sell_modifiers['for'])

		for seller_data in sellers:
			seller = get_crom_object(seller_data)
			mod = seller_data.get('auth_mod_a', '')

			if mod == 'or':
				mod_non_auth = seller_data.get('auth_mod')
				if mod_non_auth:
					acq.referred_to_by = vocab.Note(ident='', label=f'Seller modifier', content=mod_non_auth)
				warnings.warn('Handle OR buyer modifier') # TODO: some way to model this uncertainty?

			if mod in THROUGH:
				acq.carried_out_by = seller
				paym.carried_out_by = seller
			elif mod in FOR:
				acq.transferred_title_from = seller
				paym.paid_to = seller
			else:
				# covers non-modified
				acq.carried_out_by = seller
				acq.transferred_title_from = seller
				paym.carried_out_by = seller
				paym.paid_to = seller

		for buyer_data in buyers:
			buyer = get_crom_object(buyer_data)
			mod = buyer_data.get('auth_mod_a', '')

			if mod == 'or':
				# or/or others/or another
				mod_non_auth = buyer_data.get('auth_mod')
				if mod_non_auth:
					acq.referred_to_by = vocab.Note(ident='', label=f'Buyer modifier', content=mod_non_auth)
				warnings.warn(f'Handle buyer modifier: {mod}') # TODO: some way to model this uncertainty?

			if mod in THROUGH:
				acq.carried_out_by = buyer
				paym.carried_out_by = buyer
			elif mod in FOR:
				acq.transferred_title_to = buyer
				paym.paid_from = buyer
			else:
				# covers non-modified
				acq.carried_out_by = buyer
				acq.transferred_title_to = buyer
				paym.carried_out_by = buyer
				paym.paid_from = buyer

		if len(amnts) > 1:
			warnings.warn(f'Multiple Payment.paid_amount values for object {hmo.id} ({payment_id})')
		for amnt in amnts:
			paym.paid_amount = amnt
			break # TODO: sensibly handle multiplicity in paid amount data

		ts = tx_data.get('_date')
		if ts:
			acq.timespan = ts
		current_tx.part = paym
		current_tx.part = acq
		if '_procurements' not in data:
			data['_procurements'] = []
		data['_procurements'] += [add_crom_data(data={}, what=current_tx)]
	# 	lot_uid, lot_uri = helper.shared_lot_number_ids(cno, lno)
		# TODO: `annotation` here is from add_physical_catalog_objects
	# 	paym.referred_to_by = annotation

		data['_acquisition'] = add_crom_data(data={'uri': acq_id}, what=acq)

		final_owner_data = data.get('_final_org', [])
		if final_owner_data:
			data['_organizations'].append(final_owner_data)
			final_owner = get_crom_object(final_owner_data)
			tx = self.final_owner_procurement(final_owner, current_tx, hmo, ts)
			data['_procurements'].append(add_crom_data(data={}, what=tx))

		post_own = data.get('post_owner', [])
		prev_own = data.get('prev_owner', [])
		prev_post_owner_records = [(post_own, False), (prev_own, True)]
		for owner_data, rev in prev_post_owner_records:
			if rev:
				rev_name = 'prev-owner'
				source_label = 'Source of information on history of the object prior to the current sale.'
			else:
				rev_name = 'post-owner'
				source_label = 'Source of information on history of the object after the current sale.'
			for seq_no, owner_record in enumerate(owner_data):
				ignore_fields = ('own_so', 'own_auth_l', 'own_auth_d')
				if not any([bool(owner_record.get(k)) for k in owner_record.keys() if k not in ignore_fields]):
					# some records seem to have metadata (source information, location, or notes) but no other fields set
					# these should not constitute actual records of a prev/post owner.
					continue
				owner_record.update({
					'pi_record_no': data['pi_record_no'],
					'ulan': owner_record['own_ulan'],
					'auth_name': owner_record['own_auth'],
					'name': owner_record['own']
				})
				pi = self.helper.person_identity
				pi.add_uri(owner_record, record_id=f'{rev_name}-{seq_no+1}')
				pi.add_names(owner_record, referrer=sales_record, role='artist')
				make_la_person(owner_record)
				owner = get_crom_object(owner_record)

				# TODO: handle other fields of owner_record: own_auth_d, own_auth_q, own_ques, own_so

				if owner_record.get('own_auth_l'):
					loc = owner_record['own_auth_l']
					current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
					place_data = pipeline.linkedart.make_la_place(current)
					place = get_crom_object(place_data)
					owner.residence = place
					data['_owner_locations'].append(place_data)

				if '_other_owners' not in data:
					data['_other_owners'] = []
				data['_other_owners'].append(owner_record)

				tx = self.related_procurement(current_tx, hmo, ts, buyer=owner, previous=rev)

				own_info_source = owner_record.get('own_so')
				if own_info_source:
					note = vocab.SourceStatement(ident='', content=own_info_source, label=source_label)
					tx.referred_to_by = note

				ptx_data = tx_data.copy()
				data['_procurements'].append(add_crom_data(data=ptx_data, what=tx))
		yield data

	def add_bidding(self, data:dict, buyers, buy_sell_modifiers):
		'''Add modeling of bids that did not lead to an acquisition'''
		parent = data['parent_data']
		prices = parent['price']
		amnts = [get_crom_object(p) for p in prices]

		if amnts:
			auction_data = parent['auction_of_lot']
			cno, lno, date = object_key(auction_data)
			lot = get_crom_object(parent)
			hmo = get_crom_object(data)
			bidding_id = hmo.id + '-Bidding'
			all_bids = model.Activity(ident=bidding_id, label=f'Bidding on {cno} {lno} ({date})')

			all_bids.part_of = lot

			THROUGH = set(buy_sell_modifiers['through'])
			FOR = set(buy_sell_modifiers['for'])

			for seq_no, amnt in enumerate(amnts):
				bid_id = hmo.id + f'-Bid-{seq_no}'
				bid = vocab.Bidding(ident=bid_id)
				prop_id = hmo.id + f'-Bid-{seq_no}-Promise'
				try:
					amnt_label = amnt._label
					bid._label = f'Bid of {amnt_label} on {cno} {lno} ({date})'
					prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay {amnt_label}')
				except AttributeError:
					bid._label = f'Bid on {cno} {lno} ({date})'
					prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay')

				prop.refers_to = amnt
				bid.created = prop

				# TODO: there are often no buyers listed for non-sold records.
				#       should we construct an anonymous person to carry out the bid?
				for buyer_data in buyers:
					buyer = get_crom_object(buyer_data)
					mod = buyer_data.get('auth_mod_a', '')
					if mod in THROUGH:
						bid.carried_out_by = buyer
					elif mod in FOR:
						warnings.warn(f'buyer modifier {mod} for non-sale bidding: {cno} {lno} {date}')
					else:
						bid.carried_out_by = buyer

				all_bids.part = bid

			final_owner_data = data.get('_final_org')
			if final_owner_data:
				data['_organizations'].append(final_owner_data)
				final_owner = get_crom_object(final_owner_data)
				ts = lot.timespan
				hmo = get_crom_object(data)
				tx = self.final_owner_procurement(final_owner, None, hmo, ts)
				if '_procurements' not in data:
					data['_procurements'] = []
				data['_procurements'].append(add_crom_data(data={}, what=tx))

			data['_bidding'] = {'uri': bidding_id}
			add_crom_data(data=data['_bidding'], what=all_bids)
			yield data
		else:
			warnings.warn(f'*** No price data found for {parent["transaction"]!r} transaction')
			yield data

	def add_person(self, data:dict, sales_record, rec_id, *, make_la_person):
		'''
		Add modeling data for people, based on properties of the supplied `data` dict.

		This function adds properties to `data` before calling
		`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
		'''
		pi = self.helper.person_identity
		pi.add_uri(data, record_id=rec_id)
		pi.add_names(data, referrer=sales_record)

		make_la_person(data)
		return data

	def __call__(self, data:dict, buy_sell_modifiers, make_la_person, transaction_types):
		'''Determine if this record has an acquisition or bidding, and add appropriate modeling'''
		parent = data['parent_data']
		sales_record = get_crom_object(data['_record'])
		transaction = parent['transaction']
		transaction = transaction.replace('[?]', '').rstrip()

		buyers = [
			self.add_person(
				self.helper.copy_source_information(p, parent),
				sales_record,
				f'buyer_{i+1}',
				make_la_person=make_la_person
			) for i, p in enumerate(parent['buyer'])
		]

		SOLD = CaseFoldingSet(transaction_types['sold'])
		UNSOLD = CaseFoldingSet(transaction_types['unsold'])
		if transaction in SOLD:
			sellers = [
				self.add_person(
					self.helper.copy_source_information(p, parent),
					sales_record,
					f'seller_{i+1}',
					make_la_person=make_la_person
				) for i, p in enumerate(parent['seller'])
			]
			data['_owner_locations'] = []
			yield from self.add_acquisition(data, buyers, sellers, buy_sell_modifiers, make_la_person)
		elif transaction in UNSOLD:
			yield from self.add_bidding(data, buyers, buy_sell_modifiers)
		else:
			warnings.warn(f'Cannot create acquisition data for unknown transaction type: {transaction!r}')
