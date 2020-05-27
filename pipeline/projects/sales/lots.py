import warnings
import pprint
import traceback
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab

import pipeline.execution
from pipeline.projects.sales.util import object_key
from pipeline.util import \
		implode_date, \
		timespan_from_outer_bounds, \
		timespan_before, \
		timespan_after
from pipeline.util.cleaners import parse_location_name
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.provenance import ProvenanceBase

#mark - Auction of Lot

class AddAuctionOfLot(Configurable):
	'''Add modeling data for the auction of a lot of objects.'''

	helper = Option(required=True)
	problematic_records = Service('problematic_records')
	event_properties = Service('event_properties')
	non_auctions = Service('non_auctions')
	transaction_types = Service('transaction_types')
	def __init__(self, *args, **kwargs):
		self.lot_cache = {}
		super().__init__(*args, **kwargs)

	@staticmethod
	def set_lot_auction_houses(lot, cno, auction_houses):
		'''Associate the auction house with the auction lot.'''
		if auction_houses:
			for house in auction_houses:
				lot.carried_out_by = house

	def set_lot_location(self, lot, cno, auction_locations):
		'''Associate the location with the auction lot.'''
		place = auction_locations.get(cno)
		if place:
			lot.took_place_at = place.clone()
		else:
			print(f'*** No place URI found for lot in catalog {cno}')

	@staticmethod
	def set_lot_date(lot, auction_data, event_dates):
		'''Associate a timespan with the auction lot.'''
		date = implode_date(auction_data, 'lot_sale_')
		if date:
			begin = implode_date(auction_data, 'lot_sale_', clamp='begin')
			end = implode_date(auction_data, 'lot_sale_', clamp='eoe')
			bounds = [begin, end]
		else:
			bounds = []

		if bounds:
			if auction_data.get('lot_sale_mod'):
				# if the lot sale date is marked as uncertain:
				#   - use the event end date as the lot sale's end_of_the_end
				#   - if the event doesn't have a known end date, assert no end_of_the_end for the lot sale
				if event_dates and event_dates[1]:
					bounds[1] = event_dates[1]
				else:
					bounds[1] = None
			ts = timespan_from_outer_bounds(*bounds)
			ts.identified_by = model.Name(ident='', content=date)
			lot.timespan = ts

	def set_lot_notes(self, lot, auction_data, sale_type):
		'''Associate notes with the auction lot.'''
		cno, lno, _ = object_key(auction_data)
		notes = auction_data.get('lot_notes')
		if notes:
			note_id = lot.id + '-Notes'
			lot.referred_to_by = vocab.Note(ident=note_id, content=notes)
		if not lno:
			warnings.warn(f'Setting empty identifier on {lot.id}')
		lno = str(lno)
		lot.identified_by = vocab.LotNumber(ident='', content=lno)

	def set_lot_objects(self, lot, cno, lno, auction_of_lot_uri, data, sale_type):
		'''Associate the set of objects with the auction lot.'''
		shared_lot_number = self.helper.shared_lot_number_from_lno(lno)
		set_type = vocab.AuctionLotSet if sale_type == 'Auction' else vocab.CollectionSet
		coll_label = f'Object Set for Lot {cno} {shared_lot_number}'
		coll = set_type(ident=f'{auction_of_lot_uri}-Set', label=coll_label)
		coll.identified_by = model.Name(ident='', content=coll_label)
		est_price = data.get('estimated_price')
		if est_price:
			coll.dimension = get_crom_object(est_price)
		start_price = data.get('start_price')
		if start_price:
			coll.dimension = get_crom_object(start_price)

		ask_price = data.get('ask_price')
		if ask_price:
			coll.dimension = get_crom_object(ask_price)

		lot.used_specific_object = coll
		data['_lot_object_set'] = add_crom_data(data={}, what=coll)

	def __call__(self, data, non_auctions, event_properties, problematic_records, transaction_types):
		'''Add modeling data for the auction of a lot of objects.'''
		self.helper.copy_source_information(data['_object'], data)

		auction_houses_data = event_properties['auction_houses']

		auction_locations = event_properties['auction_locations']
		auction_data = data['auction_of_lot']
		try:
			lot_object_key = object_key(auction_data)
		except Exception as e:
			warnings.warn(f'Failed to compute lot object key from data {auction_data} ({e})')
			pprint.pprint({k: v for k, v in data.items() if v != ''})
			raise
		cno, lno, date = lot_object_key
		sale_type = non_auctions.get(cno, 'Auction')

		ask_price = data.get('ask_price', {}).get('ask_price')
		if ask_price:
			# if there is an asking price/currency, it's a direct sale, not an auction;
			# filter these out from subsequent modeling of auction lots.
			warnings.warn(f'Skipping {cno} {lno} because it asserts an asking price')
			return

		if sale_type != 'Auction':
			# the records in this sales catalog do not represent auction sales, so the
			# price data should not be asserted as a sale price, but instead as an
			# asking price.
			with suppress(KeyError):
				prices = data['price']
				del data['price']
				if prices:
					price_data = prices[0]
					price = get_crom_object(price_data)
					if price:
						ma = vocab.add_classification(price, vocab.AskingPrice)
						data['ask_price'] = add_crom_data(price_data, ma)

		shared_lot_number = self.helper.shared_lot_number_from_lno(lno)
		uid, uri = self.helper.shared_lot_number_ids(cno, lno, date)
		sale_data = {
			'uid': uid,
			'uri': uri
		}

		lot = self.helper.sale_for_sale_type(sale_type, lot_object_key)
		data['lot_object_id'] = f'{cno} {lno} ({date})'

		if 'link_to_pdf' in auction_data:
			url = auction_data['link_to_pdf']
			page = vocab.WebPage(ident=url, label=url)
			lot.referred_to_by = page

		for problem_key, problem in problematic_records.get('lots', []):
			# TODO: this is inefficient, but will probably be OK so long as the number
			#       of problematic records is small. We do it this way because we can't
			#       represent a tuple directly as a JSON dict key, and we don't want to
			#       have to do post-processing on the services JSON files after loading.
			if tuple(problem_key) == lot_object_key:
				note = model.LinguisticObject(ident='', content=problem)
				problem_classification = model.Type(
					ident=self.helper.problematic_record_uri,
					label='Problematic Record'
				)
				problem_classification.classified_as = vocab.instances["brief text"]
				note.classified_as = problem_classification
				lot.referred_to_by = note

		cite_content = []
		if data.get('transaction_so'):
			cite_content.append(data['transaction_so'])
		if data.get('transaction_cite'):
			cite_content.append(data['transaction_cite'])
		if cite_content:
			content = ', '.join(cite_content)
			cite = vocab.BibliographyStatement(ident='', content=content, label='Source of transaction type')
			cite.identified_by = model.Name(ident='', content='Source of transaction type')
			lot.referred_to_by = cite

		transaction = data.get('transaction')
		SOLD = transaction_types['sold']
		WITHDRAWN = transaction_types['withdrawn']
		self.set_lot_objects(lot, cno, lno, sale_data['uri'], data, sale_type)
		auction, _, _ = self.helper.sale_event_for_catalog_number(cno, sale_type)
		if transaction not in WITHDRAWN:
			lot.part_of = auction
			event_dates = event_properties['auction_dates'].get(cno)

			auction_houses = [
				get_crom_object(self.helper.add_auction_house_data(h.copy()))
				for h in auction_houses_data.get(cno, [])
			]

			self.set_lot_auction_houses(lot, cno, auction_houses)
			self.set_lot_location(lot, cno, auction_locations)
			self.set_lot_date(lot, auction_data, event_dates)
			self.set_lot_notes(lot, auction_data, sale_type)

			tx_uri = self.helper.transaction_uri_for_lot(auction_data, data)
			lots = self.helper.lots_in_transaction(auction_data, data)
			tx = vocab.ProvenanceEntry(ident=tx_uri)
			tx_label = prov_entry_label(self.helper, sale_type, transaction, 'of', cno, lots, date)
			tx._label = tx_label
			tx.identified_by = model.Name(ident='', content=tx_label)
			lot.caused = tx
			tx_data = {'uri': tx_uri}

			if transaction in SOLD:
				multi = self.helper.transaction_contains_multiple_lots(auction_data, data)
				if multi:
					tx_data['multi_lot_tx'] = lots

			with suppress(AttributeError):
				tx_data['_date'] = lot.timespan
			data['_prov_entry_data'] = add_crom_data(data=tx_data, what=tx)

			data['_event_causing_prov_entry'] = add_crom_data(data=sale_data, what=lot)
		yield data

def prov_entry_label(helper, sale_type, transaction, rel, cno, lots, date):
	transaction_types = helper.services['transaction_types']
	SOLD = transaction_types.get('sold', {'Sold'})
	id = f'{cno} {lots} ({date})'
	if sale_type == 'Auction':
		if transaction in SOLD:
			return f'Sale {rel} {id}'
		else:
			return f'Offer {rel} {id}'
	else:
		return f'Provenance Entry {rel} Lot {cno} {lots} ({date})'

class AddAcquisitionOrBidding(ProvenanceBase):
	non_auctions = Service('non_auctions')
	event_properties = Service('event_properties')
	buy_sell_modifiers = Service('buy_sell_modifiers')
	transaction_types = Service('transaction_types')

	def __init__(self, *args, **kwargs):
		self.custody_xfer_purposes = {
			'selling': vocab.instances['act of selling'],
			'returning': vocab.instances['act of returning'],
			'completing sale': vocab.instances['act of completing sale'],
		}
		super().__init__(*args, **kwargs)

	def _price_note(self, price):
		'''
		For lots with multiple payment records, the first is asserted as the real payment.
		The rest are turned into LinguisticObjects and associated with the payment as
		`referred_to_by`. This function constructs the content for that LinguisticObject,
		containing price, currency, citations, and notes.
		'''
		amnt = get_crom_object(price)
		try:
			value = amnt.value
		except:
			return None

		label = f'{value}'
		if hasattr(amnt, 'currency'):
			currency = amnt.currency
			label = f'{value} {currency._label}'

		notes = []
		cites = []
		if hasattr(amnt, 'referred_to_by'):
			for ref in amnt.referred_to_by:
				content = ref.content
				classification = {c._label for c in ref.classified_as}
				if 'Note' in classification:
					notes.append(content)
				else:
					cites.append(content)

		strings = []
		if notes:
			strings.append(', '.join(notes))
		if cites:
			strings.append(', '.join(cites))
		if strings:
			content = '; '.join(strings)
			label += f'; {content}'
		return label

	def final_owner_procurement(self, tx_label_args, final_owner, current_tx, hmo, current_ts):
		tx_uri = hmo.id + '-FinalOwnerProv'
		tx, acq = self.related_procurement(hmo, tx_label_args, current_tx, current_ts, buyer=final_owner, ident=tx_uri, make_label=prov_entry_label)
		return tx, acq

	def add_transfer_of_custody(self, data, current_tx, xfer_to, xfer_from, sequence=1, purpose=None):
		buyers = xfer_to
		sellers = xfer_from
		hmo = get_crom_object(data)
		parent = data['parent_data']
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)

		xfer_label = None
		purpose_label = f'(for {purpose}) ' if purpose else ''
		try:
			object_label = f'“{hmo._label}”'
			xfer_label = f'Transfer of Custody {purpose_label}of {cno} {lno} ({date}): {object_label}'
		except AttributeError:
			object_label = '(object)'
			xfer_label = f'Transfer of Custody {purpose_label}of {cno} {lno} ({date})'

		# TODO: pass in the tx to use instead of getting it from `parent`
		tx_data = parent['_prov_entry_data']
		current_tx = get_crom_object(tx_data)

		xfer_id = hmo.id + f'-CustodyTransfer-{sequence}'
		xfer = model.TransferOfCustody(ident=xfer_id, label=xfer_label)
		xfer.transferred_custody_of = hmo
		if purpose in self.custody_xfer_purposes:
			xfer.general_purpose = self.custody_xfer_purposes[purpose]

		for seller_data in sellers:
			seller = get_crom_object(seller_data)
			xfer.transferred_custody_from = seller

		for buyer_data in buyers:
			buyer = get_crom_object(buyer_data)
			xfer.transferred_custody_to = buyer

		current_tx.part = xfer

	def attach_source_catalog(self, data, acq, people):
		phys_catalog_notes = {}
		phys_catalogs = {}
		for p in people:
			if '_name_source_catalog_key' in p:
				source_catalog_key = p['_name_source_catalog_key']
				so_cno, so_owner, so_copy = source_catalog_key
				if source_catalog_key in phys_catalog_notes:
					hand_notes = phys_catalog_notes[source_catalog_key]
					catalog = phys_catalogs[source_catalog_key]
				else:
					hand_notes = self.helper.physical_catalog_notes(so_cno, so_owner, so_copy)
					catalog_uri = self.helper.physical_catalog_uri(so_cno, so_owner, so_copy)
					catalog = model.HumanMadeObject(ident=catalog_uri)
					phys_catalog_notes[source_catalog_key] = hand_notes
					phys_catalogs[source_catalog_key] = catalog
					catalog.carries = hand_notes
				acq.referred_to_by = hand_notes
		data['_phys_catalog_notes'] = [add_crom_data(data={}, what=n) for n in phys_catalog_notes.values()]
		data['_phys_catalogs'] = [add_crom_data(data={}, what=c) for c in phys_catalogs.values()]

	def add_acquisition(self, data, buyers, sellers, non_auctions, buy_sell_modifiers, transaction, transaction_types):
		'''Add modeling of an acquisition as a transfer of title from the seller to the buyer'''
		hmo = get_crom_object(data)
		parent = data['parent_data']
	# 	transaction = parent['transaction']
		prices = parent.get('price')
		auction_data = parent['auction_of_lot']
		lot_object_key = object_key(auction_data)
		cno, lno, date = lot_object_key
		sale_type = non_auctions.get(cno, 'Auction')
		data['buyer'] = buyers
		data['seller'] = sellers

		acq_label = None
		try:
			object_label = f'“{hmo._label}”'
			acq_label = f'Acquisition of {cno} {lno} ({date}): {object_label}'
		except AttributeError:
			object_label = '(object)'
			acq_label = f'Acquisition of {cno} {lno} ({date})'

	# 	if not prices:
	# 		print(f'*** No price data found for {transaction} transaction')

		tx_data = parent['_prov_entry_data']
		current_tx = get_crom_object(tx_data)
		payment_id = current_tx.id + '-Pay'

		acq_id = hmo.id + '-Acq'
		acq = model.Acquisition(ident=acq_id, label=acq_label)
		acq.transferred_title_of = hmo

		self.attach_source_catalog(data, acq, buyers + sellers)

		multi = tx_data.get('multi_lot_tx')
		paym_label = f'multiple lots {multi}' if multi else object_label
		paym = model.Payment(ident=payment_id, label=f'Payment for {paym_label}')

		THROUGH = set(buy_sell_modifiers['through'])
		FOR = set(buy_sell_modifiers['for'])

		single_seller = (len(sellers) == 1)
		single_buyer = (len(buyers) == 1)

		for seq_no, seller_data in enumerate(sellers):
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
			elif mod == 'or anonymous':
				acq_assignment = vocab.PossibleAssignment(ident=acq.id + f'-seller-assignment-{seq_no}', label=f'Uncertain seller as previous title holder in acquisition')
				acq_assignment.assigned_property = 'transferred_title_from'
				acq_assignment.assigned = seller
				acq.attributed_by = acq_assignment

				paym_assignment = vocab.PossibleAssignment(ident=paym.id + f'-seller-assignment-{seq_no}', label=f'Uncertain seller as recipient of payment')
				paym_assignment.assigned_property = 'paid_to'
				paym_assignment.assigned = seller
				paym.attributed_by = paym_assignment
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
			else:
				# covers FOR modifiers and non-modified
				acq.transferred_title_to = buyer
				paym.paid_from = buyer

		if prices:
			amnt = get_crom_object(prices[0])
			paym.paid_amount = amnt
			for price in prices[1:]:
				amnt = get_crom_object(price)
				content = self._price_note(price)
				if content:
					paym.referred_to_by = vocab.PriceStatement(ident='', content=content)

		ts = tx_data.get('_date')
		if ts:
			acq.timespan = ts

		current_tx.part = acq
		current_tx.part = paym
		data['_prov_entries'] += [add_crom_data(data={}, what=current_tx)]
	# 	lot_uid, lot_uri = helper.shared_lot_number_ids(cno, lno)
		# TODO: `annotation` here is from add_physical_catalog_objects
	# 	paym.referred_to_by = annotation

		data['_acquisition'] = add_crom_data(data={'uri': acq_id}, what=acq)

		final_owner_data = data.get('_final_org', [])
		if final_owner_data:
			data['_organizations'].append(final_owner_data)
			final_owner = get_crom_object(final_owner_data)
			tx_label_args = tuple([self.helper, sale_type, 'Sold', 'leading to the currently known location of'] + list(lot_object_key))
			tx, acq = self.final_owner_procurement(tx_label_args, final_owner, current_tx, hmo, ts)
			note = final_owner_data.get('note')
			if note:
				acq.referred_to_by = vocab.Note(ident='', content=note)
			data['_prov_entries'].append(add_crom_data(data={}, what=tx))

		post_own = data.get('post_owner', [])
		prev_own = data.get('prev_owner', [])
		prev_post_owner_records = [(post_own, False), (prev_own, True)]
		for owner_data, rev in prev_post_owner_records:
			if rev:
				rev_name = 'prev-owner'
			else:
				rev_name = 'post-owner'
			ignore_fields = {'own_so', 'own_auth_l', 'own_auth_d'}
			for seq_no, owner_record in enumerate(owner_data):
				record_id = f'{rev_name}-{seq_no+1}'
				if not any([bool(owner_record.get(k)) for k in owner_record.keys() if k not in ignore_fields]):
					# some records seem to have metadata (source information, location, or notes)
					# but no other fields set these should not constitute actual records of a prev/post owner.
					continue
				self.handle_prev_post_owner(data, hmo, tx_data, sale_type, lot_object_key, owner_record, record_id, rev, ts, make_label=prov_entry_label)
		yield data, current_tx

	def add_sellers(self, data:dict, sale_type, transaction, sellers, rel, source=None):
		hmo = get_crom_object(data)
		parent = data['parent_data']
		auction_data = parent['auction_of_lot']
		lot_object_key = object_key(auction_data)
		cno, lno, date = lot_object_key
		lot = get_crom_object(parent['_event_causing_prov_entry'])
		ts = getattr(lot, 'timespan', None)

		prev_procurements = []
		tx_label_args = tuple([self.helper, sale_type, 'Sold', rel] + list(lot_object_key))
		for i, seller_data in enumerate(sellers):
			seller = get_crom_object(seller_data)
			tx_uri = hmo.id + f'-seller-{i}-Prov'
			tx, acq = self.related_procurement(hmo, tx_label_args, current_ts=ts, buyer=seller, previous=True, ident=tx_uri, make_label=prov_entry_label)
			self.attach_source_catalog(data, acq, [seller_data])
			if source:
				tx.referred_to_by = source
			prev_procurements.append(add_crom_data(data={}, what=tx))
		data['_prov_entries'] += prev_procurements
		return prev_procurements

	def add_non_sale_sellers(self, data:dict, sellers, sale_type, transaction, transaction_types):
		parent = data['parent_data']
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)

		own_info_source = f'Listed as the seller of object in {cno} {lno} ({date}) that was not sold'
		note = vocab.SourceStatement(ident='', content=own_info_source)
		rel = 'leading to the previous ownership of'
		return self.add_sellers(data, sale_type, transaction, sellers, rel, source=note)

	def add_private_sellers(self, data:dict, sellers, sale_type, transaction, transaction_types):
		parent = data['parent_data']
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)

		own_info_source = f'Listed as the seller of object in {cno} {lno} ({date}) that was privately sold'
		note = vocab.SourceStatement(ident='', content=own_info_source)
		rel = 'leading to the previous ownership of'
		return self.add_sellers(data, sale_type, transaction, sellers, rel, source=note)

	def add_bidding(self, data:dict, buyers, sellers, buy_sell_modifiers, sale_type, transaction, transaction_types, auction_houses_data):
		'''Add modeling of bids that did not lead to an acquisition'''
		hmo = get_crom_object(data)
		parent = data['parent_data']
		data['seller'] = sellers
		auction_data = parent['auction_of_lot']
		lot_object_key = object_key(auction_data)
		cno, lno, date = lot_object_key
		lot_data = parent.get('_event_causing_prov_entry')
		if not lot_data:
			return
		lot = get_crom_object(lot_data)
		if not lot:
			return
		ts = lot.timespan

		UNSOLD = transaction_types['unsold']
		model_custody_return = transaction in UNSOLD
		prev_procurements = self.add_non_sale_sellers(data, sellers, sale_type, transaction, transaction_types)

		prices = parent.get('price', [])
		if not prices:
			yield data
		amnts = [get_crom_object(p) for p in prices]

		tx_data = parent.get('_prov_entry_data')
		tx = get_crom_object(tx_data)
		houses = auction_houses_data
		self.add_transfer_of_custody(data, tx, xfer_to=houses, xfer_from=sellers, sequence=1, purpose='selling')
		if model_custody_return:
			self.add_transfer_of_custody(data, tx, xfer_to=sellers, xfer_from=houses, sequence=2, purpose='returning')

		data.setdefault('_prov_entries', [])
		data['_prov_entries'].append(tx_data)

		if amnts:
			bidding_id = hmo.id + '-Bidding'
			all_bids_label = f'Bidding on {cno} {lno} ({date})'
			all_bids = model.Activity(ident=bidding_id, label=all_bids_label)
			all_bids.identified_by = model.Name(ident='', content=all_bids_label)
			for tx_data in prev_procurements:
				tx = get_crom_object(tx_data)
				all_bids.starts_after_the_end_of = tx

			all_bids.part_of = lot

			THROUGH = set(buy_sell_modifiers['through'])
			FOR = set(buy_sell_modifiers['for'])

			for seq_no, amnt in enumerate(amnts):
				bid_id = hmo.id + f'-Bid-{seq_no}'
				bid = vocab.Bidding(ident=bid_id)
				prop_id = hmo.id + f'-Bid-{seq_no}-Promise'
				try:
					amnt_label = amnt._label
					bid_label = f'Bid of {amnt_label} on {cno} {lno} ({date})'
					prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay {amnt_label}')
				except AttributeError:
					bid_label = f'Bid on {cno} {lno} ({date})'
					prop = model.PropositionalObject(ident=prop_id, label=f'Promise to pay')

				bid._label = bid_label
				bid.identified_by = model.Name(ident='', content=bid_label)
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
				hmo = get_crom_object(data)
				tx_label_args = tuple([self.helper, sale_type, 'Sold', 'leading to the currently known location of'] + list(lot_object_key))
				tx, acq = self.final_owner_procurement(tx_label_args, final_owner, None, hmo, ts)
				note = final_owner_data.get('note')
				if note:
					acq.referred_to_by = vocab.Note(ident='', content=note)
				data['_prov_entries'].append(add_crom_data(data={}, what=tx))

			data['_bidding'] = {'uri': bidding_id}
			add_crom_data(data=data['_bidding'], what=all_bids)
			yield data
		else:
			warnings.warn(f'*** No price data found for {parent["transaction"]!r} transaction')
			yield data

	def __call__(self, data:dict, non_auctions, event_properties, buy_sell_modifiers, transaction_types):
		'''Determine if this record has an acquisition or bidding, and add appropriate modeling'''
		parent = data['parent_data']

		auction_houses_data = event_properties['auction_houses']
		event_experts = event_properties['experts']
		event_commissaires = event_properties['commissaire']

		sales_record = get_crom_object(data['_record'])
		transaction = parent['transaction']
		transaction = transaction.replace('[?]', '').rstrip()
		auction_data = parent['auction_of_lot']
		cno, lno, date = object_key(auction_data)
		shared_lot_number = self.helper.shared_lot_number_from_lno(lno)
		buyers = [
			self.add_person(
				self.helper.copy_source_information(p, parent),
				record=sales_record,
				relative_id=f'buyer_{i+1}',
				catalog_number=cno
			) for i, p in enumerate(parent['buyer'])
		]

		sellers = [
			self.add_person(
				self.helper.copy_source_information(p, parent),
				record=sales_record,
				relative_id=f'seller_{i+1}',
				catalog_number=cno
			) for i, p in enumerate(parent['seller'])
		]

		SOLD = transaction_types['sold']
		UNSOLD = transaction_types['unsold']
		UNKNOWN = transaction_types['unknown']

		data.setdefault('_prov_entries', [])

		sale_type = non_auctions.get(cno, 'Auction')
		if transaction in SOLD:
			data['_owner_locations'] = []
			for data, current_tx in self.add_acquisition(data, buyers, sellers, non_auctions, buy_sell_modifiers, transaction, transaction_types):
				houses = [self.helper.add_auction_house_data(h) for h in auction_houses_data.get(cno, [])]
				experts = event_experts.get(cno, [])
				commissaires = event_commissaires.get(cno, [])
				custody_recievers = houses + [add_crom_data(data={}, what=r) for r in experts + commissaires]

				if sale_type in ('Auction', 'Collection Catalog'):
					# 'Collection Catalog' is treated just like an Auction
					self.add_transfer_of_custody(data, current_tx, xfer_to=custody_recievers, xfer_from=sellers, sequence=1, purpose='selling')
					self.add_transfer_of_custody(data, current_tx, xfer_to=buyers, xfer_from=custody_recievers, sequence=2, purpose='completing sale')
				elif sale_type in ('Private Contract Sale', 'Stock List'):
					# 'Stock List' is treated just like a Private Contract Sale, except for the catalogs
					metadata = {
						'pi_record_no': parent['pi_record_no'],
						'catalog_number': cno
					}
					for i, h in enumerate(custody_recievers):
						house = get_crom_object(h)
						if hasattr(house, 'label'):
							house._label = f'{house._label}, private sale organizer for {cno} {shared_lot_number} ({date})'
						else:
							house._label = f'Private sale organizer for {cno} {shared_lot_number} ({date})'
						data['_organizations'].append(h)

					self.add_transfer_of_custody(data, current_tx, xfer_to=custody_recievers, xfer_from=sellers, sequence=1, purpose='selling')
					self.add_transfer_of_custody(data, current_tx, xfer_to=buyers, xfer_from=custody_recievers, sequence=2, purpose='completing sale')

					prev_procurements = self.add_private_sellers(data, sellers, sale_type, transaction, transaction_types)
				yield data
		elif transaction in UNSOLD:
			houses = [self.helper.add_auction_house_data(h) for h in auction_houses_data.get(cno, [])]
			experts = event_experts.get(cno, [])
			commissaires = event_commissaires.get(cno, [])
			custody_recievers = houses + [add_crom_data(data={}, what=r) for r in experts + commissaires]
			yield from self.add_bidding(data, buyers, sellers, buy_sell_modifiers, sale_type, transaction, transaction_types, custody_recievers)
		elif transaction in UNKNOWN:
			if sale_type == 'Lottery':
				yield data
			else:
				houses = [
					self.helper.add_auction_house_data(h)
					for h in auction_houses_data.get(cno, [])
				]
				yield from self.add_bidding(data, buyers, sellers, buy_sell_modifiers, sale_type, transaction, transaction_types, houses)
		else:
			prev_procurements = self.add_non_sale_sellers(data, sellers, sale_type, transaction, transaction_types)
			lot = get_crom_object(parent['_event_causing_prov_entry'])
			for tx_data in prev_procurements:
				tx = get_crom_object(tx_data)
				lot.starts_after_the_end_of = tx
			warnings.warn(f'Cannot create acquisition data for unrecognized transaction type: {transaction!r}')
			yield data
