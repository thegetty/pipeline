import warnings
import sys
import pprint
import traceback
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab

import pipeline.execution
from pipeline.projects.sales.util import object_key, object_key_string
from pipeline.util import \
		implode_date, \
		timespan_from_outer_bounds, \
		timespan_before, \
		timespan_after, \
		CaseFoldingSet
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
			warnings.warn(f'*** No place URI found for lot in catalog {cno}')

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
			# In case the lot is reconciled with another lot, notes should not be merged.
			# Therefore, the note URI must not share a prefix with the lot URI, otherwise
			# all notes are liable to be merged during URI reconciliation.
			note_uri = self.helper.prepend_uri_key(lot.id, 'NOTE')
			lot.referred_to_by = vocab.Note(ident=note_uri, content=notes)
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
			pprint.pprint({k: v for k, v in data.items() if v != ''}, stream=sys.stderr)
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
			tx.caused_by = lot
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

	def final_owner_prov_entry(self, tx_label_args, final_owner, current_tx, hmo, current_ts):
		# It's conceivable that there could be more than one "present location" for an
		# object that is reconciled based on prev/post sale rewriting. Therefore, the
		# provenance entry URI must not share a prefix with the object URI, otherwise all
		# such provenance entries are liable to be merged during URI reconciliation as
		# part of the prev/post sale rewriting.
		tx_uri = self.helper.prepend_uri_key(hmo.id, f'PROV,CURROWN')
		tx, acq = self.related_procurement(hmo, tx_label_args, current_tx, current_ts, buyer=final_owner, ident=tx_uri, make_label=prov_entry_label)
		return tx, acq

	def add_transfer_of_custody(self, data, current_tx, xfer_to, xfer_from, buy_sell_modifiers, sequence=1, purpose=None):
		THROUGH = CaseFoldingSet(buy_sell_modifiers['through'])
		FOR = CaseFoldingSet(buy_sell_modifiers['for'])

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
		tx_data = parent.get('_prov_entry_data')
		current_tx = get_crom_object(tx_data)

		# The custody transfer sequence is specific to a single transaction. Therefore,
		# the custody transfer URI must not share a prefix with the object URI, otherwise
		# all such custody transfers are liable to be merged during URI reconciliation as
		# part of the prev/post sale rewriting.
		xfer_id = self.helper.prepend_uri_key(hmo.id, f'CustodyTransfer,{sequence}')
		xfer = model.TransferOfCustody(ident=xfer_id, label=xfer_label)
		xfer.transferred_custody_of = hmo
		if purpose in self.custody_xfer_purposes:
			xfer.general_purpose = self.custody_xfer_purposes[purpose]

		for seller_data in sellers:
			seller = get_crom_object(seller_data)
			mods = self.modifiers(seller_data, 'auth_mod_a')
			if not THROUGH.intersects(mods):
				xfer.transferred_custody_from = seller

		for buyer_data in buyers:
			buyer = get_crom_object(buyer_data)
			mods = self.modifiers(buyer_data, 'auth_mod_a')
			if not THROUGH.intersects(mods):
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

	def modifiers(self, a:dict, key:str):
		mod = a.get(key, '')
		mods = CaseFoldingSet({m.strip() for m in mod.split(';')} - {''})
		return mods

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

		tx_data = parent.get('_prov_entry_data')
		current_tx = get_crom_object(tx_data)

		# The payment URI is just the provenance entry URI with a suffix. In any case
		# where the provenance entry is merged, the payment should be merged as well.
		payment_id = current_tx.id + '-Pay'

		# The acquisition URI is just the provenance entry URI with a suffix. In any case
		# where the provenance entry is merged, the acquisition should be merged as well.
		acq_id = hmo.id + '-Acq'
		acq = model.Acquisition(ident=acq_id, label=acq_label)
		acq.transferred_title_of = hmo

		self.attach_source_catalog(data, acq, buyers + sellers)

		multi = tx_data.get('multi_lot_tx')
		paym_label = f'multiple lots {multi}' if multi else object_label
		paym = model.Payment(ident=payment_id, label=f'Payment for {paym_label}')

		THROUGH = CaseFoldingSet(buy_sell_modifiers['through'])
		FOR = CaseFoldingSet(buy_sell_modifiers['for'])

		single_seller = (len(sellers) == 1)
		single_buyer = (len(buyers) == 1)

		pi = self.helper.person_identity
		def is_or_anon(data:dict):
			mods = self.modifiers(data, 'auth_mod_a')
			return 'or anonymous' in mods
		or_anon_records = [is_or_anon(a) for a in sellers]
		uncertain_attribution = any(or_anon_records)

		for seq_no, seller_data in enumerate(sellers):
			seller = get_crom_object(seller_data)
			mod = self.modifiers(seller_data, 'auth_mod_a')
			attrib_assignment_classes = [model.AttributeAssignment]
			if uncertain_attribution:
				attrib_assignment_classes.append(vocab.PossibleAssignment)
			
			if THROUGH.intersects(mod):
				acq.carried_out_by = seller
				paym.carried_out_by = seller
			elif FOR.intersects(mod):
				acq.transferred_title_from = seller
				paym.paid_to = seller
			elif uncertain_attribution: # this is true if ANY of the sellers have an 'or anonymous' modifier
				# The assignment URIs are just the acquisition URI with a suffix.
				# In any case where the acquisition is merged, the assignments should be
				# merged as well.
				acq_assignment_uri = acq.id + f'-seller-assignment-{seq_no}'
				paym_assignment_uri = paym.id + f'-seller-assignment-{seq_no}'
				
				acq_assignment_label = f'Uncertain seller as previous title holder in acquisition'
				acq_assignment = vocab.PossibleAssignment(ident=acq_assignment_uri, label=acq_assignment_label)
				acq_assignment.referred_to_by = vocab.Note(ident='', content=acq_assignment_label)
				acq_assignment.assigned_property = 'transferred_title_from'
				acq_assignment.assigned = seller
				acq.attributed_by = acq_assignment

				paym_assignment_label = f'Uncertain seller as recipient of payment'
				paym_assignment = vocab.PossibleAssignment(ident=paym_assignment_uri, label=paym_assignment_label)
				paym_assignment.referred_to_by = vocab.Note(ident='', content=paym_assignment_label)
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
			mod = self.modifiers(buyer_data, 'auth_mod_a')

			if 'or' in mod:
				# or/or others/or another
				mod_non_auth = buyer_data.get('auth_mod')
				if mod_non_auth:
					acq.referred_to_by = vocab.Note(ident='', label=f'Buyer modifier', content=mod_non_auth)
				warnings.warn(f'Handle buyer modifier: {mod}') # TODO: some way to model this uncertainty?

			if THROUGH.intersects(mod):
				acq.carried_out_by = buyer
				paym.carried_out_by = buyer
			elif FOR.intersects(mod):
				acq.transferred_title_to = buyer
				paym.paid_from = buyer
			else:
				# covers FOR modifiers and non-modified
				acq.carried_out_by = buyer
				acq.transferred_title_to = buyer
				paym.paid_from = buyer
				paym.carried_out_by = buyer

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
			tx, acq = self.final_owner_prov_entry(tx_label_args, final_owner, current_tx, hmo, ts)
			note = final_owner_data.get('note')
			if note:
				acq.referred_to_by = vocab.Note(ident='', content=note)
			data['_prov_entries'].append(add_crom_data(data={}, what=tx))

		self.add_prev_post_owners(data, hmo, tx_data, sale_type, lot_object_key, ts)
		yield data, current_tx

	def add_prev_post_owners(self, data, hmo, tx_data, sale_type, lot_object_key, ts=None):
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

	def add_sellers(self, data:dict, sale_type, transaction, sellers, rel, source=None):
		hmo = get_crom_object(data)
		parent = data['parent_data']
		auction_data = parent['auction_of_lot']
		lot_object_key = object_key(auction_data)
		cno, lno, date = lot_object_key
		lot = get_crom_object(parent.get('_event_causing_prov_entry'))
		ts = getattr(lot, 'timespan', None)

		prev_procurements = []
		tx_label_args = tuple([self.helper, sale_type, 'Sold', rel] + list(lot_object_key))
		for i, seller_data in enumerate(sellers):
			seller = get_crom_object(seller_data)
			# The provenance entry for a seller's previous acquisition is specific to a
			# single transaction. Therefore, the provenance entry URI must not share a
			# prefix with the object URI, otherwise all such provenance entries are liable
			# to be merged during URI reconciliation as part of the prev/post sale rewriting.
			tx_uri = self.helper.prepend_uri_key(hmo.id, f'PROV,Seller-{i}')
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
		ts = getattr(lot, 'timespan', None)

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
		self.add_transfer_of_custody(data, tx, xfer_to=houses, xfer_from=sellers, buy_sell_modifiers=buy_sell_modifiers, sequence=1, purpose='selling')
		if model_custody_return:
			self.add_transfer_of_custody(data, tx, xfer_to=sellers, xfer_from=houses, buy_sell_modifiers=buy_sell_modifiers, sequence=2, purpose='returning')

		data.setdefault('_prov_entries', [])
		data['_prov_entries'].append(tx_data)

		self.add_prev_post_owners(data, hmo, tx_data, sale_type, lot_object_key, ts)

		if amnts:
			# The bidding for an object is specific to a single transaction. Therefore,
			# the bidding URI must not share a prefix with the object URI, otherwise all
			# such bidding entries are liable to be merged during URI reconciliation as
			# part of the prev/post sale rewriting.
			bidding_id = self.helper.prepend_uri_key(hmo.id, 'BID')
			all_bids_label = f'Bidding on {cno} {lno} ({date})'
			all_bids = model.Activity(ident=bidding_id, label=all_bids_label)
			all_bids.identified_by = model.Name(ident='', content=all_bids_label)
			for tx_data in prev_procurements:
				tx = get_crom_object(tx_data)
				all_bids.starts_after_the_end_of = tx

			all_bids.part_of = lot

			THROUGH = CaseFoldingSet(buy_sell_modifiers['through'])
			FOR = CaseFoldingSet(buy_sell_modifiers['for'])

			for seq_no, amnt in enumerate(amnts):
				# The individual bid and promise URIs are just the bidding URI with a suffix.
				# In any case where the bidding is merged, the bids and promises should be
				# merged as well.
				bid_id = bidding_id + f'-Bid-{seq_no}'
				bid = vocab.Bidding(ident=bid_id)
				prop_id = bidding_id + f'-Bid-{seq_no}-Promise'
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
					mod = self.modifiers(buyer_data, 'auth_mod_a')
					if THROUGH.intersects(mod):
						bid.carried_out_by = buyer
					elif FOR.intersects(mod):
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
				tx, acq = self.final_owner_prov_entry(tx_label_args, final_owner, None, hmo, ts)
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

	def add_mod_notes(self, act, all_mods, label):
		if act and all_mods:
			# Preserve the seller modifier strings as notes on the acquisition/bidding activity
			for mod in all_mods:
				note = vocab.Note(ident='', label=label, content=mod)
				note.classified_as = vocab.instances['qualifier']
				act.referred_to_by = note

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

		data.setdefault('_prov_entries', [])
		data.setdefault('_other_owners', [])

		sellers = [
			self.add_person(
				self.helper.copy_source_information(p, parent),
				record=sales_record,
				relative_id=f'seller_{i+1}',
				catalog_number=cno
			) for i, p in enumerate(parent['seller'])
		]
		all_mods = {m.lower().strip() for a in sellers for m in a.get('auth_mod_a', '').split(';')} - {''}
		seller_group = (all_mods == {'or'}) # the seller is *one* of the named people, model as a group
		
		orig_sellers = sellers
		if seller_group:
			tx_data = parent.get('_prov_entry_data')
			if tx_data: # if there is a prov entry (e.g. was not withdrawn)
				current_tx = get_crom_object(tx_data)
				# The seller group URI is just the provenance entry URI with a suffix.
				# In any case where the provenance entry is merged, the seller group
				# should be merged as well.
				group_uri = current_tx.id + '-SellerGroup'
				group_data = {
					'uri': group_uri,
				}
			else:
				pi_record_no = data['pi_record_no']
				group_uri_key = ('GROUP', 'PI', pi_record_no, 'SellerGroup')
				group_uri = self.helper.make_proj_uri(*group_uri_key)
				group_data = {
					'uri_keys': group_uri_key,
					'uri': group_uri,
				}
			g_label = f'Group containing the seller of {object_key_string(cno, lno, date)}'
			g = vocab.UncertainMemberClosedGroup(ident=group_uri, label=g_label)
			for seller_data in sellers:
				seller = get_crom_object(seller_data)
				seller.member_of = g
				data['_other_owners'].append(seller_data)
			sellers = [add_crom_data({}, g)]

		SOLD = transaction_types['sold']
		UNSOLD = transaction_types['unsold']
		UNKNOWN = transaction_types['unknown']

		sale_type = non_auctions.get(cno, 'Auction')
		data.setdefault('_owner_locations', [])
		if transaction in SOLD:
			for data, current_tx in self.add_acquisition(data, buyers, sellers, non_auctions, buy_sell_modifiers, transaction, transaction_types):
				acq = get_crom_object(data['_acquisition'])
				self.add_mod_notes(acq, all_mods, label=f'Seller modifier')
				houses = [self.helper.add_auction_house_data(h) for h in auction_houses_data.get(cno, [])]
				experts = event_experts.get(cno, [])
				commissaires = event_commissaires.get(cno, [])
				custody_recievers = houses + [add_crom_data(data={}, what=r) for r in experts + commissaires]

				if sale_type in ('Auction', 'Collection Catalog'):
					# 'Collection Catalog' is treated just like an Auction
					self.add_transfer_of_custody(data, current_tx, xfer_to=custody_recievers, xfer_from=sellers, buy_sell_modifiers=buy_sell_modifiers, sequence=1, purpose='selling')
					self.add_transfer_of_custody(data, current_tx, xfer_to=buyers, xfer_from=custody_recievers, buy_sell_modifiers=buy_sell_modifiers, sequence=2, purpose='completing sale')
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

					self.add_transfer_of_custody(data, current_tx, xfer_to=custody_recievers, xfer_from=sellers, buy_sell_modifiers=buy_sell_modifiers, sequence=1, purpose='selling')
					self.add_transfer_of_custody(data, current_tx, xfer_to=buyers, xfer_from=custody_recievers, buy_sell_modifiers=buy_sell_modifiers, sequence=2, purpose='completing sale')

					prev_procurements = self.add_private_sellers(data, sellers, sale_type, transaction, transaction_types)
				else:
					warnings.warn(f'*** not modeling transfer of custody for auction type {transaction}')
				yield data
		elif transaction in UNSOLD:
			houses = [self.helper.add_auction_house_data(h) for h in auction_houses_data.get(cno, [])]
			experts = event_experts.get(cno, [])
			commissaires = event_commissaires.get(cno, [])
			custody_recievers = houses + [add_crom_data(data={}, what=r) for r in experts + commissaires]
			bid_count = 0
			for data in self.add_bidding(data, buyers, sellers, buy_sell_modifiers, sale_type, transaction, transaction_types, custody_recievers):
				bid_count += 1
				act = get_crom_object(data.get('_bidding'))
				self.add_mod_notes(act, all_mods, label=f'Seller modifier')
				yield data
			if not bid_count:
				# there was no bidding, but we still want to model the seller(s) as
				# having previously acquired the object.
				prev_procurements = self.add_non_sale_sellers(data, sellers, sale_type, transaction, transaction_types)
				yield data
		elif transaction in UNKNOWN:
			if sale_type == 'Lottery':
				yield data
			else:
				houses = [
					self.helper.add_auction_house_data(h)
					for h in auction_houses_data.get(cno, [])
				]
				for data in self.add_bidding(data, buyers, sellers, buy_sell_modifiers, sale_type, transaction, transaction_types, houses):
					act = get_crom_object(data.get('_bidding'))
					self.add_mod_notes(act, all_mods, label=f'Seller modifier')
					yield data
		else:
			prev_procurements = self.add_non_sale_sellers(data, sellers, sale_type, transaction, transaction_types)
			lot = get_crom_object(parent['_event_causing_prov_entry'])
			for tx_data in prev_procurements:
				tx = get_crom_object(tx_data)
				lot.starts_after_the_end_of = tx
			warnings.warn(f'Cannot create acquisition data for unrecognized transaction type: {transaction!r}')
			yield data
