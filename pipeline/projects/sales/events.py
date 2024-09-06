import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab
from cromulent.model import factory

import pipeline.execution
from pipeline.util import implode_date, timespan_from_outer_bounds, timespan_from_bound_components, traverse_static_place_instances
from pipeline.util.cleaners import parse_location, parse_location_name
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object, remove_crom_object

#mark - Auction Events

class AddAuctionEvent(Configurable):
	helper = Option(required=True)
	event_properties = Service('event_properties')
	date_modifiers = Service('date_modifiers')

	def select_county(self, data):
		if data['catalog_number'][:2] == "B-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Belgium')
		if data['catalog_number'][:2] == "Br":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_British')
		if data['catalog_number'][:2] == "N-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Dutch')
		if data['catalog_number'][:2] == "F-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_French')
		if data['catalog_number'][:2] == "D-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_German')
		if data['catalog_number'][:2] == "SC":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Sandi')
		
	def __call__(self, data:dict, event_properties, date_modifiers):

		'''Add modeling for an auction event based on properties of the supplied `data` dict.'''
		record = get_crom_object(data['_catalog'])
		cno = data['catalog_number']
		sale_type = data.get('non_auction_flag', 'Auction')
		
		ts, begin, end, uses_following_days_style = timespan_from_bound_components(
			data,
			date_modifiers,
			'sale_begin_', 'begin',
			'sale_end_', 'eoe'
		)
		
		event_properties['auction_dates'][cno] = (ts, begin, end, uses_following_days_style)
		event_properties['auction_date_label'][cno] = ts._label
		
		event_date_label = event_properties['auction_date_label'].get(cno)
		auction, uid, uri = self.helper.sale_event_for_catalog_number(cno, sale_type, date_label=event_date_label)
		auction.referred_to_by = record
		auction.referred_to_by = self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_events')
		auction.referred_to_by = self.select_county(data)
		auction.identified_by = model.Name(ident='', content=auction._label)
		data['uid'] = uid
		data['uri'] = uri
		add_crom_data(data=data, what=auction)
		
		catalog = get_crom_object(data['_catalog'])
		data['_record'] = data['_catalog']
		return data

class PopulateAuctionEvent(Configurable):
	helper = Option(required=True)
	event_properties = Service('event_properties')
	date_modifiers = Service('date_modifiers')
	link_types = Service('link_types')

	def select_county(self, data):
		if data['catalog_number'][:2] == "B-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Belgium')
		if data['catalog_number'][:2] == "Br":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_British')
		if data['catalog_number'][:2] == "N-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Dutch')
		if data['catalog_number'][:2] == "F-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_French')
		if data['catalog_number'][:2] == "D-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_German')
		if data['catalog_number'][:2] == "SC":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Sandi')
		
	def auction_event_location(self, data:dict, tgn_data):
		'''
		Based on location data in the supplied `data` dict, construct a data structure
		representing a hierarchy of places (e.g. location->city->country), and return it.

		This structure will be suitable for passing to
		`pipeline.projects.UtilityHelper.make_place` to construct a Place model object.
		'''
		specific_name = data.get('specific_loc')
		city_name = data.get('city_of_sale')
		place_verbatim = data.get('sale_location')
		country_name = data.get('country_auth')
		loc = None
		if not tgn_data:
			with suppress(IndexError, ValueError, AttributeError):
				if country_name in ('England',):
					# British records sometimes include the county name in the city field
					# attempt to split those here so that the counties can be properly modeled
					city, county = city_name.split(', ', 1)
					if ' ' not in county:
						_values = []
						_types = []
						allvalues = (specific_name, city, county, country_name)
						alltypes = ('Place', 'City', 'County', 'Country')
						for v, t in zip(allvalues, alltypes):
							if v is not None:
								_values.append(v)
								_types.append(t)
						loc = parse_location(*_values, uri_base=self.helper.uid_tag_prefix, types=_types)
			if not loc:
				allvalues = (specific_name, city_name, country_name)
				alltypes = ('Place', 'City', 'Country')
				parts = []
				types = []
				for v, t in zip(allvalues, alltypes):
					if v is not None:
						parts.append(v)
						types.append(t)
				if parts:
					loc = parse_location(*parts, uri_base=self.helper.uid_tag_prefix, types=types)
			if loc and place_verbatim and place_verbatim != city_name:
				if 'part_of' in loc:
					city = loc['part_of']
					city['names'] = [place_verbatim]

		else:
			# check in which type of location the tgn refers to
			tgn_ref = data.get('loc_tgn_ref')
			l = data.get(tgn_ref)
			loc = parse_location_name(l, uri_base=self.helper.proj_prefix)

		return loc

	def __call__(self, data:dict, event_properties, date_modifiers, link_types):
		'''Add modeling data for an auction event'''
		
		if 'specific_loc' in data['location']:
			cno = data['location']['specific_loc']#data['catalog_number']
			if 'same_as' in data['location']['loc_tgn']:
				part = data['location']['loc_tgn']['same_as']
			else: part = data['location']['loc_tgn']['part_of']
		else:
			cno = data['location']['sale_location']
			if 'same_as' in data['location']['loc_tgn']:
				part = data['location']['loc_tgn']['same_as']
			else: part = data['location']['loc_tgn']['part_of']

		auction_locations = event_properties['auction_locations']
		event_experts = event_properties['experts']
		event_commissaires = event_properties['commissaire']
		auction = get_crom_object(data)
		catalog = data['_catalog']['_LOD_OBJECT']

		location_data = data['location']
		tgn_data = location_data.get('loc_tgn', None)

		current = self.auction_event_location(location_data, tgn_data)
		if not current:
			print(f'*** Empty location data: {pprint.pformat(location_data)}')
			pprint.pprint(data)

		# helper.make_place is called here instead of using make_la_place as a separate graph node because the Place object
		# gets stored in the `auction_locations` object to be used in the second graph component
		# which uses the data to associate the place with auction lots.
		base_uri = self.helper.make_proj_uri('PLACE', '')
		record = get_crom_object(data.get('_record'))
		if not tgn_data:
			current_p = current
			locs = []
			while current_p:
				l = current_p.get('name')
				if l:
					locs.append(l)
				current_p = current_p.get('part_of')
			loc = ', '.join(locs) if len(locs) else None
			canonical_place = self.helper.get_canonical_place(loc)
			if canonical_place:
				place = canonical_place
				place_data = add_crom_data(data={'uri': place.id}, what=place)
			else:
				place_data = self.helper.make_place(current, base_uri=base_uri)
				place = get_crom_object(place_data)

			if place:
				data['_locations'] = [place_data]
				auction.took_place_at = place
				auction_locations[cno] = place.clone(minimal=True)
		else:
			l = current.get('name')
			part_of = tgn_data.get("part_of") # this is a tgn id
			same_as = tgn_data.get('same_as') # this is a tgn id
			if part_of:
				tgn_instance = self.helper.static_instances.get_instance('Place', part_of)
				traverse_static_place_instances(self, tgn_instance)
				place_data = self.helper.make_place(current, base_uri=base_uri)
				o_place = get_crom_object(place_data)
				o_place.part_of = tgn_instance
				#place Database description

				o_place.referred_to_by = self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_events')
				o_place.referred_to_by = self.select_county(data)
				data['_locations'] = [place_data]
				auction.took_place_at = o_place
				auction_locations[cno] = o_place.clone(minimal=True)
			if same_as:
				tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
				if tgn_instance:
					traverse_static_place_instances(self, tgn_instance)
					alternate_exists=False
					for id in tgn_instance.identified_by:
						if isinstance(id, vocab.AlternateName) and id.content == l:
							alternate_exists = True
						
						if not alternate_exists:
							tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',current)), content=l)
					auction.took_place_at = tgn_instance
					# owner_place = tgn_instance
					# sdata['tgn'] = tgn_instance



		ts, begin, end, uses_following_days_style = timespan_from_bound_components(
			data,
			date_modifiers,
			'sale_begin_', 'begin',
			'sale_end_', 'eoe'
		)

		event_record = get_crom_object(data['_record'])
		for seq_no, expert in enumerate(data.get('expert', [])):
			self.helper.copy_source_information(expert, data),
			person = self.helper.add_person(
				expert,
				record=event_record,
				relative_id=f'expert-{seq_no+1}',
				role='expert',
				catalog_number=data['catalog_number']
			)
			event_experts[cno].append(person.clone(minimal=True))
			data['_organizers'].append(add_crom_data(data={}, what=person))
			role_id = '' # self.helper.make_proj_uri('AUCTION-EVENT', cno, 'Expert', seq_no)
			role = vocab.Expert(ident=role_id, label=f'Role of Expert in the event {cno}')
			role.carried_out_by = person
			auction.part = role
		for seq_no, commissaire in enumerate(data.get('commissaire', [])):
			self.helper.copy_source_information(commissaire, data),
			person = self.helper.add_person(
				commissaire,
				record=event_record,
				relative_id=f'commissaire-{seq_no+1}',
				role='commissaire',
				catalog_number=data['catalog_number']
			)
			event_commissaires[cno].append(person.clone(minimal=True))

			data['_organizers'].append(add_crom_data(data={}, what=person))
			
			role_id = '' # self.helper.make_proj_uri('AUCTION-EVENT', cno, 'Commissaire', seq_no)
			role = vocab.CommissairePriseur(ident=role_id, label=f'Role of Commissaire-priseur in the event {cno}')
			role.carried_out_by = person
			
			auction.part = role

		notes = data.get('notes')
		if notes:
			auction.referred_to_by = vocab.Note(ident='', content=notes)

		sellers = { **data.get('auc_copy', {}), **data.get('other_seller', {}) }
		for seller in sellers.values():
			seller_description = vocab.SellerDescription(ident='', content=seller)
			seller_description.referred_to_by = record
			auction.referred_to_by = seller_description
			

		if 'links' in data:
			event_record = get_crom_object(data['_record'])
			links = data['links']
			link_keys = set(links.keys()) - {'portal'}
			for p in links.get('portal', []):
				url = p['portal_url']
				link_data = link_types['portal_url']
				label = link_data.get('label', url)
				description = link_data.get('field-description')
				if url.startswith('http'):
					page = vocab.WebPage(ident='', label=label)
					page._validate_range = False
					page.access_point = [vocab.DigitalObject(ident=url, label=url)]
					if description:
						page.referred_to_by = vocab.Note(ident='', content=description)
					event_record.referred_to_by = page
				else:
					warnings.warn(f'*** Portal URL value does not appear to be a valid URL: {url}')
			for k in link_keys:
				url = links[k]
				link_data = {}
				if k in link_types:
					link_data = link_types[k]
				else:
					warnings.warn(f'Link type not found in link_types mapping table: {k!r}')

				if isinstance(url, str):
					label = link_data.get('label', url)
					description = link_data.get('field-description')
					link_type_cl = getattr(vocab, link_data.get('type'), vocab.WebPage)
					w = link_type_cl(ident='', label=label)
					w._validate_range = False
					w.access_point = [vocab.DigitalObject(ident=url)]
					if description:
						w.referred_to_by = vocab.Note(ident='', content=description)
					event_record.referred_to_by = w
				else:
					print(f'*** not a URL string: {k}: {url}')

		if ts:
			auction.timespan = ts
		auction.referred_to_by = catalog
		#activite database sales
		
		auction.referred_to_by = self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_events')
		auction.referred_to_by = self.select_county(data)
		return data

class AddAuctionHouses(Configurable):
	helper = Option(required=True)
	event_properties = Service('event_properties')

	def select_county(self, data):
		if data['catalog_number'][:2] == "B-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Belgium')
		if data['catalog_number'][:2] == "Br":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_British')
		if data['catalog_number'][:2] == "N-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Dutch')
		if data['catalog_number'][:2] == "F-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_French')
		if data['catalog_number'][:2] == "D-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_German')
		if data['catalog_number'][:2] == "SC":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Sandi')
			
	def create_uncertainty_atribute1(self, seller, agent_seq, label, ident, parent):
		attrib_assignment_classes = [model.AttributeAssignment]
		prod_event = model.Production(ident=seller.id, label=f'Production event for {seller._label}')
		attribute_assignment_id =  self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,Seller-{agent_seq}')
		assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {seller._label}')
		assignment.classified_as = model.Type(ident="http://vocab.getty.edu/aat/300435722", label="Possibly")
		assignment.used_specific_object = get_crom_object(parent['_record'])
		assignment.assigned_property = model.Type(ident=ident, label=label)
		assignment.assigned = seller
		return assignment
		
	def __call__(self, data:dict, event_properties):
		'''
		Add modeling data for the auction house organization(s) associated with an auction
		event.
		'''
		
		auction = get_crom_object(data)
		event_record = get_crom_object(data['_record'])
		catalog = data['_catalog']['_LOD_OBJECT']
		d1 = data.copy()
		
		houses = data.get('auction_house', [])
		cno = data['catalog_number']
		house_dicts = []
		
		d1['_organizers'] = []
		
		for i, h1 in enumerate(houses):
			house_dict = self.helper.copy_source_information(h1, data)
			house_dict_copy = house_dict.copy()
			h1['_catalog'] = catalog
			self.helper.add_auction_house_data(house_dict, sequence=i, event_record=event_record)
			house_dict_copy['uri'] = house_dict['uri']
			house_dicts.append(house_dict_copy)
			house = get_crom_object(h1)
			act = vocab.AuctionHouseActivity(ident='', label=f'Activity of {house._label}')
			act.carried_out_by = house
			if 'identified_by' in house.__dict__:
					
					for k, identified in enumerate(house.__dict__['identified_by']):
						if 'referred_to_by' in  identified.__dict__:
							for j, referred in enumerate(house.__dict__['identified_by'][0].__dict__['referred_to_by']):
								house.referred_to_by = referred
			house.referred_to_by = self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_events')
			house.referred_to_by = self.select_county(data)
			d1['_organizers'].append(h1)		
			
			auction.part = act
			
		#sellers = data.get('seller', [])
		sales_record = get_crom_object(data['_record'])
		
		sellers = [
			self.helper.add_person(
				self.helper.copy_source_information(p, data),
				record=sales_record,
				relative_id=f'seller_{i+1}',
				catalog_number=cno
			) for i, p in enumerate(data['seller'])
		]
		seller_q=data['seller']
		#all_sellers = []
		for agent_seq, seller in enumerate(sellers):
			
			#seller_dict = self.helper.copy_source_information(seller_q, data)
			#seller_dict_copy = seller_dict.copy()
			
			seller_q[agent_seq]['_catalog'] = catalog
			#self.helper.add_auction_house_data(seller_dict, sequence=agent_seq, event_record=event_record)
			#seller_dict_copy['uri'] = seller_dict['uri']
			#all_sellers.append(seller_dict_copy)
			# seller = get_crom_object(seller_q)
			act = vocab.SellerActivity(ident='', label=f'Activity of {seller._label}')
			act.carried_out_by = seller
			auction.part = act
			seller.referred_to_by = self.select_county(data)
			d1['_organizers'].append(seller_q[agent_seq])
			#act.attributed_by = seller

			
			if 'sell_auth_q' in seller_q:
				
				if '?' in  seller_q['sell_auth_q']:
					
					ident="http://www.cidoc-crm.org/cidoc-crm/P14_carried_out_by"
					label="carried out by"
					act.attributed_by = self.create_uncertainty_atribute1(seller, agent_seq, label, ident, data)
					
		event_properties['auction_houses'][cno] += house_dicts
		return d1