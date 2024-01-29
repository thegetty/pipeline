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
		# import pdb; pdb.set_trace()
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
			# import pdb; pdb.set_trace()
			# check in which type of location the tgn refers to
			tgn_ref = data.get('loc_tgn_ref')
			l = data.get(tgn_ref)
			loc = parse_location_name(l, uri_base=self.helper.proj_prefix)
		return loc

	def __call__(self, data:dict, event_properties, date_modifiers, link_types):
		'''Add modeling data for an auction event'''
		cno = data['catalog_number']
		auction_locations = event_properties['auction_locations']
		event_experts = event_properties['experts']
		event_commissaires = event_properties['commissaire']
		# import pdb; pdb.set_trace()

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
		base_uri = self.helper.make_proj_uri('AUCTION-EVENT', cno, 'PLACE', '')
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
				place_data = self.helper.make_place(current, base_uri=base_uri, record=record)
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
				data['_locations'] = [place_data]
				auction.took_place_at = o_place
				auction_locations[cno] = o_place.clone(minimal=True)

			if same_as:
				# import pdb; pdb.set_trace()
				tgn_instance = self.helper.static_instances.get_instance('Place', same_as)
				if tgn_instance:
					traverse_static_place_instances(self, tgn_instance)
					alternate_exists=False
					for id in tgn_instance.identified_by:
						if isinstance(id, vocab.AlternateName) and id.content == l:
							alternate_exists = True
						
						if not alternate_exists:
							tgn_instance.identified_by = vocab.AlternateName(ident=self.helper.make_shared_uri(('PLACE',current)), content=l)
					
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
				role='expert'
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
				role='commissaire'
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
		return data

class AddAuctionHouses(Configurable):
	helper = Option(required=True)
	event_properties = Service('event_properties')

	def __call__(self, data:dict, event_properties):
		'''
		Add modeling data for the auction house organization(s) associated with an auction
		event.
		'''
		auction = get_crom_object(data)
		event_record = get_crom_object(data['_record'])
		catalog = data['_catalog']['_LOD_OBJECT']
		d = data.copy()
		houses = data.get('auction_house', [])
		cno = data['catalog_number']

		house_dicts = []
		event_record = get_crom_object(data['_record'])
		d['_organizers'] = []
		for i, h in enumerate(houses):
			house_dict = self.helper.copy_source_information(h, data)
			house_dict_copy = house_dict.copy()
			h['_catalog'] = catalog
			self.helper.add_auction_house_data(house_dict, sequence=i, event_record=event_record)
			house_dict_copy['uri'] = house_dict['uri']
			house_dicts.append(house_dict_copy)
			house = get_crom_object(h)
			act = vocab.AuctionHouseActivity(ident='', label=f'Activity of {house._label}')
			act.carried_out_by = house
			auction.part = act
			d['_organizers'].append(h)
		event_properties['auction_houses'][cno] += house_dicts
		return d
