import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab

import pipeline.execution
from pipeline.util import implode_date, timespan_from_outer_bounds
from pipeline.util.cleaners import parse_location
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object, remove_crom_object

#mark - Auction Events

class AddAuctionEvent(Configurable):
	helper = Option(required=True)

	def __call__(self, data:dict):
		'''Add modeling for an auction event based on properties of the supplied `data` dict.'''
		cno = data['catalog_number']
		sale_type = data.get('non_auction_flag', 'Auction')
		auction, uid, uri = self.helper.sale_event_for_catalog_number(cno, sale_type)
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

	def auction_event_location(self, data:dict):
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
		return loc

	def __call__(self, data:dict, event_properties):
		'''Add modeling data for an auction event'''
		cno = data['catalog_number']
		auction_locations = event_properties['auction_locations']
		event_experts = event_properties['experts']
		event_commissaires = event_properties['commissaire']
		
		auction = get_crom_object(data)
		catalog = data['_catalog']['_LOD_OBJECT']

		location_data = data['location']
		current = self.auction_event_location(location_data)
		if not current:
			print(f'*** Empty location data: {pprint.pformat(location_data)}')
			pprint.pprint(data)

		# helper.make_place is called here instead of using make_la_place as a separate graph node because the Place object
		# gets stored in the `auction_locations` object to be used in the second graph component
		# which uses the data to associate the place with auction lots.
		base_uri = self.helper.make_proj_uri('AUCTION-EVENT', cno, 'PLACE', '')
		place_data = self.helper.make_place(current, base_uri=base_uri)
		place = get_crom_object(place_data)
		if place:
			data['_locations'] = [place_data]
			auction.took_place_at = place
			auction_locations[cno] = place.clone(minimal=True)

		begin = implode_date(data, 'sale_begin_', clamp='begin')
		end = implode_date(data, 'sale_end_', clamp='eoe')
		ts = timespan_from_outer_bounds(
			begin=begin,
			end=end,
			inclusive=True
		)
		
		event_properties['auction_dates'][cno] = (begin, end)

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

		if begin and end:
			ts.identified_by = model.Name(ident='', content=f'{begin} to {end}')
		elif begin:
			ts.identified_by = model.Name(ident='', content=f'{begin} onwards')
		elif end:
			ts.identified_by = model.Name(ident='', content=f'up to {end}')

		for p in data.get('portal', []):
			url = p['portal_url']
			if url.startswith('http'):
				auction.referred_to_by = vocab.WebPage(ident=url, label=url)
			else:
				warnings.warn(f'*** Portal URL value does not appear to be a valid URL: {url}')

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
			auction.carried_out_by = house
			d['_organizers'].append(h)
		event_properties['auction_houses'][cno] += house_dicts
		return d
