import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab

import pipeline.execution
from pipeline.util import implode_date, timespan_from_outer_bounds
from pipeline.util.cleaners import parse_location
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object

#mark - Auction Events

class AddAuctionEvent(Configurable):
	helper = Option(required=True)

	def __call__(self, data:dict):
		'''Add modeling for an auction event based on properties of the supplied `data` dict.'''
		cno = data['catalog_number']
		auction, uid, uri = self.helper.auction_event_for_catalog_number(cno)
		data['uid'] = uid
		data['uri'] = uri
		add_crom_data(data=data, what=auction)
		catalog = get_crom_object(data['_catalog'])

		record_uri = self.helper.make_proj_uri('AUCTION-EVENT', 'CATALOGNUMBER', cno, 'RECORD')
		label = f'Record of auction event from catalog {cno}'
		record = model.LinguisticObject(ident=record_uri, label=label) # TODO: needs classification
		record_data	= {'uri': record_uri}
		record_data['identifiers'] = [model.Name(ident='', content=label)]
		record.part_of = catalog

		data['_record'] = add_crom_data(data=record_data, what=record)
		return data

class PopulateAuctionEvent(Configurable):
	helper = Option(required=True)
	auction_locations = Service('auction_locations')

	def auction_event_location(self, data:dict):
		'''
		Based on location data in the supplied `data` dict, construct a data structure
		representing a hierarchy of places (e.g. location->city->country), and return it.

		This structure will be suitable for passing to `pipeline.linkedart.make_la_place`
		to construct a Place model object.
		'''
		specific_name = data.get('specific_loc')
		city_name = data.get('city_of_sale')
		country_name = data.get('country_auth')

		parts = [v for v in (specific_name, city_name, country_name) if v is not None]
		loc = parse_location(*parts, uri_base=self.helper.uid_tag_prefix, types=('Place', 'City', 'Country'))
		return loc

	def __call__(self, data:dict, auction_locations):
		'''Add modeling data for an auction event'''
		cno = data['catalog_number']
		auction = get_crom_object(data)
		catalog = data['_catalog']['_LOD_OBJECT']

		location_data = data['location']
		current = self.auction_event_location(location_data)

		# make_la_place is called here instead of as a separate graph node because the Place object
		# gets stored in the `auction_locations` object to be used in the second graph component
		# which uses the data to associate the place with auction lots.
		base_uri = self.helper.make_proj_uri('AUCTION-EVENT', 'CATALOGNUMBER', cno, 'PLACE')
		place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
		place = get_crom_object(place_data)
		if place:
			data['_locations'] = [place_data]
			auction.took_place_at = place
			auction_locations[cno] = place

		begin = implode_date(data, 'sale_begin_', clamp='begin')
		end = implode_date(data, 'sale_end_', clamp='eoe')
		ts = timespan_from_outer_bounds(
			begin=begin,
			end=end,
			inclusive=True
		)
		if begin and end:
			ts.identified_by = model.Name(ident='', content=f'{begin} to {end}')
		elif begin:
			ts.identified_by = model.Name(ident='', content=f'{begin} onwards')
		elif end:
			ts.identified_by = model.Name(ident='', content=f'up to {end}')

		for p in data.get('portal', []):
			url = p['portal_url']
			if url.startswith('http'):
				auction.referred_to_by = vocab.WebPage(ident=url)
			else:
				warnings.warn(f'*** Portal URL value does not appear to be a valid URL: {url}')

		if ts:
			auction.timespan = ts

		auction.referred_to_by = catalog
		return data

class AddAuctionHouses(Configurable):
	helper = Option(required=True)
	auction_houses = Service('auction_houses')

	def add_auction_house_data(self, a, event_record):
		'''Add modeling data for an auction house organization.'''
		catalog = a.get('_catalog')

		ulan = None
		with suppress(ValueError, TypeError):
			ulan = int(a.get('auc_house_ulan'))
		auth_name = a.get('auc_house_auth')
		a['identifiers'] = []
		if ulan:
			key = f'AUCTION-HOUSE-ULAN-{ulan}'
			a['uid'] = key
			a['uri'] = self.helper.make_proj_uri('AUCTION-HOUSE', 'ULAN', ulan)
			a['ulan'] = ulan
			house = vocab.AuctionHouseOrg(ident=a['uri'])
		elif auth_name and auth_name not in self.helper.ignore_house_authnames:
			a['uri'] = self.helper.make_proj_uri('AUCTION-HOUSE', 'AUTHNAME', auth_name)
			pname = vocab.PrimaryName(ident='', content=auth_name)
			pname.referred_to_by = event_record
			a['identifiers'].append(pname)
			house = vocab.AuctionHouseOrg(ident=a['uri'])
		else:
			# not enough information to identify this house uniquely, so use the source location in the input file
			a['uri'] = self.helper.make_proj_uri('AUCTION-HOUSE', 'CAT_NO', 'CATALOG-NUMBER', a['catalog_number'])
			house = vocab.AuctionHouseOrg(ident=a['uri'])

		name = a.get('auc_house_name') or a.get('name')
		if name:
			n = model.Name(ident='', content=name)
			n.referred_to_by = event_record
			a['identifiers'].append(n)
			a['label'] = name
		else:
			a['label'] = '(Anonymous)'

		add_crom_data(data=a, what=house)
		return a

	def __call__(self, data:dict, auction_houses):
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

		house_objects = []
		event_record = get_crom_object(data['_record'])
		for h in houses:
			h['_catalog'] = catalog
			self.add_auction_house_data(self.helper.copy_source_information(h, data), event_record)
			house = get_crom_object(h)
			auction.carried_out_by = house
			if auction_houses:
				house_objects.append(house)
		auction_houses[cno] = house_objects
		return d
