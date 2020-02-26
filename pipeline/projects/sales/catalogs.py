import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable, use

from cromulent import model, vocab

from pipeline.linkedart import add_crom_data, get_crom_object

#mark - Physical Catalogs

class AddAuctionCatalog(Configurable):
	helper = Option(required=True)
	non_auctions = Service('non_auctions')
	
	def __call__(self, data:dict, non_auctions):
		'''Add modeling for auction catalogs as linguistic objects'''
		cno = data['catalog_number']

		# this information may either come from `data` (for the auction events branch of the pipeline)
		# or from `non_auctions` (for the catalogs branch, which lacks this information,
		# but will have access to the `non_auctions` service which was shared from the events branch)
		sale_type = non_auctions.get(cno, data.get('non_auction_flag'))
		if sale_type:
			non_auctions[cno] = sale_type
		sale_type = sale_type or 'Auction'
		catalog = self.helper.catalog_text(cno, sale_type)
		cdata = {'uri': catalog.id}
		
		data['_catalog'] = add_crom_data(data=cdata, what=catalog)
		yield data

class AddPhysicalCatalogObjects(Configurable):
	helper = Option(required=True)
	non_auctions = Service('non_auctions')

	def __call__(self, data:dict, non_auctions):
		'''Add modeling for physical copies of an auction catalog'''
		catalog = get_crom_object(data['_catalog'])
		cno = data['catalog_number']
		owner = data['owner_code']
		copy = data['copy_number']
		sale_type = non_auctions.get(cno, 'Auction')
		catalogObject = self.helper.physical_catalog(cno, sale_type, owner, copy)
		data['uri'] = catalogObject.id
		info = data.get('annotation_info')
		if info:
			catalogObject.referred_to_by = vocab.Note(ident='', content=info)
		catalogObject.carries = catalog

		add_crom_data(data=data, what=catalogObject)
		return data

class AddPhysicalCatalogOwners(Configurable):
	helper = Option(required=True)
	location_codes = Service('location_codes')
	unique_catalogs = Service('unique_catalogs')

	def __call__(self, data:dict, location_codes, unique_catalogs):
		'''Add information about the ownership of a physical copy of an auction catalog'''
		# Add the URI of this physical catalog to `unique_catalogs`. This data will be used
		# later to figure out which catalogs can be uniquely identified by a catalog number
		# and owner code (e.g. for owners who do not have multiple copies of a catalog).
		cno = data['catalog_number']
		owner_code = data['owner_code']
		owner_name = None
		with suppress(KeyError):
			owner_name = location_codes[owner_code]
			owner_uri = self.helper.make_proj_uri('ORGANIZATION', 'LOCATION-CODE', owner_code)
			data['_owner'] = {
				'label': owner_name,
				'uri': owner_uri,
				'identifiers': [
					model.Name(ident='', content=owner_name),
					model.Identifier(ident='', content=str(owner_code))
				],
			}
			owner = model.Group(ident=owner_uri)
			add_crom_data(data['_owner'], owner)
			if not owner_code:
				warnings.warn(f'Setting empty identifier on {owner.id}')
			add_crom_data(data=data['_owner'], what=owner)
			catalog = get_crom_object(data)
			catalog.current_owner = owner

		uri = self.helper.make_proj_uri('CATALOG', cno, owner_code, None)
		if uri not in unique_catalogs:
			unique_catalogs[uri] = set()
		unique_catalogs[uri].add(uri)
		return data

#mark - Physical Catalogs - Informational Catalogs

class PopulateAuctionCatalog(Configurable):
	'''Add modeling data for an auction catalog'''
	helper = Option(required=True)
	static_instances = Option(default="static_instances")

	def lugt_number_id(self, content):
		lugt_number = str(content)
		lugt_id = vocab.LocalNumber(ident='', label=f'Lugt Number: {lugt_number}', content=lugt_number)
		assignment = model.AttributeAssignment(ident='')
		assignment.carried_out_by = self.static_instances.get_instance('Person', 'lugt')
		lugt_id.assigned_by = assignment
		return lugt_id

	def __call__(self, data):
		d = {k: v for k, v in data.items()}
		parent = data['parent_data']
		cno = str(parent['catalog_number'])
		sno = parent['star_record_no']
		catalog = get_crom_object(d)
		for lugt_no in parent.get('lugt', {}).values():
			if not lugt_no:
				warnings.warn(f'Setting empty identifier on {catalog.id}')
			catalog.identified_by = self.lugt_number_id(lugt_no)

		if not cno:
			warnings.warn(f'Setting empty identifier on {catalog.id}')
		
		catalog.identified_by = self.helper.gri_number_id(cno)

		if not sno:
			warnings.warn(f'Setting empty identifier on {catalog.id}')
		catalog.identified_by = vocab.SystemNumber(ident='', content=sno)
		notes = data.get('notes')
		if notes:
			note = vocab.Note(ident='', content=parent['notes'])
			catalog.referred_to_by = note
		return d
