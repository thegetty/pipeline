import re
import sys
import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable, use

from cromulent import model, vocab
from cromulent.extract import extract_physical_dimensions

import pipeline.execution
from pipeline.util import implode_date, timespan_from_outer_bounds, CaseFoldingSet
from pipeline.util.cleaners import \
			parse_location_name, \
			date_cleaner
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.util import truncate_with_ellipsis
from pipeline.provenance import ProvenanceBase

#mark - Auction of Lot - Physical Object

class PopulateSalesObject(Configurable, pipeline.linkedart.PopulateObject):
	helper = Option(required=True)
	post_sale_map = Service('post_sale_map')
	unique_catalogs = Service('unique_catalogs')
	subject_genre = Service('subject_genre')
	destruction_types_map = Service('destruction_types_map')
	materials_map = Service('materials_map')
	non_auctions = Service('non_auctions')
	title_modifiers = Service('title_modifiers')
	event_properties = Service('event_properties')
	transaction_classification = Service('transaction_classification')

	def populate_destruction_events(self, data:dict, note, *, type_map, location=None):
		destruction_types_map = type_map
		hmo = get_crom_object(data)
		title = data.get('title')
		short_title = truncate_with_ellipsis(title, 100) or title

		r = re.compile(r'[Dd]estroyed(?: (?:by|during) (\w+))?(?: in (\d{4})[.]?)?')
		m = r.search(note)
		if m:
			method = m.group(1)
			year = m.group(2)
			# The destruction URI is just the object URI with a suffix. When URIs are
			# reconciled during prev/post sale rewriting, this will allow us to also reconcile
			# the URIs for the destructions (of which there should only be one per object)
			dest_uri = hmo.id + '-Destruction'

			d = model.Destruction(ident=dest_uri, label=f'Destruction of “{short_title}”')
			d.referred_to_by = vocab.Note(ident='', content=note)
			if year is not None:
				begin, end = date_cleaner(year)
				ts = timespan_from_outer_bounds(begin, end)
				ts.identified_by = model.Name(ident='', content=year)
				d.timespan = ts

			if method:
				with suppress(KeyError, AttributeError):
					type_name = destruction_types_map[method.lower()]
					otype = vocab.instances[type_name]
					event = model.Event(label=f'{method.capitalize()} event causing the destruction of “{short_title}”')
					event.classified_as = otype
					d.caused_by = event
					data['_events'].append(add_crom_data(data={}, what=event))

			if location:
				current = parse_location_name(location, uri_base=self.helper.uid_tag_prefix)
				# The place URI used for destruction events is based on the object URI with
				# a suffix. When URIs are reconciled during prev/post sale rewriting, this
				# will allow us to also reconcile the URIs for the places of destruction
				# (of which there should only be one hierarchy per object)
				base_uri = hmo.id + '-Destruction-Place,'
				place_data = self.helper.make_place(current, base_uri=base_uri)
				place = get_crom_object(place_data)
				if place:
					data['_locations'].append(place_data)
					d.took_place_at = place

			hmo.destroyed_by = d

	def _populate_object_visual_item(self, data:dict, subject_genre, modified_title, record):
		hmo = get_crom_object(data)
		title = data.get('title')
		title = truncate_with_ellipsis(title, 100) or title

		# The visual item URI is just the object URI with a suffix. When URIs are
		# reconciled during prev/post sale rewriting, this will allow us to also reconcile
		# the URIs for the visual items (of which there should only be one per object)
		vi_uri = hmo.id + '-VisItem'
		vi = model.VisualItem(ident=vi_uri)
		vi.referred_to_by = record
		vidata = {'uri': vi_uri, 'names': [], 'identifiers': []}
		if title:
			vidata['label'] = f'Visual work of “{title}”'
			sales_record = get_crom_object(data['_record'])
			titletype = model.Name if modified_title else vocab.PrimaryName
			t = titletype(ident='', content=title)
			t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
			t.referred_to_by = sales_record
			vidata['identifiers'].append(t)

		if modified_title:
			t = vocab.PrimaryName(ident='', content=modified_title)
			t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
			t.referred_to_by = record
			vidata['identifiers'].append(t)

		for key in ('genre', 'subject'):
			if key in data:
				values = [v.strip() for v in data[key].split(';')]
				for value in values:
					for prop, mapping in subject_genre.items():
						if value in mapping:
							aat_url = mapping[value]
							type = model.Type(ident=aat_url, label=value)
							setattr(vi, prop, type)
		data['_visual_item'] = add_crom_data(data=vidata, what=vi)
		hmo.shows = vi

	def _populate_object_catalog_record(self, data:dict, parent, lot, cno, rec_num, transaction_classification):
		hmo = get_crom_object(data)

		catalog_uri = self.helper.make_proj_uri('CATALOG', cno)
		catalog = vocab.AuctionCatalogText(ident=catalog_uri, label=f'Sale Catalog {cno}')

		record_uri = self.helper.make_proj_uri('CATALOG', cno, 'RECORD', rec_num)
		lot_object_id = parent['lot_object_id']
		
		puid = parent.get('persistent_puid')
		puid_id = self.helper.gpi_number_id(puid)

		record = vocab.EntryTextForm(ident=record_uri, label=f'Sale recorded in catalog: {lot_object_id} (record number {rec_num})')
		transaction = data['parent_data']['transaction']
		tx_cl = transaction_classification.get(transaction)
		if tx_cl:
			label = tx_cl.get('label')
			url = tx_cl.get('url')
			record.about = model.Type(ident=url, label=label)
		else:
			warnings.warn(f'*** No classification found for transaction type: {transaction!r}')

		record_data	= {'uri': record_uri}
		record_data['identifiers'] = [
			model.Name(ident='', content=f'Record of sale {lot_object_id}'),
			puid_id,
		]

		# Some records will have data for which page in the auction catalog
		# the entry appears on. In that case, the record for the entry should
		# be part_of the page. Otherwise, it is part_of the catalog.
		page = data.get('parent_data', {}).get('_text_page')
		if page:
			record.part_of = get_crom_object(page)
		else:
			record.part_of = catalog

		if parent.get('transaction'):
			record.referred_to_by = vocab.PropertyStatusStatement(ident='', label='Transaction type for sales record', content=parent['transaction'])
		record.about = hmo

		data['_record'] = add_crom_data(data=record_data, what=record)
		return record

	def _populate_object_destruction(self, data:dict, parent, destruction_types_map):
		notes = parent.get('auction_of_lot', {}).get('lot_notes')
		if notes and notes.lower().startswith('destroyed'):
			# Issue AR-122 removed modeling of object destruction.
			# self.populate_destruction_events(data, notes, type_map=destruction_types_map)
			pass

	def _populate_object_present_location(self, data:dict, now_key, destruction_types_map):
		hmo = get_crom_object(data)
		locations = data.get('present_location', [])
		for location in locations:
			loc = location.get('geog')
			note = location.get('note')

			# in these two if blocks, the object was destroyed, so any "present location"
			# data is actually an indication of the location of destruction.
			if isinstance(loc, str) and 'destroyed ' in loc.lower():
				# Issue AR-122 removed modeling of object destruction.
				# self.populate_destruction_events(data, loc, type_map=destruction_types_map)
				loc = None
			elif isinstance(note, str) and 'destroyed ' in note.lower():
				# Issue AR-122 removed modeling of object destruction.
				# self.populate_destruction_events(data, note, type_map=destruction_types_map, location=loc)
				note = None

			if loc:
				inst = location.get('inst')
				if inst:
					owner_data = {
						'label': f'{inst} ({loc})',
						'identifiers': [
							model.Name(ident='', content=inst)
						]
					}
					ulan = None
					with suppress(ValueError, TypeError):
						ulan = int(location.get('insi'))
					if ulan:
						owner_data['ulan'] = ulan
						owner_data['uri'] = self.helper.make_proj_uri('ORG', 'ULAN', ulan)
					else:
						owner_data['uri'] = self.helper.make_proj_uri('ORG', 'NAME', inst, 'PLACE', loc)
				else:
					owner_data = {
						'label': '(Anonymous organization)',
						'uri': self.helper.make_proj_uri('ORG', 'CURR-OWN', *now_key),
					}

				# TODO: if `parse_location_name` fails, still preserve the location string somehow
				current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)

				# It's conceivable that there could be more than one "present location"
				# for an object that is reconciled based on prev/post sale rewriting.
				# Therefore, the place URI must not share a prefix with the object URI,
				# otherwise all such places are liable to be merged during URI
				# reconciliation as part of the prev/post sale rewriting.
				base_uri = self.helper.prepend_uri_key(hmo.id, 'PLACE')
				
				canonical_place = self.helper.get_canonical_place(loc)
				if canonical_place:
					place = canonical_place
					place_data = add_crom_data(data={'uri': place.id}, what=place)
				else:
					place_data = self.helper.make_place(current, base_uri=base_uri)
					place = get_crom_object(place_data)
				hmo.current_location = place

				make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
				owner_data = make_la_org(owner_data)
				owner = get_crom_object(owner_data)
				if owner:
					hmo.current_owner = owner
					owner.residence = place

				if note:
					owner_data['note'] = note
					desc = vocab.Description(ident='', content=note)
					if owner:
						assignment = model.AttributeAssignment(ident='')
						assignment.carried_out_by = owner
						desc.assigned_by = assignment
					hmo.referred_to_by = desc

				acc = location.get('acc')
				if acc:
					acc_number = vocab.AccessionNumber(ident='', content=acc)
					hmo.identified_by = acc_number
					assignment = model.AttributeAssignment(ident='')
					assignment.carried_out_by = owner
					acc_number.assigned_by = assignment

				data['_locations'].append(place_data)
				data['_final_org'].append(owner_data)
			else:
				pass # there is no present location place string

	def _populate_object_notes(self, data:dict, parent, unique_catalogs):
		hmo = get_crom_object(data)
		notes = data.get('hand_note', [])
		for note in notes:
			hand_note_content = note['hand_note']
			owner = note.get('hand_note_so')
			cno = parent['auction_of_lot']['catalog_number']
			catalog_uri = self.helper.make_proj_uri('CATALOG', cno, owner, None)
			catalogs = unique_catalogs.get(catalog_uri)
			note = vocab.Note(ident='', content=hand_note_content)
			hmo.referred_to_by = note
			if catalogs and len(catalogs) == 1:
				note.carried_by = vocab.AuctionCatalog(ident=catalog_uri, label=f'Sale Catalog {cno}, owned by “{owner}”')

		inscription = data.get('inscription')
		if inscription:
			hmo.referred_to_by = vocab.InscriptionStatement(ident='', content=inscription)

	def _populate_object_prev_post_sales(self, data:dict, this_key, post_sale_map):
		hmo = get_crom_object(data)
		post_sales = data.get('post_sale', [])
		prev_sales = data.get('prev_sale', [])
		prev_post_sales_records = [(post_sales, False), (prev_sales, True)]
		for sales_data, rev in prev_post_sales_records:
			for sale_record in sales_data:
				pcno = sale_record.get('cat')
				plno = sale_record.get('lot')
# 				plot = self.helper.shared_lot_number_from_lno(plno)
				pdate = implode_date(sale_record, '')
				if pcno and plno and pdate:
					if pcno == 'NA':
						desc = f'Also sold in an unidentified sale: {plno} ({pdate})'
						note = vocab.Note(ident='', content=desc)
						hmo.referred_to_by = note
					elif 'or' in plno.lower():
						desc = f'Also sold in an uncertain lot: {pcno} {plno} ({pdate})'
						note = vocab.Note(ident='', content=desc)
						hmo.referred_to_by = note
					else:
						that_key = (pcno, plno, pdate)
						if rev:
							# `that_key` is for a previous sale for this object
							post_sale_map[this_key] = that_key
						else:
							# `that_key` is for a later sale for this object
							post_sale_map[that_key] = this_key

	def _populate_object_materials(self, data:dict, materials_map):
		hmo = get_crom_object(data)

		otype = data.get('object_type')
		materials = data.get('materials', '')
		if ';' in materials:
			m = frozenset([m.strip() for m in materials.split(';')])
		else:
			m = frozenset([materials])
		material_key = (otype, m)
		if material_key in materials_map:
			mdata = materials_map[material_key]
			materials = set([m for k in ('made_of (primary)', 'made_of (support)') for m in mdata[k].split(';')]) - {''}
			for m in materials:
				aat = int(m)
				sm = self.helper.static_instances.get_instance('Material', m)
				if sm:
					hmo.made_of = sm
				else:
					warnings.warn(f'No static material instance found for AAT value {aat} ibn the materials.json service data')
			classification = set([mdata[k] for k in ('classified_as (object type) (primary)', 'classified_as (object type) (secondary)')])
			technique = mdata['technique']

	def _populate_title_modifier(self, data, title_modifiers, record):
		'''
		If the title_modifier field indicates a value that identifies which of
		the multiple objects in the current lot the current record is about
		(based on the presence of a known prefix string), assert that value
		as an additional title.
		
		Otherwise, assert any title_modifier value as a TitleStatment referring
		to the object.
		
		Returns True if a new title was asserted, False otherwise.
		'''
		hmo = get_crom_object(data)
		this_lot_prefixes = title_modifiers['this lot']
		handled = None
		if 'title_modifier' in data:
			mod = data['title_modifier']
			for prefix in this_lot_prefixes:
				r = re.compile(f'({prefix})\s*:\s*(.*)', re.IGNORECASE)
				m = r.match(mod)
				if m:
					p, mod = m.groups()
					t = vocab.PrimaryName(ident='', content=mod)
					t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
					t.referred_to_by = record
					data['identifiers'].append(t)
					handled = mod
					break
			if not handled:
				hmo.referred_to_by = vocab.Note(ident='', content=mod)
		return handled


	def __call__(self, data:dict, post_sale_map, unique_catalogs, subject_genre, destruction_types_map, materials_map, non_auctions, title_modifiers, event_properties, transaction_classification):
		'''Add modeling for an object described by a sales record'''
		hmo = get_crom_object(data)
		parent = data['parent_data']
		auction_data = parent.get('auction_of_lot')
		cno = auction_data['catalog_number']
		lno = auction_data['lot_number']
		date = implode_date(auction_data, 'lot_sale_')

		if auction_data:
			lno = str(auction_data['lot_number'])
			data.setdefault('identifiers', [])
			if not lno:
				warnings.warn(f'Setting empty identifier on {hmo.id}')

			sale_type = non_auctions.get(cno, 'Auction')
			event_date_label = event_properties['auction_date_label'].get(cno)
			lot_number = self.helper.lot_number_identifier(lno, cno, non_auctions, sale_type, date_label=event_date_label)
			data['identifiers'].append(lot_number)
		else:
			warnings.warn(f'***** NO AUCTION DATA FOUND IN populate_object')


		lot = self.helper.shared_lot_number_from_lno(lno) # the current key for this object; may be associated later with prev and post object keys
		now_key = (cno, lno, date)

		data['_locations'] = []
		data['_final_org'] = []
		data['_events'] = []
		record = self._populate_object_catalog_record(data, parent, lot, cno, parent['pi_record_no'], transaction_classification)
		self._populate_object_destruction(data, parent, destruction_types_map)
		self.populate_object_statements(data)
		self._populate_object_materials(data, materials_map)
		self._populate_object_present_location(data, now_key, destruction_types_map)
		self._populate_object_notes(data, parent, unique_catalogs)
		self._populate_object_prev_post_sales(data, now_key, post_sale_map)

		modified_title = self._populate_title_modifier(data, title_modifiers, record)
		self._populate_object_visual_item(data, subject_genre, modified_title, record)

		title_type = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
		trans_type = model.Type(ident='http://vocab.getty.edu/aat/300417194', label='Translated Title')

		if 'title' in data:
			title = data['title']
			if not hasattr(hmo, '_label'):
				typestring = data.get('object_type', 'Object')
				hmo._label = f'{typestring}: “{title}”'
			del data['title']
			shorter = truncate_with_ellipsis(title, 100)
			if shorter:
				description = vocab.Description(ident='', content=title)
				description.referred_to_by = record
				hmo.referred_to_by = description
				title = shorter
			title_class = vocab.Name if modified_title else vocab.PrimaryName
			t = title_class(ident='', content=title)
			t.classified_as = title_type
			t.referred_to_by = record
			data['identifiers'].append(t)

		if 'title_translation' in data:
			title = data['title_translation']
			t = vocab.Name(ident='', content=title)
			t.classified_as = title_type
			t.classified_as = trans_type
			t.language = vocab.instances['english']
			t.referred_to_by = record
			data['identifiers'].append(t)
			
		for d in data.get('other_titles', []):
			title = d['title']
			t = vocab.Name(ident='', content=title)
			data['identifiers'].append(t)

		return data

@use('vocab_type_map')
def add_object_type(data, vocab_type_map):
	'''Add appropriate type information for an object based on its 'object_type' name'''
	typestring = data.get('object_type', '')
	hmo = None
	if typestring in vocab_type_map:
		clsname = vocab_type_map.get(typestring, None)
		otype = getattr(vocab, clsname)
		hmo = otype(ident=data['uri'])
		add_crom_data(data=data, what=hmo)
	elif ';' in typestring:
		parts = [s.strip() for s in typestring.split(';')]
		if all([s in vocab_type_map for s in parts]):
			types = [getattr(vocab, vocab_type_map[s]) for s in parts]
			hmo = vocab.make_multitype_obj(*types, ident=data['uri'])
			add_crom_data(data=data, what=hmo)
		else:
			warnings.warn(f'*** Not all object types matched for {typestring!r}')
			hmo = model.HumanMadeObject(ident=data['uri'])
			add_crom_data(data=data, what=hmo)
	else:
		warnings.warn(f'*** No object type for {typestring!r}')
		hmo = model.HumanMadeObject(ident=data['uri'])
		add_crom_data(data=data, what=hmo)

	parent = data['parent_data']
	sale_record = get_crom_object(parent['_sale_record'])
	if hmo:
		hmo.referred_to_by = sale_record
	coll_data = parent.get('_lot_object_set')
	if coll_data:
		coll = get_crom_object(coll_data)
		if coll:
			data['member_of'] = [coll]

	return data

class AddArtists(ProvenanceBase):
	helper = Option(required=True)
	attribution_modifiers = Service('attribution_modifiers')
	attribution_group_types = Service('attribution_group_types')
	attribution_group_names = Service('attribution_group_names')

	def add_properties(self, data:dict, a:dict):
		sales_record = get_crom_object(data['_record'])
		a.setdefault('referred_to_by', [])
		a.update({
			'pi_record_no': data['pi_record_no'],
			'ulan': a['artist_ulan'],
			'auth_name': a['auth_name'],
			'name': a['artist_name'],
			'modifiers': self.modifiers(a),
# 			'label': a.get('auth_name', a.get('artist_name')),
		})
		
		if self.helper.person_identity.acceptable_person_auth_name(a.get('auth_name')):
			a.setdefault('label', a.get('auth_name'))
		a.setdefault('label', a.get('artist_name'))

		if a.get('biography'):
			bio = a['biography']
			del a['biography']
			cite = vocab.BiographyStatement(ident='', content=bio)
			cite.referred_to_by = sales_record
			a['referred_to_by'].append(cite)

	def is_or_anon(self, data:dict):
		pi = self.helper.person_identity
		if pi.is_anonymous(data):
			mods = {m.lower().strip() for m in data.get('attrib_mod_auth', '').split(';')}
			return 'or' in mods
		return False

	def model_person_or_group(self, data:dict, a:dict, attribution_group_types, attribution_group_names, role='artist', seq_no=0, sales_record=None):
		if get_crom_object(a):
			return a

		mods = a['modifiers']
			
		artist = self.helper.add_person(a, record=sales_record, relative_id=f'artist-{seq_no+1}', role=role)
		artist.referred_to_by = sales_record
		artist_label = a['label']
		person = get_crom_object(a)

		if mods:
			GROUP_TYPES = set(attribution_group_types.values())
			GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}

			if mods.intersects(GROUP_MODS):
				mod_name = list(GROUP_MODS & mods)[0] # TODO: use all matching types?
				clsname = attribution_group_types[mod_name]
				cls = getattr(vocab, clsname)
				group_name = attribution_group_names[clsname]
				group_label = f'{group_name} {artist_label}'
				# The group URI is just the person URI with a suffix. In any case
				# where the person is merged, the group should be merged as well.
				# For example, when if "RUBENS" is merged, "School of RUBENS" should
				# also be merged.
				group_id = a['uri'] + f'-{clsname}'
				group = cls(ident=group_id, label=group_label)
				group.referred_to_by = sales_record
				group.identified_by = model.Name(ident='', content=group_label)
				formation = model.Formation(ident='', label=f'Formation of {group_label}')
				formation.influenced_by = person
				group.formed_by = formation
				pi_record_no = data['pi_record_no']
				group_uri_key = ('GROUP', 'PI', pi_record_no, f'{role}Group')
				group_data = {
					'uri': group_id,
					'uri_keys': group_uri_key,
					'modifiers': mods,
				}
				add_crom_data(group_data, group)
				data['_organizations'].append(group_data)
				return group_data

		add_crom_data(a, artist)
		return a

	def modifiers(self, a:dict):
		mod = a.get('attrib_mod_auth', '')
		mods = CaseFoldingSet({m.strip() for m in mod.split(';')} - {''})
		return mods

	def model_object_artists(self, data, people, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=False):
		FORMERLY_ATTRIBUTED_TO = attribution_modifiers['formerly attributed to']
		POSSIBLY = attribution_modifiers['possibly by']
		UNCERTAIN = attribution_modifiers['uncertain']
		ATTRIBUTED_TO = attribution_modifiers['attributed to']

		event_uri = prod_event.id
		sales_record = get_crom_object(data['_record'])
		artists = [p for p in people if not self.is_or_anon(p)]
		or_anon_records = any([self.is_or_anon(a) for a in people])
		if or_anon_records:
			all_uncertain = True

		try:
			hmo_label = f'{hmo._label}'
		except AttributeError:
			hmo_label = 'object'

		# 5. Determine if the artist records represent a disjunction (similar to 2 above):
		artist_all_mods = {m.lower().strip() for a in artists for m in a.get('attrib_mod_auth', '').split(';')} - {''}
		all_or_modifiers = ['or' in a['modifiers'] for a in artists]
		artist_group_flag = (not or_anon_records) and len(all_or_modifiers) and all(all_or_modifiers)
		artist_group = None
		if artist_group_flag:
			# The artist group URI is just the production event URI with a suffix. When URIs are
			# reconciled during prev/post sale rewriting, this will allow us to also reconcile
			# the URIs for the artist groups (of which there should only be one per production/object)
			group_uri = prod_event.id + '-ArtistGroup'
			g_label = f'Group containing the artist of {hmo_label}'
			artist_group = vocab.UncertainMemberClosedGroup(ident=group_uri, label=g_label)
			artist_group.identified_by = model.Name(ident='', content=g_label)
			pi_record_no = data['pi_record_no']
			group_uri_key = ('GROUP', 'PI', pi_record_no, 'ArtistGroup')
			group_data = {
				'uri': group_uri,
				'uri_keys': group_uri_key,
				'role_label': 'uncertain artist'
			}
			add_crom_data(data=group_data, what=artist_group)
			data['_organizations'].append(group_data)

			# 6. Model all the artist records as sub-production events:
			prod_event.carried_out_by = artist_group
			for seq_no, a_data in enumerate(artists):
				mods = a_data['modifiers']
				attribute_assignment_id = self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,Artist-{seq_no}')
				artist_label = a_data.get('label') # TODO: this may not be right for groups
				a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='Artist', sales_record=sales_record)
				person = get_crom_object(a_data)
				if ATTRIBUTED_TO.intersects(mods):
					attrib_assignment_classes = [model.AttributeAssignment]
					attrib_assignment_classes.append(vocab.PossibleAssignment)
					assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
					assignment._label = f'Possibly by {artist_label}'
					person.attributed_by = assignment
					assignment.assigned_property = 'member_of'
					assignment.assigned = artist_group
				else:
					person.member_of = artist_group
		else:
			for seq_no, a_data in enumerate(artists):
				uncertain = all_uncertain
				attribute_assignment_id = self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,Artist-{seq_no}')
				artist_label = a_data.get('label') # TODO: this may not be right for groups
				a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='Artist', sales_record=sales_record)
				person = get_crom_object(a_data)
				mods = a_data['modifiers']
				attrib_assignment_classes = [model.AttributeAssignment]
				subprod_path = self.helper.make_uri_path(*a_data["uri_keys"])
				subevent_id = event_uri + f'-{subprod_path}'
				if UNCERTAIN.intersects(mods):
					if POSSIBLY.intersects(mods):
						attrib_assignment_classes.append(vocab.PossibleAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
						assignment._label = f'Possibly by {artist_label}'
					else:
						attrib_assignment_classes.append(vocab.ProbableAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Probably attributed to {artist_label}')
						assignment._label = f'Probably by {artist_label}'

					# TODO: this assigns an uncertain carried_out_by property directly to the top-level production;
					#       should it instead be an uncertain sub-production part?
					prod_event.attributed_by = assignment
					assignment.assigned_property = 'carried_out_by'
					assignment.assigned = person
				elif FORMERLY_ATTRIBUTED_TO.intersects(mods):
					attrib_assignment_classes = [vocab.ObsoleteAssignment]
					if uncertain:
						attrib_assignment_classes.append(vocab.PossibleAssignment)
					assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Formerly attributed to {artist_label}')
					prod_event.attributed_by = assignment
					assignment.assigned_property = 'carried_out_by'
					assignment.assigned = person
				else:
					if uncertain or ATTRIBUTED_TO.intersects(mods):
						attrib_assignment_classes.append(vocab.PossibleAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
						prod_event.attributed_by = assignment
						assignment.assigned_property = 'carried_out_by'
						assignment.assigned = person
					else:
						subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {artist_label}')
						subevent.carried_out_by = person
						prod_event.part = subevent

	def model_object_influence(self, data, people, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=False):
		STYLE_OF = attribution_modifiers['style of']
		COPY_AFTER = attribution_modifiers['copy after']
		NON_ARTIST_MODS = COPY_AFTER | STYLE_OF
		GROUP_TYPES = set(attribution_group_types.values())
		GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}

		non_artist_assertions = people
		sales_record = get_crom_object(data['_record'])

		try:
			hmo_label = f'{hmo._label}'
		except AttributeError:
			hmo_label = 'object'

		# 2. Determine if the non-artist records represent a disjunction. If all such records have an "or" modifier, we will represent all the people as a Group, and classify it to indicate that one and only one of the named people was the actor. If there is at least one 'or' modifier, but not all of the records have 'or', then we model each such record with uncertainty.
		non_artist_all_mods = {m.lower().strip() for a in non_artist_assertions for m in a.get('attrib_mod_auth', '').split(';')} - {''}
		non_artist_group_flag = len(non_artist_assertions) and all(['or' in a['modifiers'] for a in non_artist_assertions])
		non_artist_group = None
		if non_artist_group_flag:
			non_artist_mod = list(NON_ARTIST_MODS.intersection(non_artist_all_mods))[0]
			# The artist group URI is just the production event URI with a suffix. When URIs are
			# reconciled during prev/post sale rewriting, this will allow us to also reconcile
			# the URIs for the artist groups (of which there should only be one per production/object)
			group_uri = prod_event.id + '-NonArtistGroup'
			g_label = f'Group containing the {non_artist_mod} of {hmo_label}'
			non_artist_group = vocab.UncertainMemberClosedGroup(ident=group_uri, label=g_label)
			non_artist_group.identified_by = model.Name(ident='', content=g_label)
			group_data = {
				'uri': group_uri,
				'role_label': 'uncertain influencer'
			}
			make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
			group_data = make_la_org(group_data)
			data['_organizations'].append(group_data)

		# 3. Model all the non-artist records as an appropriate property/relationship of the object or production event:
		for seq_no, a_data in enumerate(non_artist_assertions):
			artist_label = a_data.get('label')
			a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='NonArtist', sales_record=sales_record)
			person = get_crom_object(a_data)

			mods = a_data['modifiers']
			attrib_assignment_classes = [model.AttributeAssignment]
			uncertain = all_uncertain
			if uncertain or 'or' in mods:
				if non_artist_group_flag:
					person.member_of = non_artist_group
				else:
					uncertain = True
					attrib_assignment_classes.append(vocab.PossibleAssignment)
			
			if STYLE_OF.intersects(mods):
				attribute_assignment_id = self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,NonArtist-{seq_no}')
				assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'In the style of {artist_label}')
				prod_event.attributed_by = assignment
				assignment.assigned_property = 'influenced_by'
				assignment.property_classified_as = vocab.instances['style of']
				assignment.assigned = person
			elif COPY_AFTER.intersects(mods):
				cls = type(hmo)
				# The original object URI is just the object URI with a suffix. When URIs are
				# reconciled during prev/post sale rewriting, this will allow us to also reconcile
				# the URIs for the original object (of which there should be at most one per object)
				original_id = hmo.id + '-Original'
				original_label = f'Original of {hmo_label}'
				original_hmo = cls(ident=original_id, label=original_label)
				original_hmo.referred_to_by = sales_record
				
				# original title
				original_hmo.identified_by = vocab.ConstructedTitle(ident='', content=f'[Work] by {artist_label}')
				
				# Similarly for the production of the original object.
				original_event_id = original_hmo.id + '-Production'
				original_event = model.Production(ident=original_event_id, label=f'Production event for {original_label}')
				original_hmo.produced_by = original_event

				original_subevent_id = original_event_id + f'-{seq_no}' # TODO: fix for the case of post-sales merging
				original_subevent = model.Production(ident=original_subevent_id, label=f'Production sub-event for {artist_label}')
				original_event.part = original_subevent
				original_subevent.carried_out_by = person

				if uncertain:
					assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly influenced by {person._label}')
					prod_event.attributed_by = assignment
					assignment.assigned_property = 'influenced_by'
					assignment.assigned = original_hmo
				else:
					prod_event.influenced_by = original_hmo
				data['_original_objects'].append(add_crom_data(data={'uri': original_id}, what=original_hmo))
			else:
				warnings.warn(f'Unrecognized non-artist attribution modifers: {mods}')

	def uncertain_artist_or_style(self, people:dict):
		if len(people) != 2:
			return False
		
		names = [p.get('art_authority') for p in people]
		if not names[0]:
			return False

		if names[0] != names[1]:
			return False
		
		mods = set([frozenset(p['modifiers']) for p in people])
		expected = {frozenset({'or'}), frozenset({'manner of', 'style of', 'or'})}
		return mods == expected

	def __call__(self, data:dict, *, attribution_modifiers, attribution_group_types, attribution_group_names):
		'''Add modeling for artists as people involved in the production of an object'''
		hmo = get_crom_object(data)

		self.model_artists_with_modifers(data, hmo, attribution_modifiers, attribution_group_types, attribution_group_names)
		return data
