import re
import warnings
import pprint
from contextlib import suppress

from bonobo.config import Option, Service, Configurable, use

from cromulent import model, vocab
from cromulent.extract import extract_physical_dimensions

import pipeline.execution
from pipeline.util import implode_date, timespan_from_outer_bounds
from pipeline.util.cleaners import \
			parse_location_name, \
			date_cleaner
import pipeline.linkedart
from pipeline.linkedart import add_crom_data, get_crom_object
from pipeline.util import truncate_with_ellipsis

#mark - Auction of Lot - Physical Object

class PopulateObject(Configurable):
	helper = Option(required=True)
	post_sale_map = Service('post_sale_map')
	unique_catalogs = Service('unique_catalogs')
	vocab_instance_map = Service('vocab_instance_map')
	destruction_types_map = Service('destruction_types_map')

	def genre_instance(self, value, vocab_instance_map):
		'''Return the appropriate type instance for the supplied genre name'''
		if value is None:
			return None
		value = value.lower()

		instance_name = vocab_instance_map.get(value)
		if instance_name:
			instance = vocab.instances.get(instance_name)
			if not instance:
				warnings.warn(f'*** No genre instance available for {instance_name!r} in vocab_instance_map')
			return instance
		return None

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
			dest_id = hmo.id + '-Destruction'
			d = model.Destruction(ident=dest_id, label=f'Destruction of “{short_title}”')
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
				base_uri = hmo.id + '-Place,'
				place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
				place = get_crom_object(place_data)
				if place:
					data['_locations'].append(place_data)
					d.took_place_at = place

			hmo.destroyed_by = d

	def _populate_object_visual_item(self, data:dict, vocab_instance_map):
		hmo = get_crom_object(data)
		title = data.get('title')
		title = truncate_with_ellipsis(title, 100) or title

		vi_id = hmo.id + '-VisualItem'
		vi = model.VisualItem(ident=vi_id)
		vidata = {'uri': vi_id}
		if title:
			vidata['label'] = f'Visual work of “{title}”'
			sales_record = get_crom_object(data['_record'])
			vidata['names'] = [(title,{'referred_to_by': [sales_record]})]

		genre = self.genre_instance(data.get('genre'), vocab_instance_map)
		if genre:
			vi.classified_as = genre
		data['_visual_item'] = add_crom_data(data=vidata, what=vi)
		hmo.shows = vi

	def _populate_object_catalog_record(self, data:dict, parent, lot, cno, rec_num):
		catalog_uri = self.helper.make_proj_uri('CATALOG', cno)
		catalog = vocab.AuctionCatalogText(ident=catalog_uri)

		record_uri = self.helper.make_proj_uri('CATALOG', cno, 'RECORD', rec_num)
		lot_object_id = parent['lot_object_id']
		record = model.LinguisticObject(ident=record_uri, label=f'Sale recorded in catalog: {lot_object_id} (record number {rec_num})') # TODO: needs classification
		record_data	= {'uri': record_uri}
		record_data['identifiers'] = [model.Name(ident='', content=f'Record of sale {lot_object_id}')]
		record.part_of = catalog

		data['_record'] = add_crom_data(data=record_data, what=record)
		return record

	def _populate_object_destruction(self, data:dict, parent, destruction_types_map):
		notes = parent.get('auction_of_lot', {}).get('lot_notes')
		if notes and notes.lower().startswith('destroyed'):
			self.populate_destruction_events(data, notes, type_map=destruction_types_map)

	@staticmethod
	def _populate_object_statements(data:dict):
		hmo = get_crom_object(data)
		materials = data.get('materials')
		if materials:
			matstmt = vocab.MaterialStatement(ident='', content=materials)
			sales_record = get_crom_object(data['_record'])
			matstmt.referred_to_by = sales_record
			hmo.referred_to_by = matstmt

		dimstr = data.get('dimensions')
		if dimstr:
			dimstmt = vocab.DimensionStatement(ident='', content=dimstr)
			sales_record = get_crom_object(data['_record'])
			dimstmt.referred_to_by = sales_record
			hmo.referred_to_by = dimstmt
			for dim in extract_physical_dimensions(dimstr):
				dim.referred_to_by = sales_record
				hmo.dimension = dim
		else:
			pass
	# 		print(f'No dimension data was parsed from the dimension statement: {dimstr}')

	def _populate_object_present_location(self, data:dict, now_key, destruction_types_map):
		hmo = get_crom_object(data)
		location = data.get('present_location')
		if location:
			loc = location.get('geog')
			note = location.get('note')
			if loc:
				if 'destroyed ' in loc.lower():
					self.populate_destruction_events(data, loc, type_map=destruction_types_map)
				elif isinstance(note, str) and 'destroyed ' in note.lower():
					# the object was destroyed, so any "present location" data is actually
					# an indication of the location of destruction.
					self.populate_destruction_events(data, note, type_map=destruction_types_map, location=loc)
				else:
					# TODO: if `parse_location_name` fails, still preserve the location string somehow
					current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
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
							owner_data['uri'] = self.helper.make_proj_uri('ORGANIZATION', 'ULAN', ulan)
						else:
							owner_data['uri'] = self.helper.make_proj_uri('ORGANIZATION', 'NAME', inst, 'PLACE', loc)
					else:
						owner_data = {
							'label': '(Anonymous organization)',
							'uri': self.helper.make_proj_uri('ORGANIZATION', 'PRESENT-OWNER', *now_key),
						}

					base_uri = hmo.id + '-Place,'
					place_data = pipeline.linkedart.make_la_place(current, base_uri=base_uri)
					place = get_crom_object(place_data)

					make_la_org = pipeline.linkedart.MakeLinkedArtOrganization()
					owner_data = make_la_org(owner_data)
					owner = get_crom_object(owner_data)
					owner.residence = place
					data['_locations'].append(place_data)
					data['_final_org'] = owner_data
			else:
				pass # there is no present location place string
			if note:
				pass
				# TODO: the acquisition_note needs to be attached as a Note to the final post owner acquisition

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
		post_sales = data.get('post_sale', [])
		prev_sales = data.get('prev_sale', [])
		prev_post_sales_records = [(post_sales, False), (prev_sales, True)]
		for sales_data, rev in prev_post_sales_records:
			for sale_record in sales_data:
				pcno = sale_record.get('cat')
				plno = sale_record.get('lot')
				plot = self.helper.shared_lot_number_from_lno(plno)
				pdate = implode_date(sale_record, '')
				if pcno and plot and pdate:
					that_key = (pcno, plot, pdate)
					if rev:
						# `that_key` is for a previous sale for this object
						post_sale_map[this_key] = that_key
					else:
						# `that_key` is for a later sale for this object
						post_sale_map[that_key] = this_key

	def __call__(self, data:dict, post_sale_map, unique_catalogs, vocab_instance_map, destruction_types_map):
		'''Add modeling for an object described by a sales record'''
		hmo = get_crom_object(data)
		parent = data['parent_data']
		auction_data = parent.get('auction_of_lot')
		if auction_data:
			lno = str(auction_data['lot_number'])
			if 'identifiers' not in data:
				data['identifiers'] = []
			if not lno:
				warnings.warn(f'Setting empty identifier on {hmo.id}')
			data['identifiers'].append(vocab.LotNumber(ident='', content=lno))
		else:
			warnings.warn(f'***** NO AUCTION DATA FOUND IN populate_object')


		cno = auction_data['catalog_number']
		lno = auction_data['lot_number']
		date = implode_date(auction_data, 'lot_sale_')
		lot = self.helper.shared_lot_number_from_lno(lno)
		now_key = (cno, lot, date) # the current key for this object; may be associated later with prev and post object keys

		data['_locations'] = []
		data['_events'] = []
		record = self._populate_object_catalog_record(data, parent, lot, cno, parent['pi_record_no'])
		self._populate_object_visual_item(data, vocab_instance_map)
		self._populate_object_destruction(data, parent, destruction_types_map)
		self._populate_object_statements(data)
		self._populate_object_present_location(data, now_key, destruction_types_map)
		self._populate_object_notes(data, parent, unique_catalogs)
		self._populate_object_prev_post_sales(data, now_key, post_sale_map)
		for p in data.get('portal', []):
			url = p['portal_url']
			hmo.referred_to_by = vocab.WebPage(ident=url)

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
			t = vocab.PrimaryName(ident='', content=title)
			t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300417193', label='Title')
			t.referred_to_by = record
			data['identifiers'].append(t)

		return data

@use('vocab_type_map')
def add_object_type(data, vocab_type_map):
	'''Add appropriate type information for an object based on its 'object_type' name'''
	typestring = data.get('object_type', '')
	if typestring in vocab_type_map:
		clsname = vocab_type_map.get(typestring, None)
		otype = getattr(vocab, clsname)
		add_crom_data(data=data, what=otype(ident=data['uri']))
	elif ';' in typestring:
		parts = [s.strip() for s in typestring.split(';')]
		if all([s in vocab_type_map for s in parts]):
			types = [getattr(vocab, vocab_type_map[s]) for s in parts]
			obj = vocab.make_multitype_obj(*types, ident=data['uri'])
			add_crom_data(data=data, what=obj)
		else:
			warnings.warn(f'*** Not all object types matched for {typestring!r}')
			add_crom_data(data=data, what=model.HumanMadeObject(ident=data['uri']))
	else:
		warnings.warn(f'*** No object type for {typestring!r}')
		add_crom_data(data=data, what=model.HumanMadeObject(ident=data['uri']))

	parent = data['parent_data']
	coll_data = parent.get('_lot_object_set')
	if coll_data:
		coll = get_crom_object(coll_data)
		if coll:
			data['member_of'] = [coll]

	return data

class AddArtists(Configurable):
	helper = Option(required=True)
	attribution_modifiers = Service('attribution_modifiers')
	attribution_group_types = Service('attribution_group_types')
	make_la_person = Service('make_la_person')

	def __call__(self, data:dict, *, attribution_modifiers, attribution_group_types, make_la_person):
		'''Add modeling for artists as people involved in the production of an object'''
		hmo = get_crom_object(data)
		data['_organizations'] = []
		data['_original_objects'] = []

		try:
			hmo_label = f'{hmo._label}'
		except AttributeError:
			hmo_label = 'object'
		event_id = hmo.id + '-Production'
		event = model.Production(ident=event_id, label=f'Production event for {hmo_label}')
		hmo.produced_by = event

		artists = data.get('_artists', [])

		sales_record = get_crom_object(data['_record'])
		pi = self.helper.person_identity

		for a in artists:
			a.update({
				'pi_record_no': data['pi_record_no'],
				'ulan': a['artist_ulan'],
				'auth_name': a['art_authority'],
				'name': a['artist_name']
			})

		def is_or_anon(data:dict):
			if not pi.is_anonymous(data):
				return False
			mods = {m.lower().strip() for m in data.get('attrib_mod_auth', '').split(';')}
			return 'or' in mods
		or_anon_records = [is_or_anon(a) for a in artists]
		uncertain_attribution = any(or_anon_records)
		for seq_no, a in enumerate(artists):
			attrib_assignment_classes = [model.AttributeAssignment]
			if uncertain_attribution:
				attrib_assignment_classes.append(vocab.PossibleAssignment)
			if is_or_anon(a):
				# do not model the "or anonymous" records; they turn into uncertainty on the other records
				continue
			pi.add_uri(a, record_id=f'artist-{seq_no+1}')
			pi.add_names(a, referrer=sales_record, role='artist')
			artist_label = a.get('role_label')
			make_la_person(a)
			person = get_crom_object(a)

			mod = a.get('attrib_mod_auth')
			if mod:
				mods = {m.lower().strip() for m in mod.split(';')}

				# TODO: this should probably be in its own JSON service file:
				STYLE_OF = set(attribution_modifiers['style of'])
				FORMERLY_ATTRIBUTED_TO = set(attribution_modifiers['formerly attributed to'])
				ATTRIBUTED_TO = set(attribution_modifiers['attributed to'])
				COPY_AFTER = set(attribution_modifiers['copy after'])
				PROBABLY = set(attribution_modifiers['probably by'])
				POSSIBLY = set(attribution_modifiers['possibly by'])
				UNCERTAIN = PROBABLY | POSSIBLY

				GROUP_TYPES = set(attribution_group_types.values())
				GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}

				if 'or' in mods:
					warnings.warn('Handle OR attribution modifier') # TODO: some way to model this uncertainty?

				if 'copy by' in mods:
					# equivalent to no modifier
					pass
				elif ATTRIBUTED_TO & mods:
					# equivalent to no modifier
					pass
				elif STYLE_OF & mods:
					assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident='', label=f'In the style of {artist_label}')
					event.attributed_by = assignment
					assignment.assigned_property = 'influenced_by'
					assignment.property_classified_as = vocab.instances['style of']
					assignment.assigned = person
					continue
				elif GROUP_MODS & mods:
					mod_name = list(GROUP_MODS & mods)[0] # TODO: use all matching types?
					clsname = attribution_group_types[mod_name]
					cls = getattr(vocab, clsname)
					group_label = f'{clsname} of {artist_label}'
					group_id = a['uri'] + f'-{clsname}'
					group = cls(ident=group_id, label=group_label)
					formation = model.Formation(ident='', label=f'Formation of {group_label}')
					formation.influenced_by = person
					group.formed_by = formation
					group_data = add_crom_data({'uri': group_id}, group)
					data['_organizations'].append(group_data)

					subevent_id = event_id + f'-{seq_no}' # TODO: fix for the case of post-sales merging
					subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {group_label}')
					subevent.carried_out_by = group

					if uncertain_attribution:
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident='', label=f'Possibly attributed to {group_label}')
						event.attributed_by = assignment
						assignment.assigned_property = 'part'
						assignment.assigned = subevent
					else:
						event.part = subevent
					continue
				elif FORMERLY_ATTRIBUTED_TO & mods:
					# the {uncertain_attribution} flag does not apply to this branch, because this branch is not making a statement
					# about a previous attribution. the uncertainty applies only to the current attribution.
					assignment = vocab.ObsoleteAssignment(ident='', label=f'Formerly attributed to {artist_label}')
					event.attributed_by = assignment
					assignment.assigned_property = 'carried_out_by'
					assignment.assigned = person
					continue
				elif UNCERTAIN & mods:
					if POSSIBLY & mods:
						attrib_assignment_classes.append(vocab.PossibleAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident='', label=f'Possibly attributed to {artist_label}')
						assignment._label = f'Possibly by {artist_label}'
					else:
						attrib_assignment_classes.append(vocab.ProbableAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident='', label=f'Probably attributed to {artist_label}')
						assignment._label = f'Probably by {artist_label}'
					event.attributed_by = assignment
					assignment.assigned_property = 'carried_out_by'
					assignment.assigned = person
					continue
				elif COPY_AFTER & mods:
					# the {uncertain_attribution} flag does not apply to this branch, because this branch is not making a statement
					# about the artist of the work, but about the artist of the original work that this work is a copy of.
					cls = type(hmo)
					original_id = hmo.id + '-Original'
					original_label = f'Original of {hmo_label}'
					original_hmo = cls(ident=original_id, label=original_label)
					original_event_id = original_hmo.id + '-Production'
					original_event = model.Production(ident=original_event_id, label=f'Production event for {original_label}')
					original_hmo.produced_by = original_event

					original_subevent_id = original_event_id + f'-{seq_no}' # TODO: fix for the case of post-sales merging
					original_subevent = model.Production(ident=original_subevent_id, label=f'Production sub-event for {artist_label}')
					original_event.part = original_subevent
					original_subevent.carried_out_by = person

					event.influenced_by = original_hmo
					data['_original_objects'].append(add_crom_data(data={}, what=original_hmo))
					continue
				elif {'or', 'and'} & mods:
					pass
				else:
					print(f'UNHANDLED attrib_mod_auth VALUE: {mods}')
					pprint.pprint(a)
					continue

			subprod_path = self.helper.make_uri_path(*a["uri_keys"])
			subevent_id = event_id + f'-{subprod_path}'
			subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {artist_label}')
			subevent.carried_out_by = person
			if uncertain_attribution:
				assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident='', label=f'Possibly attributed to {artist_label}')
				event.attributed_by = assignment
				assignment.assigned_property = 'part'
				assignment.assigned = subevent
			else:
				event.part = subevent
		data['_artists'] = [a for a in artists if not is_or_anon(a)]
		return data
