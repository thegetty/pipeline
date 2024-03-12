import warnings
import pprint
import traceback
from contextlib import suppress

from bonobo.config import Option, Service, Configurable

from cromulent import model, vocab
from cromulent.model import factory

from pipeline.util import \
		timespan_before, \
		timespan_after, \
		timespan_from_outer_bounds, \
		implode_date, \
		CaseFoldingSet, \
		truncate_with_ellipsis
		
from pipeline.util.cleaners import parse_location_name
from pipeline.linkedart import add_crom_data, get_crom_object, get_crom_objects


class ProvenanceBase(Configurable):
	'''
	This is a base class providing common functionality in the handling of Provenance Entries.
	'''
	
	helper = Option(required=True)

	def add_person(self, data:dict, record, relative_id, **kwargs):
		'''
		Add modeling data for people, based on properties of the supplied `data` dict.

		This function adds properties to `data` before calling
		`pipeline.linkedart.MakeLinkedArtPerson` to construct the model objects.
		'''
		self.helper.add_person(data, record=record, relative_id=relative_id, **kwargs)
		return data

	def related_procurement(self, hmo, tx_label_args, current_tx=None, current_ts=None, buyer=None, seller=None, previous=False, ident=None, make_label=None, sales_record=None):
		'''
		Returns a new `vocab.ProvenanceEntry` object (and related acquisition) that is temporally
		related to the supplied procurement and associated data. The new procurement is for
		the given object, and has the given buyer and seller (both optional).

		If the `previous` flag is `True`, the new procurement is occurs before `current_tx`,
		and if the timespan `current_ts` is given, has temporal data to that effect. If
		`previous` is `False`, this relationship is reversed.
		
		The `make_label` argument, if supplied, is used as a function to generate the
		label for the provenance entry. Its arguments (generated in `handle_prev_post_owner`)
		are:
		
		  * helper: the helper object for the current pipeline
		  * sale_type: the sale type passed to `handle_prev_post_owner` (e.g. "Auction")
		  * transaction: the transaction type being handled (e.g. "Sold")
		  * rel: a string describing the relationship between this provenance entry and the object (e.g. "leading to Ownership of")
		  * N trailing arguments used that are the contents of the `lot_object_key` tuple passed to `handle_prev_post_owner`
		'''

		def _make_label_default(helper, sale_type, transaction, rel, *args):
			# import pdb; pdb.set_trace()
			str = f'Provenance Entry {rel} object identified in book {args[2]}, page {args[3]}, row {args[4]}'
			
			#strs = [str(x) for x in args]
			
			# import pdb; pdb.set_trace()
			#return ', '.join(strs)
			return str
		
		if make_label is None:
			
			make_label = _make_label_default


		tx = vocab.ProvenanceEntry(ident=ident)
		if sales_record:
			tx.referred_to_by = sales_record
		tx_label = make_label(*tx_label_args)
		tx._label = tx_label
		tx.identified_by = model.Name(ident='', content=tx_label)
		if current_tx:
			if previous:
				tx.ends_before_the_start_of = current_tx
			else:
				tx.starts_after_the_end_of = current_tx
		modifier_label = 'Previous' if previous else 'Subsequent'
		
		prev_acq_id = ident + '-prev-acq' if ident else ''
		trsf_id = ident + '-prev-transfer' if ident else ''
		try:
			pacq = model.Acquisition(ident=prev_acq_id, label=f'{modifier_label} Acquisition of: “{hmo._label}”')
			pxfer = model.TransferOfCustody(ident=trsf_id, label=f'{modifier_label} Transfer of Custody of: “{hmo._label}”')
		except AttributeError:
			pacq = model.Acquisition(ident=prev_acq_id, label=f'{modifier_label} Acquisition')
			pxfer = model.TransferOfCustody(ident=trsf_id, label=f'{modifier_label} Transfer of Custody')
		pacq.transferred_title_of = hmo
		pxfer.transferred_custody_of = hmo
		if buyer:
			pacq.transferred_title_to = buyer
			pxfer.transferred_custody_to = buyer
		if seller:
			pacq.transferred_title_from = seller
			pxfer.transferred_custody_from = seller

		tx.part = pacq
		tx.part = pxfer
		if current_ts:
			if previous:
				pacq.timespan = timespan_before(current_ts)
			else:
				pacq.timespan = timespan_after(current_ts)
		return tx, pacq

	def handle_prev_post_owner(self, data, hmo, tx_data, sale_type, lot_object_key, owner_record, record_id, rev, ts=None, make_label=None):
		current_tx = get_crom_object(tx_data)
		sales_record = get_crom_object(data.get('_record', data.get('_text_row')))
		if rev:
			rel = f'leading to Ownership of'
			source_label = 'Source of information on history of the object prior to the current sale.'
		else:
			rel = f'leading to Ownership of'
			source_label = 'Source of information on history of the object after the current sale.'
		owner_record.update({
			'pi_record_no': data['pi_record_no'],
			'ulan': owner_record.get('ulan', owner_record.get('own_ulan')),
		})
		self.add_person(owner_record, record=sales_record, relative_id=record_id, role='artist')
		owner = get_crom_object(owner_record)

		# TODO: handle other fields of owner_record: own_auth_d, own_auth_q, own_ques, own_so

		if owner_record.get('own_auth_l'):
			loc = owner_record['own_auth_l']
			
			canonical_place = self.helper.get_canonical_place(loc)
			if canonical_place:
				place = canonical_place
				place_data = add_crom_data(data={'uri': place.id}, what=place)
			else:
				current = parse_location_name(loc, uri_base=self.helper.uid_tag_prefix)
				place_data = self.helper.make_place(current)
				place = get_crom_object(place_data)
			owner.residence = place
			data['_owner_locations'].append(place_data)
		if owner_record.get('own_auth_p'):
			content = owner_record['own_auth_p']
			owner.referred_to_by = vocab.Note(ident='', content=content)

		data.setdefault('_other_owners', [])
		data['_other_owners'].append(owner_record)

		# The Provenance Entry URI must not share a prefix with the object URI, otherwise
		# we run the rist of provenance entries being accidentally merged during URI
		# reconciliation as part of the prev/post sale rewriting.
		tx_uri = self.helper.prepend_uri_key(hmo.id, f'PROV-{record_id}')
		
		tx_label_args = tuple([self.helper, sale_type, 'Event', rel] + list(lot_object_key))
		tx, _ = self.related_procurement(hmo, tx_label_args, current_tx, ts, buyer=owner, previous=rev, ident=tx_uri, make_label=make_label, sales_record=sales_record)
		
		if owner_record.get('own_auth_e'):
			content = owner_record['own_auth_e']
			tx.referred_to_by = vocab.Note(ident='', content=content)

		own_info_source = owner_record.get('own_so')
		if own_info_source:
			note = vocab.SourceStatement(ident='', content=own_info_source, label=source_label)
			tx.referred_to_by = note

		ptx_data = tx_data.copy()
		ptx_data['uri'] = tx_uri
		data['_prov_entries'].append(add_crom_data(data=ptx_data, what=tx))

	def set_possible_attribute(self, obj, prop, data):
		value = get_crom_object(data)
		if not value:
			return
		uncertain = data.get('uncertain', False)
		if uncertain:
			assignment = vocab.PossibleAssignment(ident='')
			assignment.assigned_property = prop
			assignment.assigned = value
			obj.attributed_by = assignment
		else:
			setattr(obj, prop, value)

	def modifiers(self, a:dict):
		mod = a.get('attrib_mod_auth', '')
		mods = CaseFoldingSet({m.strip() for m in mod.split(';')} - {''})
		return mods

	def is_or_anon(self, data:dict):
		pi = self.helper.person_identity
		if pi.is_anonymous(data):
			mods = {m.lower().strip() for m in data.get('attrib_mod_auth', '').split(';')}
			return 'or' in mods
		return False

	def model_person_or_group(self, data:dict, a:dict, attribution_group_types, attribution_group_names, role='artist', seq_no=0, sales_record=None):
		if get_crom_object(a):
			return a
		# import pdb; pdb.set_trace()
		mods = a['modifiers']
			
		artist = self.helper.add_person(a, record=sales_record, relative_id=f'artist-{seq_no+1}', role=role)
		artist_label = a['label']
		person = get_crom_object(a)
		if '_record' in data:
			sales_record = get_crom_object(data['_record'])
	#	elif '_records' in data:
	#		sales_record = get_crom_object(data['_records'])
		if mods:
			GROUP_TYPES = set(attribution_group_types.values())
			GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}

			if mods.intersects(GROUP_MODS):
				mod_name = list(GROUP_MODS & mods)[0] # TODO: use all matching types?
				clsname = attribution_group_types[mod_name]
				cls = getattr(vocab, clsname)
				group_name = attribution_group_names[clsname]
				group_label = f'{group_name} {artist_label}'
				a['label'] = group_label
				# The group URI is just the person URI with a suffix. In any case
				# where the person is merged, the group should be merged as well.
				# For example, when if "RUBENS" is merged, "School of RUBENS" should
				# also be merged.
				group_id = a['uri'] + f'-{clsname}'
				group = cls(ident=group_id, label=group_label)
				group.identified_by = model.Name(ident='', content=group_label)
				formation = model.Formation(ident='', label=f'Formation of {group_label}')
				formation.influenced_by = person
				group.formed_by = formation
				# add referred_to_by, to groups that have mods 
				if '_records' in data:
					if len(sales_record) > 1:
						group.referred_to_by = sales_record
					else:
						group.referred_to_by = sales_record[0]
				elif '_record' in data:
					group.referred_to_by = sales_record

				# preceed goupil_object_id, if found, over pi_record_no
				if a.get('goupil_object_id'):
					id_number = a['goupil_object_id']
				else:
					id_number = data['pi_record_no']
				group_uri_key = ('GROUP', 'PI', id_number, f'{role}Group')
				group_data = {
					'uri': group_id,
					'uri_keys': group_uri_key,
					'modifiers': mods,
					'label': group_label,
					'referred_to_by': sales_record
				}
				add_crom_data(group_data, group)
				data['_organizations'].append(group_data)
				return group_data

		add_crom_data(a, artist)
		return a

	def uncertain_artist_or_style(self, people:dict):
		if len(people) != 2:
			return False
		
		names = [p.get('auth_name') for p in people]
		if not names[0]:
			return False

		if names[0] != names[1]:
			return False
		
		mods = set([frozenset(p['modifiers']) for p in people])
		expected = {frozenset({'or'}), frozenset({'manner of', 'style of', 'or'})}
		return mods == expected

	def populate_original_object_visual_item(self, data, object_data, original_hmo, sales_record, original_label, seq_no):

		title = original_hmo.identified_by[0].content
		vi_uri = original_hmo.id + '-VisItem'
		vi = model.VisualItem(ident=vi_uri)
		vidata = {
			'uri': vi_uri,
			'referred_to_by': [sales_record],
		}
		if title:
			vidata['label'] = f'Visual work of “{title}”'
			vidata['names'] = [(title,{'referred_to_by': [sales_record]})]
		
		objgenre = object_data['genre'] if 'genre' in object_data else None
		objsubject = object_data['subject'] if 'subject' in object_data else None

		
		subject_genre = self.helper.services['subject_genre']
		subject_genre_style = self.helper.services['subject_genre_style']

		for prop, mapping in subject_genre.items():
			key = ', '.join((objsubject,objgenre)) if objsubject else objgenre
			try:
				aat_terms = mapping[key]
				for label, aat_url in aat_terms.items():
					t = model.Type(ident=aat_url, label=label)
					setattr(vi, prop, t)
			except:
				pass

		for prop, mapping in subject_genre_style.items():
			key = ', '.join((objsubject,objgenre)) if objsubject else objgenre
			try:
				aat_terms = mapping[key]
				for label, aat_url in aat_terms.items():
					t = model.Type(ident=aat_url, label=label)
					t.classified_as = model.Type(ident='http://vocab.getty.edu/aat/300015646', label='Styles and Periods (hierarchy name)')
					setattr(vi, prop, t)
			except:
				pass
		
		data[seq_no]['_visual_item'] = add_crom_data(data=vidata, what=vi)
		original_hmo.shows = vi

	def model_object_influence(self, data, people, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=False):
		STYLE_OF = attribution_modifiers['style of']
		COPY_AFTER = attribution_modifiers['copy after']
		NON_ARTIST_MODS = COPY_AFTER | STYLE_OF
		GROUP_TYPES = set(attribution_group_types.values())
		GROUP_MODS = {k for k, v in attribution_group_types.items() if v in GROUP_TYPES}
		non_artist_assertions = people

		if '_record' not in data:
			sales_record = get_crom_objects(data.get('_records', []))
		else:
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
			a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='NonArtist', sales_record=sales_record)
			artist_label = a_data.get('label')
			person = get_crom_object(a_data)

			mods = a_data['modifiers']
			if 'attrib_mod_auth' in a_data and a_data.get('attrib_mod_auth', '') != '':
				verbatim_mod = a_data.get('attrib_mod_auth', '')
			elif 'attrib_mod' in a_data and a_data.get('attrib_mod', '') != '' :
				verbatim_mod = a_data.get('attrib_mod', '')
				
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
				if isinstance(sales_record, list):
					for sale in sales_record:
						assignment.used_specific_object = sale
				else:
					assignment.used_specific_object = sales_record
				assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mod)
				assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
			elif COPY_AFTER.intersects(mods):
				cls = type(hmo)
				# The original object URI is just the object URI with a suffix. When URIs are
				# reconciled during prev/post sale rewriting, this will allow us to also reconcile
				# the URIs for the original object (of which there should be at most one per object)
				original_id = hmo.id + '-Original'
				original_label = f'Original of {hmo_label}'
				original_hmo = cls(ident=original_id, label=original_label)
				
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
					assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mod)
					assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
				else:
					prod_event.influenced_by = original_hmo
				# import pdb; pdb.set_trace()
				data['_original_objects'].append(add_crom_data(data={'uri': original_id}, what=original_hmo))
				if 'object' in data:
					self.populate_original_object_visual_item(data['_original_objects'], data['object'], original_hmo, sales_record, original_label, seq_no)
			else:
				warnings.warn(f'Unrecognized non-artist attribution modifers: {mods}')

	def model_object_artists(self, data, people, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=False):
		FORMERLY_ATTRIBUTED_TO = attribution_modifiers['formerly attributed to']
		POSSIBLY = attribution_modifiers['possibly by']
		UNCERTAIN = attribution_modifiers['uncertain']
		ATTRIBUTED_TO = attribution_modifiers['attributed to']
		EDIT_BY = attribution_modifiers['edit by']

		event_uri = prod_event.id
		# import pdb; pdb.set_trace()
		if '_record' not in data:
			sales_record = get_crom_objects(data.get('_records', []))
			if len(sales_record) > 1:
				with open("number_of_records.txt", "w") as file:
					file.write("multiple records found for record: " + data['pi_record_no'] + "\n")

					for s in sales_record:
						file.write(s.identified_by[0].content + "\n")
		else:
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
		# HERE IS THE GROUP FLAG??
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
				a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='Artist', sales_record=sales_record)
				artist_label = a_data.get('label') # TODO: this may not be right for groups
				person = get_crom_object(a_data)
				verbatim_mods = a_data.get('attrib_mod_auth', '')

				if ATTRIBUTED_TO.intersects(mods):
					attrib_assignment_classes = [model.AttributeAssignment]
					attrib_assignment_classes.append(vocab.PossibleAssignment)
					assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
					assignment._label = f'Possibly by {artist_label}'
					person.attributed_by = assignment
					assignment.assigned_property = 'member_of'
					assignment.assigned = person
					assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mods)
					assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
				else:
					person.member_of = artist_group
		else:
			for seq_no, a_data in enumerate(artists):
				mods = a_data['modifiers']
				if EDIT_BY.intersects(mods):
					# goupil only attribution modifier that's modelled seperately and not a sub event of the production
					continue
				# import pdb; pdb.set_trace()
				uncertain = all_uncertain
				verbatim_mods = a_data.get('attrib_mod_auth', '')
				attribute_assignment_id = self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,Artist-{seq_no}')
				## HERE MAYBE GROUP IS MODELED
				a_data = self.model_person_or_group(data, a_data, attribution_group_types, attribution_group_names, seq_no=seq_no, role='Artist', sales_record=sales_record)
				artist_label = a_data.get('label') # TODO: this may not be right for groups
				person = get_crom_object(a_data)
				attrib_assignment_classes = [model.AttributeAssignment]
				subprod_path = self.helper.make_uri_path(*a_data["uri"])
				subevent_id = event_uri + f'-{subprod_path}'
				if UNCERTAIN.intersects(mods):
					if POSSIBLY.intersects(mods):
						attrib_assignment_classes.append(vocab.PossibleAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
						assignment._label = f'Possibly by {artist_label}'
						if isinstance(sales_record, list):
							for sale in sales_record:
								assignment.used_specific_object = sale
						else:
							assignment.used_specific_object = sales_record
						assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mods)
						assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
					else:
						attrib_assignment_classes.append(vocab.ProbableAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Probably attributed to {artist_label}')
						assignment._label = f'Probably by {artist_label}'
						if isinstance(sales_record, list):
							for sale in sales_record:
								assignment.used_specific_object = sale
						else:
							assignment.used_specific_object = sales_record
						assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mods)
						assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')

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
					if isinstance(sales_record, list):
						for sale in sales_record:
							assignment.used_specific_object = sale
					else:
						assignment.used_specific_object = sales_record
					assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mods)
					assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
				else:
					if uncertain or ATTRIBUTED_TO.intersects(mods):
						attrib_assignment_classes.append(vocab.PossibleAssignment)
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=attribute_assignment_id, label=f'Possibly attributed to {artist_label}')
						prod_event.attributed_by = assignment
						assignment.assigned_property = 'carried_out_by'
						assignment.assigned = person
						if isinstance(sales_record, list):
							for sale in sales_record:
								assignment.used_specific_object = sale
						else:
							assignment.used_specific_object = sales_record
						assignment.referred_to_by = vocab.Note(ident='', content=verbatim_mods)
						assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
					else:
						attribute_assignment_id = self.helper.prepend_uri_key(prod_event.id, f'ASSIGNMENT,NonArtist-{seq_no}')
						ident = person.__dict__['_label'] +artist_label
						assignment = vocab.make_multitype_obj(*attrib_assignment_classes, ident=ident, label='')
						prod_event.attributed_by = assignment
						assignment.assigned_property = 'carried_out_by'
						assignment.assigned = person
						if isinstance(sales_record, list):
							for sale in sales_record:
								assignment.used_specific_object = sale
						else:
							assignment.used_specific_object = sales_record
						assignment.carried_out_by = self.helper.static_instances.get_instance('Group', 'knoedler')
						subevent = model.Production(ident=subevent_id, label=f'Production sub-event for {artist_label}')
						subevent.carried_out_by = person
						prod_event.part = subevent

	def select_county(self, data):
		if data['auction_of_lot']['catalog_number'][:2] == "B-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Belgium')
		if data['auction_of_lot']['catalog_number'][:2] == "Br":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_British')
		if data['auction_of_lot']['catalog_number'][:2] == "N-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Dutch')
		if data['auction_of_lot']['catalog_number'][:2] == "F-":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_British')
		if data['auction_of_lot']['catalog_number'][:2] == "D-2":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_German')
		if data['auction_of_lot']['catalog_number'][:2] == "SC":
			return self.helper.static_instances.get_instance('LinguisticObject', 'db-sales_Sandi')

	def model_artists_with_modifers(self, data:dict, hmo, attribution_modifiers, attribution_group_types, attribution_group_names):
		'''Add modeling for artists as people involved in the production of an object'''
		# sales_record = get_crom_object(data['_record'])
		# import pdb; pdb.set_trace()
		data.setdefault('_organizations', [])
		data.setdefault('_original_objects', [])
		
		try:
			hmo_label = f'{hmo._label}'
		except AttributeError:
			hmo_label = 'object'

		STYLE_OF = attribution_modifiers['style of']
		ATTRIBUTED_TO = attribution_modifiers['attributed to']
		COPY_AFTER = attribution_modifiers['copy after']
		COPY_BY = attribution_modifiers['copy by']
		POSSIBLY = attribution_modifiers['possibly by']
		UNCERTAIN = attribution_modifiers['uncertain']
		FORMERLY_MODS = attribution_modifiers['formerly attributed to']
		COPY_BY = attribution_modifiers['copy by']

		# The production event URI is just the object URI with a suffix. When URIs are
		# reconciled during prev/post sale rewriting, this will allow us to also reconcile
		# the URIs for the production events (of which there should only be one per object)
		event_uri = hmo.id + '-Production'
		prod_event = model.Production(ident=event_uri, label=f'Production event for {hmo_label}')
		hmo.produced_by = prod_event
		if "help_sales" in data:
			hmo.referred_to_by = self.select_county(data)
		artists = data.get('_artists', [])
		for a in artists:
			# here it adds properties like label, modifiers, pi_record_no
			self.add_properties(data, a)

		# 1. Remove "copy by/after" modifiers when in the presence of "manner of; style of". This combination is not meaningful, and the intended semantics are preserved by keeping only the style assertion (with the understanding that every "copy by" modifier has a paired "copy after" in another artist record, and vice-versa)
		for a in artists:
			mods = a['modifiers']
			if STYLE_OF.intersects(mods):
				for COPY in (COPY_BY, COPY_AFTER):
					if COPY.intersects(mods):
						a['modifiers'] -= COPY.intersection(mods)

		NON_ARTIST_MODS = COPY_AFTER | STYLE_OF
		ARTIST_NON_GROUP_MODS = FORMERLY_MODS | COPY_BY | {'attributed to'}
		artist_assertions = []
		non_artist_assertions = []
		for a in artists:
			mods = a['modifiers']
			if NON_ARTIST_MODS.intersects(mods):
				non_artist_assertions.append(a)
				if ARTIST_NON_GROUP_MODS.intersects(mods):
					# these have both artist and non-artist assertions
					artist_assertions.append(a)
			else:
				artist_assertions.append(a)

		# 4. Check for the special case of "A or style of A"
		uncertain = False
		if self.uncertain_artist_or_style(artists):
			artist = artists[0]['label']
			uncertain = True
			note = f'Record indicates certainty that this object was either created by {artist} or was created in the style of {artist}'
			hmo.referred_to_by = vocab.Note(ident='', content=note)
		
		# 2--3
		self.model_object_influence(data, non_artist_assertions, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=uncertain)

		# 5
		self.model_object_artists(data, artist_assertions, hmo, prod_event, attribution_modifiers, attribution_group_types, attribution_group_names, all_uncertain=uncertain)
		
		# data['_artists'] is what's pulled out by the serializers
		data['_artists'] = [a for a in artists if not self.is_or_anon(a)]
		return data

	def model_people_as_possible_group(self, people, tx_data, data, object_key, label, store_key='_other_owners', mod_key='auth_mod_a'):
		'''
		Takes an array of crom-object-containing dicts containing Person objects (people)
		and returns an array of people that should be used in its place according to
		the modifier values in each dict's mod_key field. Also returns the set of
		modifier values.
		
		If all modifiers are 'or', then the set of people are set as members of a
		new Group, and that group is returned as the single stand-in for the set of
		people.
		
		tx_data is the crom-containing dict for a related prov entry.
		
		If a group is created, the people records will be stored in data[store_key].
		
		object_key and label will be used in constructing a descriptive string
		for the group: 'Group containing the {label.lower()} of {object_key}'.
		For example, label='buyer' and object_key='B-340 0291 (1820-07-19)'.
		'''
		all_mods = {m.lower().strip() for a in people for m in a.get(mod_key, '').split(';')} - {''}
		group = (all_mods == {'or'}) # the person is *one* of the named people, model as a group
		if group:
			names = []
			for person_data in people:
				if len(person_data['identifiers']):
					names.append(person_data['identifiers'][0].content)
				else:
					names.append(person_data['label'])
			group_name = ' OR '.join(names)
			if tx_data: # if there is a prov entry (e.g. was not withdrawn)
				current_tx = get_crom_object(tx_data)
				# The person group URI is just the provenance entry URI with a suffix.
				# In any case where the provenance entry is merged, the person group
				# should be merged as well.
				group_uri = current_tx.id + f'-{label}Group'
				group_data = {
					'uri': group_uri,
				}
			else:
				pi_record_no = data['pi_record_no']
				group_uri_key = ('GROUP', 'PI', pi_record_no, f'{label}Group')
				group_uri = self.helper.make_proj_uri(*group_uri_key)
				group_data = {
					'uri_keys': group_uri_key,
					'uri': group_uri,
				}
			g_label = f'Group containing the {label.lower()} of {object_key}'
			g = vocab.UncertainMemberClosedGroup(ident=group_uri, label=g_label)
			g.identified_by = model.Name(ident='', content=group_name)
			import pdb; pdb.set_trace()
			
			for person_data in people:
				person = get_crom_object(person_data)
				person.member_of = g
				data['_other_owners'].append(person_data)
			people = [add_crom_data(group_data, g)]
		return people, all_mods
