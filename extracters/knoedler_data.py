# Knoedler Extracters

from bonobo.config import use
from extracters.cleaners import date_cleaner, share_parse
from .basic import fetch_uuid, get_actor_type, get_aat_label
import copy

# Here we extract the data from the sources and collect it together

all_names = {
	'gpi_people': ['uid', 'label', 'ulan', 'birth', 'death', 'active_early', 'active_late', 
		'nationality', 'aat_nationality_1', 'aat_nationality_2', 'aat_nationality_3'],
	'knoedler': ['star_id', 'pi_record_no', 'stock_book_no', 'page_number', 'row_number', 
		'description', 'subject', 'genre', 'object_type', 'materials', 'dimensions', 'folio', 
		'working_note', 'verbatim_notes', 'link', 'main_heading', 'subheading', 'auction_sale', 
		'auction_purchase','object_id', 'share_note', 'sale_event', 'purchase_event', 'inventory_event'],
	'purchase_info': ['uid', 'amount', 'currency', 'k_amount', 'k_curr', 'note', 'k_note', 
		'year', 'month', 'day', 'dec_amount', 'currency_aat', 'dec_k_amount', 'k_curr_aat'],
	'sale_info': ['uid', 'type', 'year', 'month', 'day', 'share_amount', 'share_currency', 
		'share_note', 'amount', 'currency', 'note', 'dec_amount', 'currency_aat', 
		'dec_share_amount', 'share_currency_aat'],
	'raw': ['orig_file', 'star_id', 'pi_id', 'stock_book_id', 'knoedler_id', 'page_num', 
		'row_num', 'consign_num', 'consign_name', 'consign_loc', 'consign_ulan_id', 
		'artist_name_1', 'artist_name_auth_1', 'nationality_1', 'attrib_mod_1', 'attrib_mod_auth_1', 'star_rec_no_1', 'artist_ulan_1', 
		'artist_name_2', 'artist_name_auth_2', 'nationality_2', 'attrib_mod_2', 'attrib_mod_auth_2', 'star_rec_no_2', 'artist_ulan_2', 
		'title', 'description', 'subject', 'genre', 'object_type', 'materials', 'dimensions', 
		'entry_date_year', 'entry_date_month', 'entry_date_day', 'sale_date_year', 
		'sale_date_month', 'sale_date_day', 'purchase_amount', 'purchase_currency', 
		'purchase_note', 'knoedler_purchase_amount', 'knoedler_purchase_currency', 'knoedler_purchase_note',
		'price_amount', 'price_currency', 'price_note', 'share_amount', 'share_currency', 'share_note',
		'seller_name_1', 'seller_loc_1', 'seller_mod_1', 'seller_name_auth_1', 'seller_auth_loc_1', 'seller_auth_mod_1', 'seller_ulan_1',
		'seller_name_2', 'seller_loc_2', 'seller_mod_2', 'seller_name_auth_2', 'seller_auth_loc_2', 'seller_auth_mod_2', 'seller_ulan_2',
		'joint_owner_1', 'joint_owner_sh_1', 'joint_owner_ulan_1', 
		'joint_owner_2', 'joint_owner_sh_2', 'joint_owner_ulan_2',
		'joint_owner_3', 'joint_owner_sh_3', 'joint_owner_ulan_3', 'transaction',
		'buyer_name_1', 'buyer_loc_1', 'buyer_mod_1', 'buyer_name_auth_1', 'buyer_addr_auth_1', 'buyer_auth_mod_1', 'buyer_ulan_1',
		'buyer_name_2', 'buyer_loc_2', 'buyer_mod_2', 'buyer_name_auth_2', 'buyer_addr_auth_2', 'buyer_auth_mod_2', 'buyer_ulan_2',
		'folio',
		'prev_owner_1', 'prev_owner_loc_1', 'prev_owner_ulan_1',
		'prev_owner_2', 'prev_owner_loc_2', 'prev_owner_ulan_2',
		'prev_owner_3', 'prev_owner_loc_3', 'prev_owner_ulan_3',
		'prev_owner_4', 'prev_owner_loc_4', 'prev_owner_ulan_4',				
		'prev_owner_5', 'prev_owner_loc_5', 'prev_owner_ulan_5',
		'prev_owner_6', 'prev_owner_loc_6', 'prev_owner_ulan_6',
		'prev_owner_7', 'prev_owner_loc_7', 'prev_owner_ulan_7',
		'prev_owner_8', 'prev_owner_loc_8', 'prev_owner_ulan_8',
		'prev_owner_9', 'prev_owner_loc_9', 'prev_owner_ulan_9',
		'post_owner', 'post_owner_ulan', 'present_loc_geog', 'present_loc_inst', 'present_loc_acc', 'present_loc_note',
		'present_owner_ulan', 'working_notes', 'verbatim_notes', 'link', 'heading', 'subheading',
		'MATT_object_id', 'MATT_inventory_id', 'MATT_sale_event_id', 'MATT_purchase_event_id'
		],
		'prev_post_owners': ['rowid', 'owner_uid', 'object_uid', 'owner_ulan', 'owner_label']
}


# ~~~~ Object Tables ~~~~

@use('gpi')
@use('uuid_cache')
def make_objects(object_id, gpi=None, uuid_cache=None):

	data = {'uid': object_id}
	fields = ['recno', 'book', 'page', 'row', 'subject', 'genre', 'object_type', 'materials', 'dimensions']
	for f in fields[4:]:
		data[f] = []
	s = '''
		SELECT pi_record_no, stock_book_no, page_number, row_number, subject, genre, object_type, materials, dimensions
		FROM knoedler
		WHERE object_id="%s"
		''' % object_id
	res = gpi.execute(s)
	for r in res:
		# multiple rows per object with different information possible
		row = dict(zip(fields, r))
		uu = fetch_uuid(row['recno'], uuid_cache)
		source = [row['recno'], uu, row['book'], row['page'], row['row']]
		for lo in ['subject', 'genre', 'materials', 'dimensions']:
			if row[lo]:
				if data[lo]:
					found = 0
					for l in data[lo]:
						if l['value'] == row[lo]:
							l['sources'].append(source)
							found =1
							break
					if not found:
						data[lo].append({"value": row[lo], "sources": [source]})
				else:
					data[lo] = [{"value": row[lo], "sources": [source]}]

		if row['object_type'] and not data['object_type']:
			data['object_type'] = row['object_type']
			if data['object_type'] in ["Painting [?]", "Watercolor; Painting", "Watercolor"]:
				data['object_type'] = "Painting"
	return data

@use('uuid_cache')
@use('gpi')
def make_objects_artists(data, gpi=None, uuid_cache=None):
	object_id = data['uid']
	s = '''
		SELECT
			a.artist_uid, peep.person_label, peep.person_ulan, a.artist_attribution_mod, a.attribution_is_preferred, a.star_record_no, k.stock_book_no, k.page_number, k.row_number
		FROM
			knoedler_artists AS a
			JOIN gpi_people AS peep ON (peep.person_uid = a.artist_uid)
			JOIN knoedler AS k ON (k.star_record_no = a.star_record_no)
		WHERE
			a.object_id="%s"
		''' % object_id
	res = gpi.execute(s)

	artists = []
	artistById = {}
	for row in res:
		attr = dict(zip(["artist", "label", "ulan", "mod", "pref", 'star_id', "book", "page", "row"], row))
		if attr['mod']:
			attr['mod'] = attr['mod'].lower()
			if attr['mod'].startswith('and '):
				attr['mod'] = None

		uu = fetch_uuid("K-ROW-%s-%s-%s" % (attr['book'], attr['page'], attr['row']), uuid_cache)

		if attr['artist'] in artistById:
			# Add source
			artistById[attr['artist']]['sources'].append([ attr['star_id'], uu, attr['book'], attr['page'], attr['row']])				
			# Check mod is recorded
			if attr['mod']:
				artistById[attr['artist']]['mod'] = attr['mod']
			if attr['pref']:
				artistById[attr['artist']]['pref'] = 1
		else:
			ut = fetch_uuid(attr['artist'], uuid_cache)
			atype = get_actor_type(attr['ulan'], uuid_cache)
			who = {'uuid': ut, 'uid': attr['artist'], 'ulan': attr['ulan'], 'type': atype, 'mod': attr['mod'], 'label': attr['label'], 'pref': attr['pref'],
					'sources': [ [attr['star_id'], uu, attr['book'], attr['page'], attr['row']] ]}
			artists.append(who)
			artistById[attr['artist']] = who

	data['artists'] = artists
	return data


# These are separated out so that they can be parallelized

@use('uuid_cache')
@use('gpi')
def make_objects_dims(data, gpi=None, uuid_cache=None):
	object_id = data['uid']

	# Pull in parsed dimensions
	s = '''
		SELECT
			d.object_id, d.star_record_no, d.dimension_value, d.dimension_unit_aat, d.dimension_type_aat,
			k.stock_book_no, k.page_number, k.row_number, k.object_type
		FROM
			knoedler_dimensions AS d
			JOIN knoedler as k ON (k.star_record_no = d.star_record_no)
		WHERE d.object_id = :id
		'''
	res = gpi.execute(s, id=object_id)
	dfields = ['object_id', 'star_record_no', 'value', 'unit', 'type', 'book', 'page', 'row', 'obj_type']
	dimByType = {300055624: [], 300055644: [], 300055647: [], 300072633: [], 300055642: []}
	dims = []
	for dim in res:
		dimdata = dict(zip(dfields, dim))
		# see if we have the same value, if so don't create a new Dimension
		newdim = None
		if 'type' in dimdata:
			for d in dimByType[dimdata['type']]:
				if d['value'] == dimdata['value']:
					newdim = d
					break
		else:
			# no type for this dimension :(
			# Assert "Unknown" (aka 'size (general extent)')
			dimdata['type'] = 300055642

		if newdim is None:
			# create a new Dimension of appropriate type
			newdim = {'type': dimdata['type'], 'value': dimdata['value'], 'unit': dimdata['unit'], 'sources': []}
			dimByType[dimdata['type']].append(newdim)
			dims.append(newdim)
		# And now add reference
		uu = fetch_uuid("K-ROW-%s-%s-%s" % (dimdata['book'], dimdata['page'], dimdata['row']), uuid_cache)
		newdim['sources'].append([dimdata['star_record_no'], uu, dimdata['book'], dimdata['page'], dimdata['row']])
	data['dimensions_clean'] = dims

	return data


@use('uuid_cache')
@use('gpi')
def make_objects_names(data, gpi=None, uuid_cache=None):
	object_id = data['uid']
	# Pull in object names
	tfields = ['value', 'preferred', 'star_no', 'book', 'page', 'row']
	s = '''
		SELECT
			t.title, t.is_preferred_title, t.star_record_no, k.stock_book_no, k.page_number, k.row_number
		FROM
			knoedler_object_titles AS t
			JOIN knoedler AS k ON (t.star_record_no = k.star_record_no)
		WHERE
			t.object_id = :id
		'''
	res = gpi.execute(s, id=object_id)
	names = []
	nameByVal = {}
	for row in res:
		title = dict(zip(tfields, row))

		# XXX clean value, generate comparison value
		# SHOULD WE DO THIS? (Rob thinks yes)
		value = title['value']
		if value[0] == '"' and value[-1] == '"':
			value = value[1:-1]
			title['value'] = value
		compValue = value.lower()

		uu = fetch_uuid("K-ROW-%s-%s-%s" % (title['book'], title['page'], title['row']), uuid_cache)	
		if compValue in nameByVal:
			nameByVal[compValue]['sources'].append([title['star_no'], uu, title['book'], title['page'], title['row']])
			if title['preferred']:
				nameByVal[compValue]['value'] = title['value']
		else:
			name = {'value': title['value'], 'pref': title['preferred'], 'sources': [[title['star_no'], uu, title['book'], title['page'], title['row']]]}
			nameByVal[compValue] = name
			names.append(name)
	data['names'] = names
	return data


@use('gpi')
@use('uuid_cache')
def make_objects_tags_ids(data, gpi=None, uuid_cache=None):
	object_id = data['uid']
	# Pull in "tags"
	# knoedler_depicts_aat - https://linked.art/model/object/aboutness/#depiction
	# knoedler_materials_classified_as_aat - https://linked.art/model/object/identity/#types (n.b. from LA docs that all objects must also have aat:300133025 (works of art) in addition to any of these tags)
	# knoedler_materials_object_aat - https://linked.art/model/object/physical/#materials
	# knoedler_materials_support_aat - https://linked.art/model/object/physical/#parts (where there are multiple terms per object, attach all terms to one Part)
	# knoedler_materials_technique_aat - https://linked.art/model/provenance/production.html#roles-techniques
	# knoedler_style_aat - https://linked.art/model/object/aboutness/#style
	# knoedler_subject_classified_as_aat - http://linked.art/model/object/aboutness/#classifications

	tagMap = {"knoedler_depicts_aat": "depicts", "knoedler_materials_classified_as_aat": "classified_as",
			"knoedler_materials_object_aat": "object_material", "knoedler_materials_support_aat": "support_material",
			"knoedler_materials_technique_aat": "technique", "knoedler_style_aat": "style", 
			"knoedler_subject_classified_as_aat": "subject"}

	s = 'SELECT tag_type, aat_term FROM knoedler_object_tags WHERE object_id = :id'
	res = gpi.execute(s, id=object_id)
	tags = []
	for tag in res:
		lbl = get_aat_label(tag[1], gpi=gpi)
		tags.append({"type": tagMap[tag[0]], "aat": str(tag[1]), "label": lbl})
	data['tags'] = tags

	# Pull in Identifiers
	s = 'SELECT knoedler_number FROM knoedler_object_identifiers WHERE object_id = :id'
	res = gpi.execute(s, id=object_id)
	ids = [str(x[0]) for x in res]
	data['knoedler_ids'] = ids

	# Add a UUID for the visual item, separate from the object
	data['vizitem_uuid'] = fetch_uuid(object_id + '-vizitem', uuid_cache)

	return data

###
### XXX Shouldn't there be make_objects_materials() ?
### This would use the google spreadsheet to turn material stmt into parts + materials
###


# ~~~~ Acquisitions ~~~~

@use('gpi')
@use('uuid_cache')
def add_purchase_people(thing: dict, gpi=None, uuid_cache=None):	
	s = '''
		SELECT
			b.purchase_buyer_uid, b.purchase_buyer_share, p.person_label, p.person_ulan
		FROM
			knoedler_purchase_buyers AS b
			JOIN gpi_people as p ON (b.purchase_buyer_uid = p.person_uid)
		WHERE
			b.purchase_event_id = :id
		'''
	buyers = []
	res = gpi.execute(s, id=thing['uid'])

	shares = False
	for row in res:
		uu = fetch_uuid(row[0], uuid_cache)
		atype = get_actor_type(str(row[3]), uuid_cache)
		buyers.append({'uuid': uu, 'ulan': row[3], 'type': atype, 'label': str(row[2]), 'share': row[1]})
		if row[1] < 1.0:
			shares = True
	thing['buyers'] = buyers

	s = '''
		SELECT
			s.purchase_seller_uid, s.purchase_seller_auth_mod, p.person_label, p.person_ulan
		FROM
			knoedler_purchase_sellers AS s
			JOIN gpi_people as p ON (s.purchase_seller_uid = p.person_uid)
		WHERE
			s.purchase_event_id = :id
		'''
	sellers = []
	res = gpi.execute(s, id=thing['uid'])
	for row in res:
		uu = fetch_uuid(row[0], uuid_cache)
		atype = get_actor_type(str(row[3]), uuid_cache)
		sellers.append({'uuid': uu, 'ulan': row[3], 'type': atype, 'label': str(row[2]), 'mod': row[1]})
	thing['sellers'] = sellers
	return thing

@use('gpi')
@use('uuid_cache')
def add_purchase_thing(thing: dict, gpi=None, uuid_cache=None):
	s = '''
		SELECT
			k.star_record_no, k.stock_book_no, k.page_number, k.row_number, k.object_id, n.title
		FROM
			knoedler AS k
			JOIN knoedler_object_titles AS n ON (k.object_id = n.object_id)
		WHERE
			 n.is_preferred_title = 1
			 AND k.purchase_event_id = :id
		'''
	res = gpi.execute(s, id=thing['uid'])

	thing['sources'] = []
	thing['objects'] = []
	for row in res:
		uu = fetch_uuid("K-ROW-%s-%s-%s" % (row[1], row[2], row[3]), uuid_cache)
		thing['sources'].append([str(row[0]), uu, row[1], row[2], row[3]])
		ouu = fetch_uuid(str(row[4]), uuid_cache)	
		phase_uu = fetch_uuid(row[0] + '-ownership-phase', uuid_cache)		
		pi = {'uuid': phase_uu, 'star_id': row[0]}
		thing['objects'].append({'uid': str(row[4]), 'uuid': ouu, 'label': row[5], 'phase_info': pi})
	return thing

@use('gpi')
@use('uuid_cache')
def add_ownership_phase_purchase(data: dict, gpi=None, uuid_cache=None):
	# Coming from a purchase event, need to collect sale data
	# current data is only knoedler_purchase_info
	puid = data['uid']
	s = '''
		SELECT
			k.star_record_no, k.sale_event_id, k.object_id, s.transaction_type, s.sale_date_year, s.sale_date_month, s.sale_date_day
		FROM
			knoedler AS k
			JOIN knoedler_sale_info AS s ON (s.sale_event_id = k.sale_event_id)
		WHERE
			k.purchase_event_id = :id
		'''

	gr = gpi.execute(s, id=puid)
	res = gr.fetchall()

	if res:
		duids = {}
		for o in data['objects']:
			duids[o['uid']] = o

		for row in res:
			# 1 acquisition of n objects makes n ownership phases
			# Align ownership phase per object
			oid = row[2]
			if oid in duids:
				phase_uu = duids[oid]['phase_info']['uuid']
				pi = {'uuid': phase_uu, 'star_id': row[0], 'sale_id': row[1], 's_type': row[3], 
					's_year': row[4], 's_month': row[5], 's_day': row[6]}
				duids[oid]['phase_info'] = pi		
	# This might not get called, if no sale recorded
	# Downstream code needs to check that the transaction type is Sold to end?
	return data

def fan_object_phases(data: dict):

	# Copy the relevant info for phase off of Purchase

	for o in data['objects']:
		new = copy.copy(o['phase_info'])
		new['object_uuid'] = o['uuid']
		new['object_label'] = o['label']
		new['p_year'] = data['year']
		new['p_month'] = data['month']
		new['p_day'] = data['day']
		new['buyers'] = data['buyers']
		yield new
	return None

@use('gpi')
@use('uuid_cache')
def add_sale_people(thing: dict, gpi=None, uuid_cache=None):	
	s = '''
		SELECT
			s.sale_buyer_uid, p.person_label, p.person_ulan, s.sale_buyer_mod, s.sale_buyer_auth_mod
		FROM
			knoedler_sale_buyers AS s
			JOIN gpi_people AS p ON (s.sale_buyer_uid = p.person_uid)
		WHERE
			s.sale_event_id = :id
		'''
	buyers = []
	res = gpi.execute(s, id=thing['uid'])
	for row in res:
		uu = fetch_uuid(row[0], uuid_cache)
		atype = get_actor_type(str(row[2]), uuid_cache)
		buyers.append({'uuid': uu, 'ulan': row[2], 'type': atype, 'label': str(row[1]), 'mod': row[3], 'auth_mod': row[4]})
	thing['buyers'] = buyers

	s = '''
		SELECT
			s.sale_seller_uid, p.person_label, p.person_ulan, s.sale_seller_share
		FROM
			knoedler_sale_sellers AS s
			JOIN gpi_people AS p ON (s.sale_seller_uid = p.person_uid)
		WHERE
			s.sale_event_id = :id
		'''
	sellers = []
	res = gpi.execute(s, id=thing['uid'])
	for row in res:
		uu = fetch_uuid(row[0], uuid_cache)
		atype = get_actor_type(str(row[2]), uuid_cache)
		sellers.append({'uuid': uu, 'type': atype, 'ulan': row[2], 'label': str(row[1]), 'share': row[3]})
	thing['sellers'] = sellers
	return thing

@use('gpi')
@use('uuid_cache')
def add_sale_thing(thing: dict, gpi=None, uuid_cache=None):
	s = '''
		SELECT
			k.star_record_no, k.stock_book_no, k.page_number, k.row_number, k.object_id, n.title
		FROM
			knoedler AS k
			JOIN knoedler_object_titles AS n ON (k.object_id = n.object_id)
		WHERE
			n.is_preferred_title = 1
			AND k.sale_event_id = :id
		'''
	res = gpi.execute(s, id=thing['uid'])
	thing['sources'] = []
	thing['objects'] = []
	for row in res:
		uu = fetch_uuid("K-ROW-%s-%s-%s" % (row[1], row[2], row[3]), uuid_cache)
		thing['sources'].append([str(row[0]), uu, row[1], row[2], row[3]])
		ouu = fetch_uuid(str(row[4]), uuid_cache)
		phase = fetch_uuid(row[0] + "-ownership-phase", uuid_cache)
		thing['objects'].append({'uid': str(row[4]), 'uuid':ouu, 'label': row[5], 'ends_phase': phase})

	return thing

@use('raw')
def find_raw(*row, raw=None):
	(recno, obj_id, inv_id, sale_id, purch_id) = row
	s = 'SELECT * FROM raw_knoedler WHERE pi_record_no = :id'
	res = raw.execute(s, id=recno)
	t = list(res.fetchone())
	t.extend([obj_id, inv_id, sale_id, purch_id])
	return tuple(t)

def make_missing_purchase_data(data: dict, uuid_cache=None):
	# This is the raw data from the export, as Matt screwed up the processing for purchases :(
	# Entry date is the inventory date

	def process_amount(amnt):
		if not amnt:
			return amnt
		amnt = amnt.replace('[', '')
		amnt = amnt.replace(']', '')
		amnt = amnt.replace('?', '')
		amnt = amnt.strip()
		try:
			return float(amnt)
		except:
			print("Could not process %s when decimalizing" % amnt)
			return None

	currencies = {
		"pounds": 300411998,
		"dollars": 300411994,
		"francs": 300412016,
		"marks": 300412169,
		"dollar": 300411994,
		"florins": 300412160,
		"thalers": 300412168,
		"lire": 300412015,
		"Swiss francs":300412001,
		None: None
	}


	seller1 = {'name': data['seller_name_1'], 'loc': data['seller_loc_1'], 'loc_auth': data['seller_auth_loc_1'], 
		'mod': data['seller_mod_1'], 'mod_auth': data['seller_auth_mod_1'], 'ulan': data['seller_ulan_1']}
	seller2 = {'name': data['seller_name_2'], 'loc': data['seller_loc_2'], 'loc_auth': data['seller_auth_loc_2'], 
		'mod': data['seller_mod_2'], 'mod_auth': data['seller_auth_mod_2'], 'ulan': data['seller_ulan_2']}

	knoedler = {'label': "Knoedler", 'type': 'Group', 'share': None, 'uuid': 'c19d4bfc-0375-4994-98f3-0659cffc40d8'}

	# default to Group, as that's most likely
	joint_1 = {'label': data['joint_owner_1'], 'type': 'Group', 'share': share_parse(data['joint_owner_sh_1']), 'ulan': data['joint_owner_ulan_1']}
	joint_2 = {'label': data['joint_owner_2'], 'type': 'Group', 'share': share_parse(data['joint_owner_sh_2']), 'ulan': data['joint_owner_ulan_2']}
	joint_3 = {'label': data['joint_owner_3'], 'type': 'Group', 'share': share_parse(data['joint_owner_sh_3']), 'ulan': data['joint_owner_ulan_3']}

	buyers = [knoedler]
	k_share = 1.0
	if joint_1['share'] is not None:
		k_share -= joint_1['share']
		buyers.append(joint_1)
	if joint_2['share'] is not None:
		k_share -= joint_2['share']
		buyers.append(joint_2)		
	if joint_3['share'] is not None:
		k_share -= joint_3['share']
		buyers.append(joint_3)	
	knoedler['share'] = k_share

	return {
		'uid': 'rob-%s-%s' % (data['pi_id'], data['MATT_inventory_id']),
		"amount": data['purchase_amount'],
		'dec_amount': process_amount(data['purchase_amount']),
		'currency_aat': currencies[data['purchase_currency']],
		"currency": data['purchase_currency'],
		'k_amount': data['knoedler_purchase_amount'],
		'k_curr': data['knoedler_purchase_currency'],
		'dec_k_amount': process_amount(data['knoedler_purchase_amount']),
		'k_curr_aat': currencies[data['knoedler_purchase_currency']],
		"note": data['purchase_note'],
		'k_note': data['knoedler_purchase_note'],
		'year': data['entry_date_year'],
		'month': data['entry_date_month'],
		'day': data['entry_date_day'],
		"sellers": [seller1, seller2],
		'buyers': buyers,
		"object_id": data['MATT_object_id'],
		"inventory_id": data['MATT_inventory_id'],
		"pi_id": data['pi_id'],
		"star_id": data['star_id'],
		"transaction_type": data['transaction'],
		'sources': [],
		'objects': [],
		'sale_event_id': data['MATT_sale_event_id'],
		'purchase_event_id': data['MATT_purchase_event_id']
	}


@use('gpi')
@use('uuid_cache')
def make_missing_shared(data: dict, gpi=None, uuid_cache=None):
	# Generate our UUID
	data['uuid'] = fetch_uuid(data['uid'], uuid_cache)

	# Build the object reference (just UUID and label)
	s = "SELECT title FROM knoedler_object_titles WHERE is_preferred_title=1 AND object_id = :id"
	res = gpi.execute(s, id=data['object_id'])
	label = res.fetchone()[0]
	data['objects'] = [{'uuid': fetch_uuid(data['object_id'], uuid_cache), 'label': label}]

	# String to Int / Date conversions
	if not data['day']:
		data['day'] = 1
	else:
		data['day'] = int(data['day'])
	if not data['month']:
		data['month'] = 1
	else:
		data['month'] = int(data['month'])			
	if data['year']:
		data['year'] = int(data['year'])

	# Add Source
	s = '''
		SELECT
			k.stock_book_no, k.page_number, k.row_number
		FROM
			knoedler as k
		WHERE
			k.pi_record_no = :id
		'''
	res =  gpi.execute(s, id=data['pi_id'])
	(book, page, row) = res.fetchone()
	uu = fetch_uuid("K-ROW-%s-%s-%s" % (book, page, row), uuid_cache)
	data['sources'] = [[None, uu, book, page, row]]

	return data


@use('gpi')
@use('uuid_cache')
def make_missing_purchase(data: dict, gpi=None, uuid_cache=None):
	if data['transaction_type'] == "Unsold" and not data['amount'] and not data['note'] and not data['sellers'][0]['name']:
		# Inventory
		return None	
	else:

		# Clean up Sellers
		new_sellers = []
		for sell in data['sellers']:
			if sell['name'] is None and sell['ulan'] is None:
				continue
			
			if sell['ulan'] is not None:
				s = "SELECT person_uid, person_label FROM gpi_people WHERE person_ulan = :ulan"
				res = gpi.execute(s, ulan=sell['ulan'])
			else:
				s = '''
					SELECT
						DISTINCT names.person_uid, names.person_name as uid
					FROM
						gpi_people_names_references AS ref
						JOIN gpi_people_names AS names ON (ref.person_name_id = names.person_name_id)
					WHERE
						ref.source_record_id = :id
						AND names.person_name = :name
					'''
				res = gpi.execute(s, id="KNOEDLER-%s" % data['star_id'], name=sell['name'])

			rows = res.fetchall()
			if len(rows) == 1:
				(puid, plabel) = rows[0]
				puu = fetch_uuid(puid, uuid_cache)
				ptyp = get_actor_type(sell['ulan'], uuid_cache)
			else:
				print("Data: %r" % data)
				print("Seller: %r" % sell)
				print("Sent: %s %s Got: %s" % (sell['name'], sell['ulan'], len(rows)))
				raise ValueError("No matching person")
			new_sellers.append({'type': ptyp, 'uuid': puu, 'label': plabel, 'mod': ''})
		data['sellers'] = new_sellers

		# Aaaaand ... clean up buyers
		for buy in data['buyers']:
			if not 'uuid' in buy:
				# find type based on ULAN, otherwise default to Group			
				if buy['ulan'] is not None:
					ptyp = get_actor_type(buy['ulan'], uuid_cache, default="Group")
					s = "SELECT person_uid FROM gpi_people WHERE person_ulan = :ulan"
					res = gpi.execute(s, ulan=buy['ulan'])
				else:
					ptyp = "Group"
					s = '''
					SELECT
						DISTINCT names.person_uid
					FROM
						gpi_people_names_references AS ref
						JOIN gpi_people_names as names ON (ref.person_name_id = names.person_name_id)
					WHERE
						ref.source_record_id = :id
						AND names.person_name = :name
					'''
					res = gpi.execute(s, id="KNOEDLER-%s" % data['star_id'], name=buy['name'])
				rows = res.fetchall()
				if len(rows) == 1:
					puid = rows[0][0]
					puu = fetch_uuid(puid, uuid_cache)
				else:
					print("Data: %r" % data)
					print("buyer: %r" % buy)
					print("Sent: %s %s Got: %s" % (buy['name'], buy['ulan'], len(rows)))
					continue				
				buy['type'] = ptyp
				buy['uuid'] = puu
				buy['uid'] = puid

		return data


def make_inventory(data: dict):
	if data['transaction_type'] != "Unsold" or data['amount'] or data['note'] or data['sellers'][0]['name']:
		# purchase
		return None	
	else:
		# pass through to make_la_inventory
		return data


@use('gpi')
def add_prev_prev(data: dict, gpi=None):

	s = '''
		SELECT rowid
		FROM knoedler_previous_owners
		WHERE
			rowid < :row
			AND object_id = :id
		ORDER BY rowid ASC
		LIMIT 1
		'''
	res = gpi.execute(s, row=int(data['rowid']), id=data['object_uid'])
	rows = res.fetchall()
	if len(rows):
		data['prev_uid'] = "%s_sale" % rows[0][0]
	else:
		data['prev_uid'] = None
	return data

@use('uuid_cache')
def fan_prev_post_purchase_sale(data: dict, uuid_cache=None):
	# One owner = two acquisitions... transfer to, transfer from

	data['owner_uuid'] = fetch_uuid(data['owner_uid'], uuid_cache)
	data['owner_type'] = get_actor_type(data['owner_ulan'], uuid_cache)
	data['object_uuid'] = fetch_uuid(data['object_uid'], uuid_cache)
	if 'prev_uid' in data and data['prev_uid']:
		data['prev_uuid'] = fetch_uuid(data['prev_uid'], uuid_cache)

	for t in ['purchase', 'sale']:
		data = data.copy()
		data['uid'] = "%s_%s" % (data['rowid'], t)
		data['uuid'] = fetch_uuid(data['uid'], uuid_cache)
		data['acq_type'] = t
		if t == 'sale':
			data['prev_uid'] = "%s_purchase" % data['rowid']
			data['prev_uuid'] = fetch_uuid("%s_purchase" % data['rowid'], uuid_cache)

		yield data


# ~~~~ People Tables ~~~~

@use('gpi')
@use('uuid_cache')
def add_person_names(thing: dict, gpi=None, uuid_cache=None):
	s = 'SELECT * FROM gpi_people_names WHERE person_uid = :id'
	thing['names'] = []
	for r in gpi.execute(s, id=thing['uid']):
		name = [r[0]]
		nid = r[2]
		# s2 = 'SELECT source_record_id from gpi_people_names_references WHERE 
		#    person_name_id=%r' % (nid,)
		s2 = '''
			SELECT
				k.star_record_no, k.stock_book_no, k.page_number, k.row_number
			FROM
				knoedler AS k
				JOIN gpi_people_names_references AS ref ON (k.star_record_no = ref.source_record_id)
			WHERE
				ref.person_name_id = :id
			'''
		for r2 in gpi.execute(s2, id=nid):
			# uid, book, page, row
			val = [r2[0]]

			uu = fetch_uuid("K-ROW-%s-%s-%s" % (r2[1], r2[2], r2[3]), uuid_cache)		
			val.append(uu)
			val.append([r2[1], r2[2], r2[3]])
			name.append(val)
		thing['names'].append(name)
	return thing

@use('gpi')
def add_person_aat_labels(data: dict, gpi=None):
	if data.get('aat_nationality_1'):
		data['aat_nationality_1_label'] = get_aat_label(data['aat_nationality_1'], gpi=gpi)
	if data.get('aat_nationality_2'):
		data['aat_nationality_2_label'] = get_aat_label(data['aat_nationality_2'], gpi=gpi)
	if data.get('aat_nationality_3'):
		data['aat_nationality_4_label'] = get_aat_label(data['aat_nationality_3'], gpi=gpi)
	return data

@use('gpi')
@use('uuid_cache')
def add_person_locations(data: dict, gpi=None, uuid_cache=None):
	# XXX Should probably have referred to by from the record
	# XXX This should be a fan, not an add

	places = {}
	fields = ['loc', 'recno', 'book', 'page', 'row']
	fields2 = ['loc', 'auth', 'recno', 'book', 'page', 'row']

	s = '''
		SELECT
			s.purchase_seller_loc, k.star_record_no, k.stock_book_no, k.page_number, k.row_number
		FROM
			knoedler_purchase_sellers AS s
			JOIN knoedler as k ON (s.purchase_event_id = k.purchase_event_id)
		WHERE
			s.purchase_seller_uid = :id
		'''
	for row in gpi.execute(s, id=data['uid']):
		print(row)
		if row[0]:
			pl = dict(zip(fields, row))
			print(pl)
			# hohum, get source info...
			uu = fetch_uuid("K-ROW-%s-%s-%s" % (pl['book'], pl['page'], pl['row']), uuid_cache)
			source = [pl['recno'], uu, pl['book'], pl['page'], pl['row']]
			if pl['loc'] in places:
				places[pl['loc']].append(source)
			else:
				places[pl['loc']] = [source]

	s = '''
		SELECT
			s.sale_buyer_loc, s.sale_buyer_addr, k.star_record_no, k.stock_book_no, k.page_number, k.row_number
		FROM
			knoedler_sale_buyers AS s
			JOIN knoedler as k ON (s.sale_event_id = k.sale_event_id)
		WHERE
			sale_buyer_uid = :id
		'''
	for row in gpi.execute(s, id=data['uid']):
		if row[0] or row[1]:
			pl = dict(zip(fields2, row))
			uu = fetch_uuid("K-ROW-%s-%s-%s" % (pl['book'], pl['page'], pl['row']), uuid_cache)
			source = [pl['recno'], uu, pl['book'], pl['page'], pl['row']]
			if pl['auth'] in places:
				places[pl['auth']].append(source)
			else:
				places[pl['auth']] = [source]
			# if pl['loc'] in places, then merge refs
			if pl['loc'] in places:
				places[pl['auth']].extend(places[pl['loc']])
				del places[pl['loc']]

	data['places'] = [{"label": k, "sources": v} for (k,v) in places.items()]
	return data

def clean_dates(data: dict):
	if data.get('birth'):
		data['birth_clean'] = date_cleaner(data['birth'])
	if data.get('death'):
		data['death_clean'] = date_cleaner(data['death'])
	return data


# ~~~~ Stock Book ~~~~

def make_stock_books(*thing):
	# make baseline
	uid = thing[0]
	return {'uid': 'K-BOOK-%s' % uid, 'identifier': uid}

@use('gpi')
def fan_pages(thing: dict, gpi=None):
	# For each incoming book, find all the pages
	s = 'SELECT DISTINCT page_number, link, main_heading, subheading FROM knoedler WHERE stock_book_no = :id ORDER BY page_number'
	for r in gpi.execute(s, id=thing['identifier']):
		page = {'parent': thing, 'identifier': r[0], 'image': r[1], 'heading': r[2], 'subheading': r[3], 
			'uid': 'K-PAGE-%s-%s' % (thing['identifier'], r[0])}
		yield page

@use('gpi')
def fan_rows(thing: dict, gpi=None):
	# For each incoming book, find all the pages
	s = '''
		SELECT
			DISTINCT row_number, star_record_no, pi_record_no, description, working_note, verbatim_notes
		FROM
			knoedler
		WHERE
			stock_book_no = :book
			AND page_number = :page
		ORDER BY row_number
		'''
	for r in gpi.execute(s, book=thing['parent']['identifier'], page=thing['identifier']):
		row = {'parent': thing, 'identifier': r[0], 'star_id': r[1], 'pi_id': r[2], 'description': r[3],
			'working': r[4], 'verbatim': r[5],
			'uid': 'K-ROW-%s-%s-%s' % (thing['parent']['identifier'], thing['identifier'], r[0])}
		yield row





