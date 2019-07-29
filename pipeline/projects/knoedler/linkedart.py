from bonobo.config import use
from pipeline.util.cleaners import ymd_to_datetime

from cromulent import model, vocab
from cromulent.model import factory
from pipeline.linkedart import add_crom_data
factory.auto_id_type = 'uuid'
vocab.add_art_setter()

from pipeline.projects.knoedler import UID_TAG_PREFIX

vocab.register_aat_class("Clock", {"parent": model.HumanMadeObject, "id": "300041575", "label": "Clock"})
vocab.register_aat_class("Cards", {"parent": model.HumanMadeObject, "id":"300211294", "label": "Playing Cards"})
dimTypes = {300055624: vocab.Diameter, 300055644: vocab.Height, 300055647: vocab.Width}
dimUnits = {300379100: vocab.instances["inches"], 300379098: vocab.instances["cm"]}

# Here we take the data that has been collected and map it into Linked Art
# This should just be mapping, not data collection, manipulation or validation

def _book_label(book):
	return "Knoedler Stock Book %s" % book
def _page_label(book, page):
	return "Knoedler Stock Book %s, Page %s" % (book, page)
def _row_label(book, page, row):
	return "Knoedler Stock Book %s, Page %s, Row %s" % (book, page, row)
def _row_uid(book, page, row):
	return f'{UID_TAG_PREFIX}K-ROW-{book}-{page}-{row}'
def _page_uid(book, page):
	return f'{UID_TAG_PREFIX}K-PAGE-{book}-{page}'
def _book_uid(book):
	return f'{UID_TAG_PREFIX}K-BOOK-{book}'
def _uid_uri(uid):
	uid = urllib.parse.quote(uid)
	return f'{UID_TAG_PREFIX}{uid}'


def make_la_book(data: dict):
	book = vocab.AccountBook(ident=_book_uid(data['identifier']))
	book._label = _book_label(data['identifier'])
	ident = vocab.LocalNumber()
	ident.content = str(data['identifier'])
	book.identified_by = ident
	booknum = int(data['identifier'])
	d = vocab.SequencePosition()
	d.value = booknum
	d.unit = vocab.instances['numbers']
	book.dimension = d
	return add_crom_data(data=data, what=book)

def make_la_page(data: dict):
	page = vocab.Page(ident=_page_uid(data['parent']['identifier'], data['identifier']))
	page._label = _page_label(data['parent']['identifier'], data['identifier'])
	ident = vocab.LocalNumber()
	ident.content = str(data['identifier'])
	page.identified_by = ident
	pagenum = int(data['identifier'])
	d = vocab.SequencePosition()
	d.value = pagenum
	d.unit = vocab.instances['numbers']
	page.dimension = d

	# XXX This should go through a HumanMadeObject for consistency
	# with Sales
	if 'image' in data:
		img = vocab.DigitalImage()
		imgid = model.Identifier()
		imgid.content = data['image']
		img.identified_by = imgid
		page.representation = img

	if data['heading']:
		# This is a transcription of the heading of the page
		# Meaning it is part of the page linguistic object
		l = vocab.Heading()
		l.content = data['heading']
		page.part = l
	if data['subheading']:
		# Transcription of the subheading of the page
		l = vocab.SubHeading()
		l.content = data['subheading']
		page.part = l

	book = model.LinguisticObject(ident=_book_uid(data['parent']['identifier']))
	book._label = _book_label(data['parent']['identifier'])
	page.part_of = book

	return add_crom_data(data=data, what=page)


def make_la_row(data: dict):
	row = model.LinguisticObject(ident=_row_uid(data['parent']['parent']['identifier'], data['parent']['identifier'], data['identifier']))
	row._label = _row_label(data['parent']['parent']['identifier'], data['parent']['identifier'], data['identifier'])

	rownum = int(data['identifier'])
	d = vocab.SequencePosition()
	d.value = rownum
	d.unit = vocab.instances['numbers']
	row.dimension = d

	ident = vocab.LocalNumber()
	ident.content = data['star_id']
	row.identified_by = ident
	pi = vocab.LocalNumber()
	pi.content = data['pi_id']
	row.identified_by = pi
	if data['description']:
		note1 = vocab.Note()
		note1.content = data['description']
		row.referred_to_by = note1
	if data['working']:
		note2 = vocab.Note()
		note2.content = data['working']
		row.referred_to_by = note2
	if data['verbatim']:
		note3 = vocab.Note()
		note3.content = data['verbatim']
		row.referred_to_by = note3

	page = model.LinguisticObject(ident=_page_uid(data['parent']['parent']['identifier'], data['parent']['identifier']))
	page._label = _page_label(data['parent']['parent']['identifier'], data['parent']['identifier'])
	row.part_of = page
	return add_crom_data(data=data, what=row)

###
### Labels will be commented out as resource-instance in Arches won't accept them
### and adding label to the model won't export, plus doesn't work
### with resource-instance-list, as there's one label per list and
### the -list UI is much much nicer for editors
###

@use('vocab_type_map')
def make_la_object(data: dict, vocab_type_map=None):
	clsname = vocab_type_map.get(data['object_type'], None)
	if clsname:
		cls = getattr(vocab, clsname)
	else:
		cls = model.HumanMadeObject
		print("Could not match object type %s" % data['object_type'])

	what = cls(ident=_uid_uri(data['uid']), art=1)

	for dv in data['dimensions']:
		ds = vocab.DimensionStatement()
		ds.content =dv['value']
		# add source as part_of, as this is transcription
		for s in dv['sources']:
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			ds.referred_to_by = l
		what.referred_to_by = ds

	for dm in data['materials']:
		ds = vocab.MaterialStatement()
		ds.content = dm['value']
		# add source as part_of, as this is transcription
		for s in dm['sources']:
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			ds.referred_to_by = l
		what.referred_to_by = ds

	for n in data['names']:
		name = vocab.Title()
		if n['pref']:
			what._label = n['value']
			name.classified_as = vocab.instances['primary']
		name.content = n['value']

		for s in n['sources']:
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			name.referred_to_by = l
		what.identified_by = name

	if not hasattr(what, 'label') and hasattr(what, 'identified_by'):
		what._label = what.identified_by[0].content
	else:
		what._label = "Unlabeled Object"

	for dim in data['dimensions_clean']:
		d = dimTypes.get(dim['type'], model.Dimension)()
		d.value = dim['value']
		d.unit = dimUnits[dim['unit']]
		for s in dim['sources']:
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			d.referred_to_by = l
		what.dimension = d

	for iv in data['knoedler_ids']:
		idv = vocab.LocalNumber()
		idv.content = str(iv)
		what.identified_by = idv

	prod = model.Production()
	what.produced_by = prod
	# XXX Check prior attribution model
	pref = []
	former = []
	for a in data['artists']:
		if a['pref']:
			pref.append(a)
		else:
			former.append(a)

	for a in pref:
		# This is currently always a person. Need to process Workshop of X
		# XXX FIXME this is the arches issue with multiple resource-instance models
		#who = model.Person(ident="urn:uuid:%s" % a['uuid'])

		# XXX This should use ulan_type mapping?
		who = model.Actor(ident=_uid_uri(a["uid"]))
		who._label = a['label']
		prod.carried_out_by = who

		for s in a['sources']:
			# Can't associate with the relationship directly (as it's a source for carried_out_by)
			# So just add to the Production, which is still true, and 99.9% of the time is sufficient
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			prod.referred_to_by = l

	for a in former:
		fprod = model.Production()
		who = model.Person(ident=_uid_uri(a["uid"]))
		who._label = a['label']
		fprod.carried_out_by = who
		aa = model.AttributeAssignment()
		what.attributed_by = aa
		aa.assigned = fprod
		# XXX FIXME: aa.classified_as = produced_by
		for s in a['sources']:
			# Conversely, this is correct, as the LO refers to the AA carried out by Knoedler
			l = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]))
			l._label = _row_label(s[1], s[2], s[3])
			aa.referred_to_by = l

	add_vi = False
	for t in data['tags']:
		aatv = f"aat:{t['aat']}"
		aaturi = f"http://vocab.getty.edu/aat/{t['aat']}" 
		if t['type'] == 'classified_as':
			# classification of the object
			curr = [x.id for x in what.classified_as]
			if not aaturi in curr:
				# this is otherwise from object_type map
				what.classified_as = model.Type(ident=aaturi, label=t['label'])
		elif t['type'] == 'technique':
			# technique for production
			prod.technique = model.Type(ident=aaturi, label=t['label'])
		elif t['type'] == 'subject':
			# XXX NOTE WELL -- Matt put these under subject but they're just classifications
			add_vi = True
		elif t['type'] == 'depicts':
			# what depicted in VI
			add_vi = True
		elif t['type'] == 'object_material':
			# material of object
			what.made_of = model.Material(ident=aaturi, label=t['label'])
		elif t['type'] == 'support_material':
			# material of support
			supp = vocab.SupportPart()
			supp.made_of = model.Material(ident=aaturi, label=t['label'])
			what.part = supp
		elif t['type'] == 'style':
			# style
			add_vi = True
		else:
			print("UNKNOWN TAG TYPE: %s" % t['type'])

	if add_vi:
		# This will be built in a different fork
		vi = model.VisualItem(ident=_uid_uri(data["uid"]+'-vizitem'))
		what.shows = vi

	return add_crom_data(data=data, what=what)


def make_la_vizitem(data: dict):
	vi = model.VisualItem(ident=_uid_uri(data["uid"]+'-vizitem'))
	add_vi = False
	for t in data['tags']:
		aaturi = "http://vocab.getty.edu/aat/%s" % t['aat']
		if t['type'] == 'depicts':
			# what depicted in VI
			vi.represents = model.Type(ident=aaturi, label=t['label'])
			add_vi = True
		elif t['type'] == 'subject':
			# XXX NOTE WELL -- Matt put these under subject but they're just classifications
			vi.classified_as = model.Type(ident=aaturi, label=t['label'])
			add_vi = True
		elif t['type'] == 'style':
			# style
			vi.style = model.Type(ident=aaturi, label=t['label'])
			add_vi = True

	if add_vi:
		return add_crom_data(data=data, what=vi)
	else:
		# This None will terminate processing on this branch, as there is no VI
		return None

def make_la_purchase(data: dict):

	what = model.Acquisition(ident= _uid_uri(data['uid']))
	try:
		what._label = "Purchase of %s by %s" % (data['objects'][0]['label'], data['buyers'][0]['label'])
	except IndexError:
		if not data['buyers']:
			what._label = "Purchase of %s" % data['objects'][0]['label']
		else:
			# No objects??
			print("Acquisition record (%s) has no objects! " % data['uid'])
			what._label = "Purchase?"

	for o in data['objects']:
		what.transferred_title_of = model.HumanMadeObject(ident=_uid_uri(o["uid"]), label=o['label'])
		if 'phase_info' in o:
			what.initiated = model.Phase(ident=_uid_uri(['phase_info']+'-phase'))
	for b in data['buyers']:
		# XXX Could [indeed very very likely to] be Group
		if b['type'] in ["Person", "Actor"]:
			try:
				what.transferred_title_to = model.Person(ident=_uid_uri(b['uid']), label=b['label'])
			except:
				print("Could not build person in make_la_purchase: %r" % b)
				# ????
		else:
			try:
				what.transferred_title_to = model.Group(ident=_uid_uri(b['uid']), label=b['label'])
			except:
				print("Could not build group in make_la_purchase: %r" % b)
				# What to do??

	for s in data['sellers']:
		if s['type'] in ['Person', 'Actor']:
			what.transferred_title_from = model.Person(ident=_uid_uri(s['uid']), label=s['label'])
		else:
			what.transferred_title_from = model.Group(ident=_uid_uri(s['uid']), label=s['label'])
		if s['mod']:
			print("NOT HANDLED MOD: %s" % s['mod'])

	if data['dec_amount']:
		p = model.Payment()
		am = model.MonetaryAmount()
		am._label = "%s %s" % (data['amount'], data['currency'])
		am.value = data['dec_amount']
		am.currency = model.Currency(ident="http://vocab.getty.edu/aat/%s" % data['currency_aat'], label=data['currency'])
		p. paid_amount = am
		for pt in what.transferred_title_to:
			p.paid_from = pt
		if hasattr(what, "transferred_title_from"):
			for pf in what.transferred_title_from:
				p.paid_to = pf
		what.part = p

	if data['year']:
		t = model.TimeSpan()
		nm = model.Name()
		nm.content = "%s %s %s" % (data['year'], data['month'], data['day'])
		t.identified_by = nm
		t.begin_of_the_begin = ymd_to_datetime(data['year'], data['month'], data['day'])
		t.end_of_the_end = ymd_to_datetime(data['year'], data['month'], data['day'], which="end")
		what.timespan = t
	for s in data['sources']:
		what.referred_to_by = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]), label=_row_label(s[1], s[2], s[3]))

	if data['note']:
		n = vocab.Note()
		n.content = data['note']
		what.referred_to_by = n
	if data['k_note']:
		n = vocab.Note()
		n.content = data['k_note']
		what.referred_to_by = n

	return add_crom_data(data=data, what=what)

def make_la_phase(data: dict):

	phase = vocab.OwnershipPhase(ident=_uid_uri({data['uid']}+"-phase"))
	try:
		phase._label = "Ownership Phase of %s" % data['object_label']
	except:
		phase._label = "Ownership Phase of unknown object"

	what = model.HumanMadeObject(ident=_uid_uri(data['object_uid']), label=data['object_label'])
	phase.phase_of = what
	pi = model.PropertyInterest()
	pi.interest_for = what

	if 'p_year' in data and data['p_year']:
		ts = model.TimeSpan()
		ts.begin_of_the_begin = ymd_to_datetime(data['p_year'], data['p_month'], data['p_day'])
		phase.timespan = ts
		# End comes from sale
		if 's_type' in data:
			stype = data['s_type']
			if stype != "Sold":
				# XXX Not sure what to do with these, see below!
				print("Non 'sold' transaction (%s) is end of ownership phase for %s" % (stype, data['object_uuid'])  )
			else:
				ts.end_of_the_end = ymd_to_datetime(data['s_year'], data['s_month'], data['s_day'], which="end")
		nm = model.Name()
		nm.content = "%s %s %s to %s %s %s" % (data.get('p_year', '????'), data.get('p_month', '??'),
			data.get('p_day', '??'), data.get('s_year', '????'), data.get('s_month', '??'), data.get('s_day', '??'))
		ts.identified_by = nm

	for b in data['buyers']:
		if b['type'] in ["Person", "Actor"]:
			who = model.Person(ident=_uid_uri(b['uid']), label=b['label'])
		else:
			who = model.Group(ident=_uid_uri(b['uid']), label=b['label'])
		pi.claimed_by = who

		if b['share'] != 1.0:
			pip = model.PropertyInterest()
			pip.claimed_by = who
			pip.interest_for = what
			pi.interest_part = pip
			d = model.Dimension()
			pip.dimension = d
			d.value = b['share']
			d.unit = vocab.instances['percent']

	return add_crom_data(data=data, what=phase)

def make_la_sale(data: dict):

	if data['type'] != "Sold":
		print("Matt's notes say not to generate acquisitions for non-Sold, but not what to do instead")
		print("Generating it, and we can sort it out later")

	what = model.Acquisition(ident=_uid_uri(data['uid']))
	what._label = "Sale of %s by %s" % (data['objects'][0]['label'], data['sellers'][0]['label'])
	for o in data['objects']:
		what.transferred_title_of = model.HumanMadeObject(ident=_uid_uri(o['uid']), label=o['label'])
		if 'phase' in o:
			what.terminated = model.Phase(ident=_uid_uri(o['phase']+"-phase"))

	for b in data['sellers']:
		if b['type'] in ["Person", "Actor"]:
			what.transferred_title_to = model.Person(ident=_uid_uri(b['uid']), label=b['label'])
		else:
			what.transferred_title_to = model.Group(ident=_uid_uri(b['uid']), label=b['label'])
		if b['share'] != 1.0:
			do_property_interest = True
			print("NOT HANDLED SHARES FOR SALE %s" % data['uid'])
	for s in data['buyers']:
		if s['type'] in ['Person', 'Actor']:
			what.transferred_title_from = model.Person(ident=_uid_uri(s['uid']), label=s['label'])
		else:
			what.transferred_title_from = model.Group(ident=_uid_uri(s['uid']), label=s['label'])
		if s['mod']:
			print("NOT HANDLED MOD: %s" % s['mod'])
		if s['auth_mod']:
			print("NOT HANDLED AUTH_MOD: %s" % s['auth_mod'])

	if data['dec_amount']:
		p = model.Payment()
		am = model.MonetaryAmount()
		am._label = "%s %s" % (data['amount'], data['currency'])
		am.value = data['dec_amount']
		am.currency = model.Currency(ident="http://vocab.getty.edu/aat/%s" % data['currency_aat'], label=data['currency'])
		p.paid_amount = am
		if hasattr(what, 'transferred_title_to'):
			for pt in what.transferred_title_to:
				p.paid_from = pt
		if hasattr(what, 'transferred_title_from'):
			for pf in what.transferred_title_from:
				p.paid_to = pf
		what.part = p

	if data['year']:
		t = model.TimeSpan()
		nm = model.Name()
		nm.content = "%s %s %s" % (data['year'], data['month'], data['day'])
		t.identified_by = nm
		t.begin_of_the_begin = ymd_to_datetime(data['year'], data['month'], data['day'])
		t.end_of_the_end = ymd_to_datetime(data['year'], data['month'], data['day'], which="end")
		what.timespan = t
	for s in data['sources']:
		what.referred_to_by = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]), \
			label=_row_label(s[1], s[2], s[3]))

	if data['note']:
		n = vocab.Note()
		n.content = data['note']
		what.referred_to_by = n
	if data['share_note']:
		n = vocab.Note()
		n.content = data['share_note']
		what.referred_to_by = n

	return add_crom_data(data=data, what=what)

def make_la_inventory(data: dict):

	what = vocab.Inventorying(ident=_uid_uri(data['uid']))
	date = "%s-%s-%s" % (data['year'], data['month'], data['day'])
	what._label = "Inventory taking for %s on %s" % (data['objects'][0]['label'], date)

	o = data['objects'][0]
	obj = model.HumanMadeObject(ident=_uid_uri(o['uid']), label=o['label'])
	what.used_specific_object = obj

	buy = data['buyers'][0]
	who = model.Group(ident=_uid_uri(buy['uid']), label=buy['label'])
	what.carried_out_by = who

	if data['year']:
		t = model.TimeSpan()
		nm = model.Name()
		nm.content = "%s %s %s" % (data['year'], data['month'], data['day'])
		t.identified_by = nm
		t.begin_of_the_begin = ymd_to_datetime(data['year'], data['month'], data['day'])
		t.end_of_the_end = ymd_to_datetime(data['year'], data['month'], data['day'], which="end")
		what.timespan = t

	for s in data['sources']:
		what.referred_to_by = model.LinguisticObject(ident=_row_uid(s[1], s[2], s[3]), \
			label=_row_label(s[1], s[2], s[3]))

	if data['note']:
		n = vocab.Note()
		n.content = data['note']
		what.referred_to_by = n

	return add_crom_data(data=data, what=what)

def make_la_prev_post(data: dict):

	what = model.Acquisition(ident=_uid_uri(data['uid']))
	what._label = "%s of object by %s" % (data['acq_type'], data['owner_label'])

	if data['owner_type'] in ["Person", "Actor"]:
		who = model.Person(ident=_uid_uri(data['owner_uid']), label=data['owner_label'])
	else:
		who = model.Group(ident=_uid_uri(data['owner_uid']), label=data['owner_label'])

	if data['acq_type'] == 'purchase':
		what.transferred_title_to = who
	else:
		what.transferred_title_from = who

	# XXX Should we capture labels
	obj = model.HumanMadeObject(ident=_uid_uri(data['object_uid']))
	what.transferred_title_of = obj

	if 'prev_uuid' in data and data['prev_uid']:
		prev = model.Acquisition(ident=_uid_uri(data['prev_uid']))
		# XXX p183
		what.occurs_after = prev

	return add_crom_data(data=data, what=what)
