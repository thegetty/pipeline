# Knoedler Extracters

from bonobo.config import use
from extracters.cleaners import date_cleaner, share_parse
from .basic import fetch_uuid, get_actor_type, get_aat_label

# Here we extract the data from the sources and collect it together

all_names = {

}

# ~~~~ Object Tables ~~~~

@use('gpi')
@use('uuid_cache')
def make_objects(object_id, gpi=None, uuid_cache=None):

	data = {'uid': object_id}
	fields = ['recno', 'book', 'page', 'row', 'subject', 'genre', 'object_type', 'materials', 'dimensions']
	for f in fields[4:]:
		data[f] = []
	s = 'SELECT pi_record_no, stock_book_no, page_number, row_number, subject, genre, object_type, materials, dimensions FROM knoedler WHERE object_id="%s"' % object_id
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


# Linguistic object lo_type:
# "description"
# "transaction_type"
# "object_type"
# "materials"
# "dimensions"
# "genre"
# "subject"
# "lot_notes"
# "hand_note"
# "format"
# "inscription"
# "present_loc_note"




