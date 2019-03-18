
import sys, os
from sqlalchemy import create_engine
import bonobo
import bonobo_sqlalchemy

sys.path.insert(0,'/Users/rsanderson/Development/getty/pipeline')
#sys.path.insert(0,'/home/rsanderson/Development/pipeline')

from extracters.basic import *
from extracters.crom import *
from extracters.arches import ArchesWriter, FileWriter
from settings import *

# Set up environment
def get_services():
    return {
        'gpi': create_engine(gpi_engine),
 		'uuid_cache': create_engine(uuid_cache_engine),
 		'raw': create_engine(raw_engine)
    }

### Pipeline

if DEBUG:
	LIMIT     = 50
	PACK_SIZE = 50
	SRLZ = Serializer(compact=False)
	WRITER = FileWriter(directory=output_file_path)
	# WRITER = ArchesWriter()
else:
	LIMIT     =10000000
	PACK_SIZE =10000000
	SRLZ = Serializer(compact=True)
	WRITER = ArchesWriter()



def add_sales(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT * from knoedler_purchase_info', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="purchase_info"),
		AddArchesModel(model='b5fdce59-2e41-11e9-b1c2-a4d18cec433a'),
		add_uuid,
		add_purchase_people,
		add_purchase_thing,
		add_ownership_phase_purchase,
		make_la_purchase,
		SRLZ,
		WRITER
	)

	graph.add_chain(
		fan_object_phases,
		AddArchesModel(model='17871ac7-2e42-11e9-87b2-a4d18cec433a'),
		make_la_phase,
		SRLZ,
		WRITER,
		_input=add_ownership_phase_purchase
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=len(graph.nodes)-1)

	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT * from knoedler_sale_info', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="sale_info"),
		AddArchesModel(model='b5fdce59-2e41-11e9-b1c2-a4d18cec433a'),
		add_uuid,
		add_sale_people,
		add_sale_thing, # includes adding reference to phase it terminates
		make_la_sale,
		SRLZ,
		WRITER
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=len(graph.nodes)-1)

def add_missing(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT pi_record_no, object_id, inventory_event_id, sale_event_id, purchase_event_id FROM knoedler WHERE inventory_event_id NOT NULL', 
			engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		find_raw,
		AddFieldNames(key="raw"),
		# bonobo.PrettyPrinter(),	
		make_missing_purchase_data,
		make_missing_shared
	)

	graph.add_chain(
		make_missing_purchase,
		AddArchesModel(model='b5fdce59-2e41-11e9-b1c2-a4d18cec433a'),
		#bonobo.PrettyPrinter(),
		make_la_purchase,
		SRLZ,
		WRITER,			
		_input=make_missing_shared
	)

	# This actually makes /all/ the inventory activities
	graph.add_chain(
		make_inventory,
		AddArchesModel(model='24c45975-3955-11e9-80f0-a4d18cec433a'),
		# bonobo.PrettyPrinter(),
		make_la_inventory,
		SRLZ,
		WRITER,
		_input=make_missing_shared
	)

def add_pre_post(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT pp.rowid, pp.previous_owner_uid, pp.object_id, p.person_ulan, p.person_label ' +\
			' FROM knoedler_previous_owners as pp, gpi_people as p ' +\
			' WHERE p.person_uid = pp.previous_owner_uid', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
			AddFieldNames(key="prev_post_owners"),
			add_prev_prev
	)
	chain1 = graph.nodes[-1]

	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT pp.rowid, pp.post_owner_uid, pp.object_id, p.person_ulan, p.person_label ' +\
			' FROM knoedler_post_owners as pp, gpi_people as p ' +\
			' WHERE p.person_uid = pp.post_owner_uid', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
			AddFieldNames(key="prev_post_owners"),
	)
	chain2 = graph.nodes[-1]

	for cin in [chain1, chain2]:
		graph.add_chain(
			AddArchesModel(model='b5fdce59-2e41-11e9-b1c2-a4d18cec433a'),
			fan_prev_post_purchase_sale,
			make_la_prev_post,
			SRLZ,
			WRITER,
			_input = cin
		)	

def add_objects(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT DISTINCT object_id FROM knoedler', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		make_objects,
		AddArchesModel(model='2486c17d-2e42-11e9-bd33-a4d18cec433a'),
		add_uuid,
		make_objects_names,
		make_objects_dims,
		make_objects_tags_ids,
		make_objects_artists,
		make_la_object,
		SRLZ,
		WRITER
	)

	graph.add_chain(
		deep_copy,
		AddArchesModel(model='504dcf0a-2e42-11e9-b4e2-a4d18cec433a'),
		make_la_vizitem,
		SRLZ,
		WRITER,
		_input = make_objects_artists
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=len(graph.nodes)-1)

def add_people(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT DISTINCT peeps.* from gpi_people as peeps, gpi_people_names_references as ref, gpi_people_names as names ' + \
			'WHERE peeps.person_uid = names.person_uid AND names.person_name_id = ref.person_name_id and ref.source_record_id like "KNO%"', \
			engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="gpi_people"),
		AddArchesModel(model='0b47366e-2e42-11e9-9018-a4d18cec433a'),
		add_uuid,
		add_person_names,
		add_person_aat_labels,
		clean_dates,
		make_la_person,	
		SRLZ,
		WRITER
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=len(graph.nodes)-1)	

def add_documents(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT DISTINCT stock_book_no FROM knoedler ORDER BY stock_book_no', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		make_stock_books,
		AddArchesModel(model='41a41e47-2e42-11e9-b5ee-a4d18cec433a'),
		add_uuid,
		make_la_book,

		fan_pages,
		bonobo.Limit(100),
		AddArchesModel(model='41a41e47-2e42-11e9-b5ee-a4d18cec433a'),
		add_uuid,
		make_la_page,

		fan_rows,
		bonobo.Limit(100),
		AddArchesModel(model='41a41e47-2e42-11e9-b5ee-a4d18cec433a'),
		add_uuid,
		make_la_row
	)

	# create subsequent branches
	for xin in [make_la_book, make_la_page, make_la_row]:
		graph.add_chain(
			SRLZ,
			WRITER,
			_input = xin
		)
		if DEBUG and SPAM:
			graph.add_chain(print_jsonld, _input=len(graph.nodes)-1)	


def get_graph():
	graph = bonobo.Graph()

	# Sales
	if 1:
		add_sales(graph)

	# Here we do both missing purchases and inventory events
	if 1:
		add_missing(graph)

	# Pre/Post owners
	if 1:
		add_pre_post(graph)

	# Objects
	if 0:
		add_objects(graph)

	# People
	if 0:
		add_people(graph)

	# Documents
	if 0:
		add_documents(graph)

	return graph


if __name__ == '__main__':
	parser = bonobo.get_argument_parser()
	with bonobo.parse_args(parser) as options:
		try:
			bonobo.run(
				get_graph(**options),
				services=get_services(**options)
			)
		except RuntimeError:
			raise ValueError()

