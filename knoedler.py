#!/usr/bin/env python3 -B

import sys, os
from sqlalchemy import create_engine
import bonobo
import itertools
import bonobo_sqlalchemy

from pipeline.nodes.basic import AddArchesModel, AddFieldNames, Serializer, deep_copy, Offset, add_uuid, Trace
from pipeline.projects.knoedler.data import *
from pipeline.projects.knoedler.linkedart import *
from pipeline.io.file import FileWriter
from pipeline.io.arches import ArchesWriter
from pipeline.linkedart import make_la_person
from settings import *

PROJECT_NAME = "knoedler"
UID_TAG_PREFIX = f'tag:getty.edu,2019:digital:pipeline:{PROJECT_NAME}:REPLACE-WITH-UUID#'

# Set up environment
def get_services():
    return {
    	'trace_counter': itertools.count(),
        'aat': create_engine(aat_engine),
        'gpi': create_engine(gpi_engine),
 		'uuid_cache': create_engine(uuid_cache_engine),
 		'raw': create_engine(raw_engine)
    }

### Pipeline

if DEBUG:
	LIMIT     = os.environ.get('GETTY_PIPELINE_LIMIT', 10)
	PACK_SIZE = 10
	SRLZ = Serializer(compact=False)
	WRITER = FileWriter(directory=output_file_path)
	# WRITER = ArchesWriter()
else:
	LIMIT     =10000000
	PACK_SIZE =10000000
	SRLZ = Serializer(compact=True)
	WRITER = FileWriter(directory=output_file_path)
	# WRITER = ArchesWriter()



def add_sales(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT * from knoedler_purchase_info', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="purchase_info", field_names=all_names),
		AddArchesModel(model=arches_models['Acquisition']),
		add_uuid,
		add_purchase_people,
		add_purchase_thing,
		add_ownership_phase_purchase,
		make_la_purchase,
		SRLZ,
		WRITER
	)

	phases = graph.add_chain(
		fan_object_phases,
		AddArchesModel(model=arches_models['Phase']),
		make_la_phase,
		SRLZ,
		WRITER,
		_input=add_ownership_phase_purchase
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=phases.output)

	acqs = graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT * from knoedler_sale_info', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="sale_info", field_names=all_names),
		AddArchesModel(model=arches_models['Acquisition']),
		add_uuid,
		add_sale_people,
		add_sale_thing, # includes adding reference to phase it terminates
		make_la_sale,
		SRLZ,
		WRITER
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=acqs.output)

def add_missing(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('''
			SELECT pi_record_no, object_id, inventory_event_id, sale_event_id, purchase_event_id
			FROM knoedler
			WHERE inventory_event_id NOT NULL
			''',
			engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		find_raw,
		AddFieldNames(key="raw", field_names=all_names),
		# bonobo.PrettyPrinter(),
		make_missing_purchase_data,
		make_missing_shared
	)

	graph.add_chain(
		make_missing_purchase,
		AddArchesModel(model=arches_models['Acquisition']),
		#bonobo.PrettyPrinter(),
		make_la_purchase,
		SRLZ,
		WRITER,
		_input=make_missing_shared
	)

	# This actually makes /all/ the inventory activities
	graph.add_chain(
		make_inventory,
		AddArchesModel(model=arches_models['Activity']),
		make_la_inventory,
		SRLZ,
		# bonobo.PrettyPrinter(),
		WRITER,
		_input=make_missing_shared
	)

def add_pre_post(graph):
	chain1 = graph.add_chain(
		bonobo_sqlalchemy.Select('''
			SELECT pp.rowid, pp.previous_owner_uid, pp.object_id, p.person_ulan, p.person_label
			FROM knoedler_previous_owners AS pp
				JOIN gpi_people as p ON (p.person_uid = pp.previous_owner_uid)
			''', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
			AddFieldNames(key="prev_post_owners", field_names=all_names),
			add_prev_prev
	)

	chain2 = graph.add_chain(
		bonobo_sqlalchemy.Select('''
			SELECT pp.rowid, pp.post_owner_uid, pp.object_id, p.person_ulan, p.person_label
			FROM
				knoedler_post_owners AS pp
				JOIN gpi_people as p ON (p.person_uid = pp.post_owner_uid)
			''',
			engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
			AddFieldNames(key="prev_post_owners", field_names=all_names),
	)

	for cin in [chain1.output, chain2.output]:
		graph.add_chain(
			AddArchesModel(model=arches_models['Acquisition']),
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
		AddArchesModel(model=arches_models['HumanMadeObject']),
		add_uuid,
		make_objects_names,
		make_objects_dims,
		make_objects_tags_ids,
		make_objects_artists,
		make_la_object,
		SRLZ,
		WRITER
	)

	visitems = graph.add_chain(
		deep_copy,
		AddArchesModel(model=arches_models['VisualItem']),
		make_la_vizitem,
		SRLZ,
		WRITER,
		_input = make_objects_artists
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=visitems.output)

def add_people(graph):
	people = graph.add_chain(
		bonobo_sqlalchemy.Select('''
			SELECT DISTINCT peeps.*
			FROM
				gpi_people AS peeps
				JOIN gpi_people_names AS names ON (peeps.person_uid = names.person_uid)
				JOIN gpi_people_names_references AS ref ON (names.person_name_id = ref.person_name_id)
			WHERE
				ref.source_record_id LIKE "KNO%"
			''',
			engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		AddFieldNames(key="gpi_people", field_names=all_names),
		AddArchesModel(model=arches_models['Person']),
		add_uuid,
		add_person_names,
		add_person_aat_labels,
		clean_dates,
		make_la_person,	
		SRLZ,
		WRITER
	)

	if DEBUG and SPAM:
		graph.add_chain(print_jsonld, _input=people.output)

def add_documents(graph):
	graph.add_chain(
		bonobo_sqlalchemy.Select('SELECT DISTINCT stock_book_no FROM knoedler ORDER BY stock_book_no', engine='gpi', limit=LIMIT, pack_size=PACK_SIZE),
		make_stock_books,
		AddArchesModel(model=arches_models['LinguisticObject']),
		add_uuid,
		make_la_book,

		fan_pages,
		AddArchesModel(model=arches_models['LinguisticObject']),
		add_uuid,
		make_la_page,

		fan_rows,
		AddArchesModel(model=arches_models['LinguisticObject']),
		add_uuid,
		make_la_row
	)

	# create subsequent branches
	for xin in [make_la_book, make_la_page, make_la_row]:
		out = graph.add_chain(
			SRLZ,
			WRITER,
			_input = xin
		)
		if DEBUG and SPAM:
			graph.add_chain(print_jsonld, _input=out.output)


def get_graph():
	graph = bonobo.Graph()

	# Sales
	if not DEBUG or 1:
		add_sales(graph)

	# Here we do both missing purchases and inventory events
	if not DEBUG or 1:
		add_missing(graph)

	# Pre/Post owners
	if not DEBUG or 1:
		add_pre_post(graph)

	# Objects
	if not DEBUG or 1:
		add_objects(graph)

	# People
	if not DEBUG or 1:
		add_people(graph)

	# Documents
	if not DEBUG or 1:
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

