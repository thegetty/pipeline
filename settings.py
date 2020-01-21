import os

arches_models = {
	'Event': 'event',
	'Group': 'group',
	'Journal': 'journal',
	'Place': 'place',
	'ProvenanceEntry': 'provenance_activity',
	'Series': 'series',
	'Activity': 'auction',
	'AuctionOfLot': 'auction_of_lot',
	'Bidding': 'bidding',
	'HumanMadeObject': 'physical_thing',
	'Inventorying': 'inventorying',
	'LinguisticObject': 'textual_work',
	'Person': 'person',
	'Phase': 'phase',
	'Set': 'collection_or_set',
	'VisualItem': 'visual_work'
}

arches_endpoint = os.environ.get('GETTY_PIPELINE_ARCHES_ENDPOINT', 'http://localhost:8001/resources/')
arches_endpoint_username = os.environ.get('GETTY_PIPELINE_ARCHES_USERNAME', 'admin')
arches_endpoint_password = os.environ.get('GETTY_PIPELINE_ARCHES_PASSWORD', 'admin')
arches_auth_endpoint = os.environ.get('GETTY_PIPELINE_ARCHES_AUTH_ENDPOINT', 'http://localhost:8001/o/token/')
arches_client_id = os.environ.get('GETTY_PIPELINE_ARCHES_CLIENT_ID', 'OaGs0HfnBNd2VpI4Hnrc8nhOSTbnV1Q3O1CPjlX6')

data_path = os.environ.get('GETTY_PIPELINE_INPUT', '/data')
pipeline_tmp_path = os.environ.get('GETTY_PIPELINE_TMP_PATH', '/tmp')
pipeline_common_service_files_path = os.environ.get('GETTY_PIPELINE_COMMON_SERVICE_FILES_PATH', os.path.join(data_path, 'common'))
output_file_path = os.environ.get('GETTY_PIPELINE_OUTPUT', '/data2/output')
DEBUG = os.environ.get('GETTY_PIPELINE_DEBUG', True)
SPAM = os.environ.get('GETTY_PIPELINE_VERBOSE', False)

gpi_engine = 'sqlite:///%s/gpi.sqlite' % (data_path,)
raw_engine = 'sqlite:///%s/raw_gpi.sqlite' % (data_path,)

def project_data_path(project_name):
	return os.path.join(data_path, project_name)

def pipeline_project_service_files_path(project_name):
	path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH')
	if not path:
		path = os.path.join(data_path, project_name)
	return path
