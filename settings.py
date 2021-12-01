import os

arches_models = {
	'Event': 'event',
	'Group': 'group',
	'Place': 'place',
	'ProvenanceEntry': 'provenance',
	'Series': 'series',
	'SaleActivity': 'activity',
	'Activity': 'auction',
	'AuctionOfLot': 'auction_of_lot',
	'Bidding': 'bidding',
	'Drawing': 'auction_of_lot',
	'HumanMadeObject': 'object',
	'Inventorying': 'inventorying',
	'LinguisticObject': 'textual_work',
	'Person': 'person',
	'Phase': 'phase',
	'Set': 'set',
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
pipeline_service_files_base_path = os.environ.get('GETTY_PIPELINE_SERVICE_FILES_PATH', data_path)
output_file_path = os.environ.get('GETTY_PIPELINE_OUTPUT', '/data2/output')
DEBUG = os.environ.get('GETTY_PIPELINE_DEBUG', True)
SPAM = os.environ.get('GETTY_PIPELINE_VERBOSE', False)

gpi_engine = 'sqlite:///%s/gpi.sqlite' % (data_path,)
raw_engine = 'sqlite:///%s/raw_gpi.sqlite' % (data_path,)

def project_data_path(project_name):
	return os.path.join(data_path, project_name)

def pipeline_project_service_files_path(project_name):
	path = os.path.join(pipeline_service_files_base_path, project_name)
	return path
