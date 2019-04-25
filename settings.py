
import os

arches_models = {
	"Acquisition": "b5fdce59-2e41-11e9-b1c2-a4d18cec433a",
	"Phase": "17871ac7-2e42-11e9-87b2-a4d18cec433a",
	"Activity": "24c45975-3955-11e9-80f0-a4d18cec433a",
	"ManMadeObject": "2486c17d-2e42-11e9-bd33-a4d18cec433a",
	"VisualItem": "504dcf0a-2e42-11e9-b4e2-a4d18cec433a",
	"Person": "0b47366e-2e42-11e9-9018-a4d18cec433a",
	"LinguisticObject": "41a41e47-2e42-11e9-b5ee-a4d18cec433a"
}

aata_data_path = os.environ.get('GETTY_PIPELINE_AATA_INPUT', '/data/input/aata')
data_path = os.environ.get('GETTY_PIPELINE_INPUT', '/data/input/provenance/knoedler')
output_file_path = os.environ.get('GETTY_PIPELINE_OUTPUT', '/data2/output/provenance/knoedler')
DEBUG = os.environ.get('GETTY_PIPELINE_DEBUG', False)
SPAM = os.environ.get('GETTY_PIPELINE_VERBOSE', False)

if os.path.exists('/Users/rsanderson'):
	aat_engine = 'sqlite:////Users/rsanderson/Development/getty/provenance/matt/gpi.sqlite'
	gpi_engine = 'sqlite:////Users/rsanderson/Development/getty/provenance/matt/gpi.sqlite'
	uuid_cache_engine = 'sqlite:////Users/rsanderson/Development/getty/pipeline/uuid_cache.sqlite'
	raw_engine = 'sqlite:////Users/rsanderson/Development/getty/pipeline/data/raw_gpi.sqlite'
	output_file_path = 'output'
	DEBUG = True
	SPAM = False
else:
	aat_engine = 'sqlite:///%s/aat.sqlite' % (data_path,)
	gpi_engine = 'sqlite:///%s/gpi.sqlite' % (data_path,)
	uuid_cache_engine = 'sqlite:///%s/uuid_cache.sqlite' % (data_path,)
	raw_engine = 'sqlite:///%s/raw_gpi.sqlite' % (data_path,)

