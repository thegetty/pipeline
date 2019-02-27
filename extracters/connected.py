
from bonobo.config import Configurable, Option, use

PERSON_REFS = {}
SOURCE_REFS = {}

def record_object_uids(data):
	for a in data['artists']:
		PERSON_REFS[a['uid']] = 1
		for s in a['sources']:
			SOURCE_REFS[s[0]] = 1

	for lo in ['subject', 'genre', 'materials', 'dimensions']:
		for v in data[lo]:
			for s in v['sources']:
				SOURCE_REFS[s[0]] = 1

	return data
