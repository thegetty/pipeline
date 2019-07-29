
import requests
from lxml import etree

def check_ulan(ulan):
	r = requests.get('http://vocabsservices.getty.edu/ULANService.asmx/ULANGetSyncSubjectID?subjectID=%s' % ulan)
	xml = etree.XML(r.text.encode('utf-8'))
	new = xml.find('Current_Subject_ID')
	if new is not None:
		return new.text
	else:
		print(ulan)
		print(r.text)
		return ''

def fetch_actor_type(ulan):
	url = "http://vocab.getty.edu/ulan/%s-agent.jsonld" % ulan
	r = requests.get(url)
	try:
		js = r.json()
		return js['@type']
	except:
		new = check_ulan(ulan)
		if new and new != ulan:
			return fetch_actor_type(new)
		else:
			return ""
