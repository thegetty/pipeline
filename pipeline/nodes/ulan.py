

import requests
import sqlite3
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


def init_actor_type():
	c = sqlite3.connect('/Users/rsanderson/Development/getty/provenance/matt/gpi.sqlite')
	c2 = sqlite3.connect('/Users/rsanderson/Development/getty/pipeline/uuid_cache.sqlite')
	cur = c2.cursor()

	for row in c.execute("SELECT person_ulan FROM gpi_people WHERE person_ulan NOT NULL"):
		ulan = str(row[0])
		exists = cur.execute("SELECT type FROM actor_type WHERE ulan='%s'" % ulan)
		if not exists.fetchall():
			t = fetch_actor_type(ulan)
			if t:
				t = t.replace('http://schema.org/', '')
				res = cur.execute('INSERT INTO actor_type (ulan, type) VALUES ("%s", "%s")' % (ulan,t))
			c2.commit()


if __name__ == "__main__":

	# Build the actor type table
	c = sqlite3.connect('/Users/rsanderson/Development/getty/provenance/matt/gpi.sqlite')
	c2 = sqlite3.connect('/Users/rsanderson/Development/getty/pipeline/data/raw_gpi.sqlite')

	s = 'SELECT pi_record_no FROM knoedler WHERE sale_event_id IS NULL AND inventory_event_id NOT NULL'
	for row in c.execute(s):
		orig = c2.execute('SELECT * from raw_knoedler WHERE pi_record_no = "%s"' % row[0])
		print(orig.fetchone())
