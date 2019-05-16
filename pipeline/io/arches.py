import os
import os.path
import hashlib

import requests
from bonobo.config import Configurable, Option
from pipeline.util import ExclusiveValue

class FileWriter(Configurable):
	directory = Option(default="output")

	def __call__(self, data: dict):
		d = data['_OUTPUT']
		uuid = data['uuid']
		dr = os.path.join(self.directory, data['_ARCHES_MODEL'])
		with ExclusiveValue(dr):
			if not os.path.exists(dr):
				os.mkdir(dr)
		ddr = os.path.join(dr, uuid)
		with ExclusiveValue(ddr):
			if not os.path.exists(ddr):
				os.mkdir(ddr)
			h = hashlib.md5(d.encode('utf-8')).hexdigest()
			fn = os.path.join(ddr, "%s.json" % (h,))
			if not os.path.exists(fn):
				fh = open(fn, 'w')
				fh.write(d)
				fh.close()
			return data

class ArchesWriter(Configurable):
	endpoint = Option(default="http://localhost:8001/resources/")
	auth_endpoint = Option(default="http://localhost:8001/o/token/")
	username = Option(default="admin")
	password = Option(default="admin")
	client_id = Option(default="OaGs0HfnBNd2VpI4Hnrc8nhOSTbnV1Q3O1CPjlX6")

	current_auth = ""
	refresh_token = ""

	def get_auth(self):
		resp = requests.post(self.auth_endpoint, data={'username': self.username,
			'password': self.password, 'grant_type': 'password',
			'client_id': self.client_id})
		data = resp.json()
		if 'access_token' in data:
			self.current_auth = data['access_token']
			self.refresh_token = data['refresh_token']
		else:
			print(data)

	def __call__(self, data: dict):
		# Grab serialized JSON-LD and send it to the endpoint, with Auth
		if not self.current_auth:
			self.get_auth()
		headers = {"Authorization": "Bearer %s" % self.current_auth,
			"Accept": "application/ld+json"}

		print(data['_OUTPUT'])
		ep = self.endpoint + ("%s/%s" % (data['_ARCHES_MODEL'], data['uuid']))
		resp = requests.put(ep, headers=headers,data=data['_OUTPUT'])
		# XXX if this gets denied, retry after using the refresh token

		print(resp.text)
		return resp


