#!/usr/bin/env python3

import os
import sys
import json
import uuid
import pathlib
import itertools
import settings
from flask import Flask, escape, request

class Builder:
	def __init__(self):
		self.counter = itertools.count()
		self.class_styles = {
					"HumanMadeObject": "object",
					"Place": "place",
					"Actor": "actor",
					"Person": "actor",
					"Group": "actor",
					"Type": "type",
					"MeasurementUnit": "type",
					"Currency": "type",
					"Material": "type",
					"Language": "type",
					"Name": "name",
					"Identifier": "name",
					"Dimension": "dims",
					"MonetaryAmount": "dims",			
					"LinguisticObject": "infoobj",
					"VisualItem": "infoobj",
					"InformationObject": "infoobj",
					"Set": "infoobj",
					"PropositionalObject": "infoobj",
					"Right": "infoobj",
					"PropertyInterest": "infoobj",
					"TimeSpan": "timespan",
					"Activity": "event",
					"Event": "event",
					"Birth": "event",
					"Death": "event",
					"Production": "event",
					"Destruction": "event",
					"Creation": "event",
					"Formation": "event",
					"Dissolution": "event",
					"Acquisition": "event",
					"TransferOfCustody": "event",
					"Move": "event",
					"Payment": "event",
					"AttributeAssignment": "event",
					"Phase": "event"
				}	

	def uri_to_label(self, uri):
		if uri.startswith('urn:uuid:'):
			return f'#{next(self.counter)}'
		elif uri.startswith('http://vocab.getty.edu/'):
			uri = uri.replace('http://vocab.getty.edu/', '')
			uri = uri.replace('/', ': ')
			return uri
		elif uri.startswith('https://linked.art/example/'):
			uri = uri.replace('https://linked.art/example/', '')
			uri = uri.replace('/', '')
			return uri
		else:
			print("Unhandled URI: %s" % uri)
			return uri

	def walk(self, js, curr_int, id_map, mermaid):
		if isinstance(js, dict):
			# Resource
			curr = js.get('id', f'b{next(self.counter)}')
			if curr in id_map:
				currid = id_map[curr]
			else:
				currid = "O%s" % curr_int
				curr_int += 1
				id_map[curr] = currid
			lbl = self.uri_to_label(curr)
			line = "%s(%s)" % (currid, lbl)
			if not line in mermaid:
				mermaid.append(line)
			t = js.get('type', '')
			if t:
				style = self.class_styles.get(t, '')
				if style:
					line = "class %s %s;" % (currid, style)
					if not line in mermaid:
						mermaid.append("class %s %s;" % (currid, style))
				else:
					print("No style for class %s" % t)
				line = "%s-- type -->%s_0[%s]" % (currid, currid, t)
				if not line in mermaid:
					mermaid.append(line) 			
					mermaid.append("class %s_0 classstyle;" % currid)

			n = 0
			for k,v in js.items():
				n += 1
				if k in ["@context", "id", "type"]:
					continue
				elif isinstance(v, list):
					for vi in v:
						if isinstance(vi, dict):
							(rng, curr_int, id_map) = self.walk(vi, curr_int, id_map, mermaid)
							mermaid.append("%s-- %s -->%s" % (currid, k, rng))				
						else:
							print("Iterating a list and found %r" % vi)
				elif isinstance(v, dict):
					(rng, curr_int, id_map) = self.walk(v, curr_int, id_map, mermaid)
					line = "%s-- %s -->%s" % (currid, k, rng)
					if not line in mermaid:
						mermaid.append(line)				
				else:
					if type(v) in (str,):
						# :|
						v = "\"''%s''\""% v
					line = "%s-- %s -->%s_%s(%s)" % (currid, k, currid, n, v)
					if not line in mermaid:
						mermaid.append(line)
						mermaid.append("class %s_%s literal;" % (currid, n))
			return (currid, curr_int, id_map)

	def build_mermaid(self, js):
		curr_int = 1
		mermaid = []
		id_map = {}
		mermaid.append("graph TD")
		mermaid.append("classDef object stroke:black,fill:#E1BA9C,rx:20px,ry:20px;")
		mermaid.append("classDef actor stroke:black,fill:#FFBDCA,rx:20px,ry:20px;")
		mermaid.append("classDef type stroke:red,fill:#FAB565,rx:20px,ry:20px;")
		mermaid.append("classDef name stroke:orange,fill:#FEF3BA,rx:20px,ry:20px;")
		mermaid.append("classDef dims stroke:black,fill:#c6c6c6,rx:20px,ry:20px;")
		mermaid.append("classDef infoobj stroke:#907010,fill:#fffa40,rx:20px,ry:20px")
		mermaid.append("classDef timespan stroke:blue,fill:#ddfffe,rx:20px,ry:20px")
		mermaid.append("classDef place stroke:#3a7a3a,fill:#aff090,rx:20px,ry:20px")
		mermaid.append("classDef event stroke:blue,fill:#96e0f6,rx:20px,ry:20px")
		mermaid.append("classDef literal stroke:black,fill:#f0f0e0;")
		mermaid.append("classDef classstyle stroke:black,fill:white;")
		self.walk(js, curr_int, id_map, mermaid)
		return "\n".join(mermaid)
	
	def write_html(self, j, *, title):
		m = self.build_mermaid(j)
		initialize = "mermaid.initialize({ theme: 'neutral', logLevel: 3, flowchart: { curve: 'linear' } });"
		s = f'''<!DOCTYPE html>
<html>
<head>
	<title>{title}</title>
<!-- 	<script src="/static/mermaid.min.js">​</script>​ -->
	<script src="https://linked.art/media/vendor/mermaid.min.js"></script>
</head>
<body>
	<div class="mermaid">{m}</div>
	<script>
		{initialize}
	</script>
</body>
</html>
		'''.strip()
		return s

def label_from_file(p):
	with open(p, 'r') as fh:
		j = json.load(fh)
		return j.get('_label', '')

app = Flask(__name__)

@app.route('/output/<string:model>/<path:file>')
def render_file(model, file):
	p = pathlib.Path('output') / model / file
	with open(p, 'r') as fh:
		j = json.load(fh)
		title = j.get('_label', 'Model')
		b = Builder()
		return b.write_html(j, title=title)

@app.route('/output/<string:model>')
def list_files(model):
	p = pathlib.Path('output') / model
	files = p.rglob('*.json')
	items = sorted([(label_from_file(s) or str(s), s) for s in files])
	items_html = ''.join([f'<li><a href="/{s}">{label}</a></li>\n' for label, s in items])
	return f'<ul>{items_html}</ul>'

@app.route('/')
def list_models():
	p = pathlib.Path('output')
	models = [s for s in p.glob('[0-9a-f]*')]
	names = {v: k for k, v in settings.arches_models.items()}
	items = sorted([(names.get(s.name, s), s) for s in models])
	items_html = ''.join([f'<li><a href="/{s}">{label}</a></li>\n' for label, s in items])
	return f'<ul>{items_html}</ul>'
