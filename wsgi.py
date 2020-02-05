#!/usr/bin/env python3

import os
import sys
import json
import uuid
import pathlib
import itertools
import settings
import pprint
from unidecode import unidecode
from contextlib import suppress
from pipeline.util import truncate_with_ellipsis
from flask import Flask, escape, request

class Builder:
	def __init__(self, path):
		self.counter = itertools.count()
		self.path = path
		self.seen = set()
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

	def normalize_string(self, s, length=40):
		s = unidecode(s)
		s = s.replace('"', "'")
		s = truncate_with_ellipsis(s, length) or s
		return s
		
	def uri_to_label_link(self, uri, type):
		link = None
		if uri.startswith('urn:uuid:'):
			label = f'#{next(self.counter)}'
			uu = uri[9:]
			with suppress(StopIteration):
				file = next(self.path.rglob(f'{uu}.json'))
				if file:
					label = self.normalize_string(label_from_file(file))
					link = f'/{file.relative_to(self.path.parent)}'
			return label, link
		elif uri.startswith('http://vocab.getty.edu/'):
			uri = uri.replace('http://vocab.getty.edu/', '')
			uri = uri.replace('/', ': ')
			return uri, link
		elif uri.startswith('https://linked.art/example/'):
			uri = uri.replace('https://linked.art/example/', '')
			uri = uri.replace('/', '')
			return uri, link
		else:
			print("Unhandled URI: %s" % uri)
			return uri, link

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
			lbl, link = self.uri_to_label_link(curr, js.get('type'))
			line = "%s(\"%s\")" % (currid, lbl)
			if not line in mermaid:
				mermaid.append(line)
			if link:
				line = f'click {currid} "{link}" "Link"'
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
						v = "\"''%s''\""% self.normalize_string(v, 100)
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
<!-- 	<script src="/static/mermaid.min.js">y</script>y -->
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
output_path = pathlib.Path(settings.output_file_path)

@app.route(f'/{output_path.name}/<string:model>/<path:file>')
def render_file(model, file):
	p = output_path / model / file
	with open(p, 'r') as fh:
		j = json.load(fh)
		title = j.get('_label', 'Model')
		b = Builder(output_path)
		return b.write_html(j, title=title)

@app.route(f'/{output_path.name}/<string:model>')
def list_files(model):
	p = output_path / model
	files = p.rglob('*.json')
	items = sorted([(label_from_file(s) or str(s), s.relative_to(output_path.parent)) for s in files])
	items_html = ''.join([f'<li><a href="/{s}">{truncate_with_ellipsis(label, 100) or label}</a></li>\n' for label, s in items])
	return f'<ul>{items_html}</ul>'

@app.route('/')
def list_models():
	p = output_path
	models = [s.relative_to(output_path.parent) for s in p.glob('*') if s.is_dir()]
	names = {v: k for k, v in settings.arches_models.items()}
	items = sorted([(names.get(s.name, s), s) for s in models])
	items_html = ''.join([f'<li><a href="/{s}">{label}</a></li>\n' for label, s in items])
	return f'<ul>{items_html}</ul>'
