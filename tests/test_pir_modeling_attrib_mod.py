#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
import inspect
from pathlib import Path
import warnings

from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AttributionModifiers(TestProvenancePipelineOutput):
	def test_modeling_for_attribution_modifiers(self):
		'''
		There are 9 records, each with at least one attribution modifier:
		
			"manner of; style of"
			"formerly attributed to","attributed to"
			"attributed to"
			"copy after"
			"possibly by"
			"school of"
			"workshop of"
			"circle of"
			"follower of"
		
		'''
		output = self.run_pipeline('attrib_mod')

		objects = output['model-object']
		people = output['model-person']
		groups = output['model-groups']
		
		# there are 9 records, but 10 objects (paintings), because one of the objects
		# was "copy after" an original painting.
		self.assertEqual(len(objects), 10)
		
		# there are 16 people:
		# 	"Pierre André Joseph Knyff" (seller)
		# 	"Jeffrey, Henry" (seller)
		# 	"Schgosdass" (artist, formerly attributed to)
		# 	"HOLBEIN, HANS (THE YOUNGER)" (style of)
		# 	"SAVERY (XAVERY)" (artist, attributed to)
		# 	"DYCK, ANTHONIE VAN" (copy after)
		# 	"Simpson" (buyer)
		# 	"POUSSIN, NICOLAS" (circle of)
		# 	"Col. Nugent" (buyer)
		# 	"WEST, BENJAMIN" (studio of)
		# 	"Gosdaert [?]" (artist, changed from Schgosdass)
		# 	"RUBENS, PETER PAUL" (school of, follower of)
		# 	"Giot" (buyer)
		# 	"H Sudn [?]" (seller)
		# 	"CORNEILLE, JEAN BAPTISTE" (artist, possibly by)
		#   "Dr. S. (Berlin W 15, Kaiserallee 208)" (seller of an unsold transaction ("Unverkauft"))
		self.assertEqual(len(people), 16)

		# there are 4 groups:
		# 	'FollowerGroup of artist “RUBENS, PETER PAUL”' (influencer of the formation of the "follower of" group)
		# 	'School of artist “RUBENS, PETER PAUL”' (influencer of the formation of the "school of" group)
		# 	'Workshop of artist “WEST, BENJAMIN”' (influencer of the formation of the "workshop of" group)
		# 	'Circle of artist “POUSSIN, NICOLAS”' (influencer of the formation of the "circle of" group)
		self.assertEqual(len(groups), 4)
		
		# 'style of' modifiers use an AttributeAssignment that classifies the 'influenced_by' property as being 'Style of'
		style_of_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,Br-A2493,0029%5Bb%5D,1800-03-01']
		production = style_of_obj['produced_by']
		attr_assignment = production['attributed_by'][0]
		self.assertEqual(attr_assignment['assigned_property'], 'influenced_by')
		self.assertEqual(attr_assignment['property_classified_as']['_label'], 'Style Of')
		self.assertEqual(attr_assignment['assigned']['id'], 'tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#PERSON,ULAN,500005259')

		# 'possibly by' modifiers use an AttributeAssignment that is classified as 'possibly' to assert the 'carried_out_by' property
		possibly_by_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-A13,0086,1738-07-21']
		production = possibly_by_obj['produced_by']
		attr_assignment = production['attributed_by'][0]
		self.assertEqual(attr_assignment['assigned_property'], 'carried_out_by')
		self.assertIn('Possibly', {c['_label'] for c in attr_assignment['classified_as']})
		
		# 'formerly attributed to' modifiers use an AttributeAssignment that is classified as 'obsolete' to assert the 'carried_out_by' property
		formerly_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-A160,0221,1776-04-30']
		production = formerly_obj['produced_by']
		attr_assignment = production['attributed_by'][0]
		self.assertEqual(attr_assignment['assigned_property'], 'carried_out_by')
		self.assertIn('Obsolete', {c['_label'] for c in attr_assignment['classified_as']})
		self.assertEqual(attr_assignment['assigned']['_label'], 'Schgosdass')
		people = {person['_label'] for part in production['part'] for person in part['carried_out_by']}
		self.assertEqual(people, {'Gosdaert [?]'})
		
		# 'copy after' modifiers assert that the object's production was 'influenced_by' another object by the named influencer artist
		copy_after_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-A136,0089,1773-07-20']
		production = copy_after_obj['produced_by']
		self.assertEqual(1, len(production['influenced_by']))
		influence = production['influenced_by'][0]
		orig_production = influence['produced_by']
		people = {person['_label'] for part in orig_production['part'] for person in part['carried_out_by']}
		self.assertEqual(people, {'DYCK, ANTHONIE VAN'})

		# 'attributed to' modifiers are no-ops; they result in a normal carried_out_by property
		attributed_to_obj = objects['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#OBJECT,B-A138,0022,1774-05-30']
		production = attributed_to_obj['produced_by']
		people = {person['_label'] for part in production['part'] for person in part['carried_out_by']}
		self.assertEqual(people, {'SAVERY (XAVERY)'})


if __name__ == '__main__':
	unittest.main()
