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

from pipeline.projects.provenance.util import pir_uri
from tests import TestProvenancePipelineOutput
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AttributionModifiers(TestProvenancePipelineOutput):
	def test_modeling_for_attribution_modifiers(self):
		'''
		There are 4 records, each with at least one buyer or seller modifier:
		
			"for"
			"through"
			"and"
			"or"
		'''
		output = self.run_pipeline('buy_sell_mod')

		objects = output['model-object']
		people = output['model-person']
		activities = output['model-activity']
		
		# there are 4 records, but 5 objects (paintings), because one of the objects
		# was "copy after" an original painting.
		self.assertEqual(len(objects), 5)
		
		# there are 17 people:
		# 	"Frits Lugt" (static instance emitted by the pipeline)
		# 	"DUBBELS, HENDRIK JACOBSZ." (artist)
		# 	"RUBENS, PETER PAUL" (artist)
		# 	"PANINI, GIOVANNI PAOLO" (artist)
		# 	"CIMAROLI, GIOVANNI BATTISTA" (artist)
		# 	"Ewing" (buyer, through)
		# 	"Gucht, van der" (buyer, for)
		# 	"Raymond" (buyer, and)
		# 	"Pond, Arthur" (buyer, and)
		# 	"Heath" (buyer)
		# 	"Blackwood, John" (seller)
		# 	"Orford, Robert Walpole, 1st Earl of" (seller, or)
		# 	"Orford, Robert Walpole, 2nd Earl of" (seller, or)
		# 	"Grey, Charles" (seller, and)
		# 	"Gent, G.W." (seller, and)
		# 	"Mackey, Mrs." (seller, for)
		# 	"Nelthorpe" (seller, through)
		self.assertEqual(len(people), 17)

		# buyer 'for'/'through' is modeled an AGENT who carries out the payment and acquisition, and a BUYER who pays for the object and to whom the object's title is transferred
		b_ft_obj = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION-TX,Br-A450,1751-03-08,0044']
		acq = [p for p in b_ft_obj['part'] if p['type'] == 'Acquisition'][0]
		pay = [p for p in b_ft_obj['part'] if p['type'] == 'Payment'][0]
		self.assertIn('Ewing', {p['_label'] for p in acq['carried_out_by']})
		self.assertEqual(acq['transferred_title_to'][0]['_label'], 'Gucht, van der')
		self.assertEqual(pay['paid_from'][0]['_label'], 'Gucht, van der')
		self.assertIn('Ewing', {p['_label'] for p in acq['carried_out_by']})
		self.assertIn('Ewing', {p['_label'] for p in pay['carried_out_by']})

		# buyer 'and' is modeled as a procurement with multiple records for both transferred_title_to and paid_from
		b_and_obj = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION-TX,Br-A458,1751-06-13,0031']
		acq = [p for p in b_and_obj['part'] if p['type'] == 'Acquisition'][0]
		pay = [p for p in b_and_obj['part'] if p['type'] == 'Payment'][0]
		acq_people = {p['_label'] for p in acq['transferred_title_to']}
		pay_people = {p['_label'] for p in pay['paid_from']}
		self.assertEqual(acq_people, pay_people)
		self.assertEqual(acq_people, {'Raymond', 'Pond, Arthur'})

		# the verbatim text of 'or' modifiers are preserved in notes attached to the acquisition; in this case, the sellers are 'or'
		acq_notes = {n['content'] for n in acq['referred_to_by']}
		self.assertEqual(acq_notes, {'or Orford, Robert Walpole, 1st Earl of', 'or Orford, Robert Walpole, 2nd Earl of'})
		
		# seller 'and' is modeled as a procurement with multiple records for both transferred_title_from and paid_to
		s_and_obj = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION-TX,Br-1299,1815-06-10,0060']
		acq = [p for p in s_and_obj['part'] if p['type'] == 'Acquisition'][0]
		pay = [p for p in s_and_obj['part'] if p['type'] == 'Payment'][0]
		acq_people = {p['_label'] for p in acq['transferred_title_from']}
		pay_people = {p['_label'] for p in pay['paid_to']}
		self.assertEqual(acq_people, pay_people)
		self.assertEqual(acq_people, {'Grey, Charles', 'Gent, G.W.'})

		# seller 'for'/'through' is modeled an AGENT who carries out the payment and acquisition, and a SELLER who is paid for the object and from whom the object's title is transferred
		s_ft_obj = activities['tag:getty.edu,2019:digital:pipeline:provenance:REPLACE-WITH-UUID#AUCTION-TX,Br-1344,1815-12-02,0075']
		acq = [p for p in s_ft_obj['part'] if p['type'] == 'Acquisition'][0]
		pay = [p for p in s_ft_obj['part'] if p['type'] == 'Payment'][0]
		self.assertIn('Nelthorpe', {p['_label'] for p in acq['carried_out_by']})
		self.assertEqual(acq['transferred_title_from'][0]['_label'], 'Mackey, Mrs.')
		self.assertEqual(pay['paid_to'][0]['_label'], 'Mackey, Mrs.')
		self.assertIn('Nelthorpe', {p['_label'] for p in acq['carried_out_by']})
		self.assertIn('Nelthorpe', {p['_label'] for p in pay['carried_out_by']})


if __name__ == '__main__':
	unittest.main()
