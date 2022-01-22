#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR42(TestSalesPipelineOutput):
    '''
    AR-42: Use PLOC Data to Populate Current Location Field
    '''
    def test_modeling_ar42(self):
        output = self.run_pipeline('ar42')

        objects = output['model-object']
        groups = output['model-groups']
        
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-4575,0111,1836-03-26']

        self.assertIn('current_location', obj)
        self.assertEqual(obj['current_location']['_label'], 'Detroit, MI, USA')

        self.assertIn('current_owner', obj)
        self.assertEqual(len(obj['current_owner']), 1)
        self.assertEqual(obj['current_owner'][0]['_label'], 'Detroit Institute of Arts (Detroit, MI, USA)')
        
        # ensure that the ploc note is attached to the object, and that it was assigned by the ploc institution
        ploc_notes = [i for i in obj.get('referred_to_by', []) if i.get('content') == 'as copy after Leonardo da Vinci']
        self.assertEqual(len(ploc_notes), 1)
        ploc_note = ploc_notes[0]
        self.assertEqual(ploc_note['assigned_by'][0]['carried_out_by'][0]['_label'], 'Detroit Institute of Arts (Detroit, MI, USA)')

		# Ensure that the current owner of an object appearing in a Lottery event is modeled
		# (This takes a different code path, and wasn't being handled previously, so verify it's fixed.)
        lottery_obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-A1,0012,1670-04-21']
        self.assertIn('current_owner', lottery_obj)
        self.assertEqual(len(lottery_obj['current_owner']), 1)
        self.assertEqual(lottery_obj['current_owner'][0]['_label'], 'Gemäldegalerie (Dresden, Germany)')
        inst = lottery_obj['current_owner'][0]['id']
        self.assertIn(inst, groups)

if __name__ == '__main__':
    unittest.main()
