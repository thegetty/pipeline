#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR54(TestPeoplePipelineOutput):
    '''
    AR-54: Update people to have Star identifiers assigned by GPI
    '''
    def test_modeling_ar54(self):
        output = self.run_pipeline('ar54')

        people = output['model-person']
        person = people['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Parsons%2C%20Charles%20H.']
        self.assertEqual(classified_identifiers(person), {
        	'STAR Identifier': '46035',
        	'Personal Name': 'Parsons, Chs.',
        	'Primary Name': 'Parsons, Charles H.',
        })
        
        star_id = [i for i in person['identified_by'] if i['content'] == '46035'][0]
        self.assertIn('assigned_by', star_id)
        assignments = star_id['assigned_by']
        self.assertEqual(len(assignments), 1)
        assignors = assignments[0]['carried_out_by']
        self.assertEqual(len(assignors), 1)
        gpi = assignors[0]
        self.assertEqual(gpi['_label'], 'Getty Provenance Index')

if __name__ == '__main__':
    unittest.main()