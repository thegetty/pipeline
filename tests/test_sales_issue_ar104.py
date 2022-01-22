#!/usr/bin/env python3 -B
import unittest

from tests import TestSalesPipelineOutput
from cromulent import vocab

class PIRModelingTest_AR104(TestSalesPipelineOutput):
    def test_modeling_ar104(self):
        '''
        AR-104: Relate each Sales row records to the things it is about
        '''
        output = self.run_pipeline('ar104')
        lo = output['model-lo']
        objects = output['model-object']
        activities = output['model-activity']
        people = output['model-person']
        los = output['model-lo']
        vi = output['model-visual-item']
        
        row_name_prefix = 'Sale recorded in catalog'
        
        # test the link from the physical object to the row record
        painting = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-A99,0027,1777-02-26']
        painting_refs = {r.get('_label') for r in painting['referred_to_by']}
        self.assertTrue(any([r and r.startswith(row_name_prefix) for r in painting_refs]))

        # test the link from the activities to the row record
        for data in activities.values():
            refs = {r.get('_label') for r in data['referred_to_by']}
            self.assertTrue(any([r and r.startswith(row_name_prefix) for r in refs]))

        # test the link from the people to the row record
        for person in people.values():
            self.assertIn('referred_to_by', person)
            refs = {r.get('_label') for r in person['referred_to_by']}
            self.assertTrue(any([r and r.startswith(row_name_prefix) for r in refs]))

        # test the link from the visual items to the row record
        for person in vi.values():
            self.assertIn('referred_to_by', person)
            refs = {r.get('_label') for r in person['referred_to_by']}
            self.assertTrue(any([r and r.startswith(row_name_prefix) for r in refs]))


if __name__ == '__main__':
    unittest.main()
