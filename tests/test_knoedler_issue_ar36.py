#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput
from cromulent import vocab

class PIRModelingTest_AR36(TestKnoedlerPipelineOutput):
    def test_modeling_ar36(self):
        '''
        AR-36: Relate each Knoedler row records to the things it is about
        '''
        output = self.run_pipeline('ar36')
        lo = output['model-lo']
        objects = output['model-object']
        activities = output['model-activity']
        people = output['model-person']
        los = output['model-lo']
        
        row_record = los['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,29,Row,14']
        
        row_name = 'Knoedler Stock Book 4, Page 29, Row 14'
        self.assertEqual(row_record['_label'], row_name)
        
        # test the link from the physical object to the row record
        painting = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Object,4655a']
        painting_refs = {r['_label'] for r in painting['referred_to_by']}
        self.assertIn(row_name, painting_refs)

        # test the link from the activities to the row record
        for data in activities.values():
            refs = {r['_label'] for r in data['referred_to_by']}
            self.assertIn(row_name, refs)

        # test the link from the people to the row record
        for person in people.values():
            self.assertIn('referred_to_by', person)
            refs = {r['_label'] for r in person['referred_to_by']}
            self.assertIn(row_name, refs)


if __name__ == '__main__':
    unittest.main()
