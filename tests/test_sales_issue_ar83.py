#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR83(TestSalesPipelineOutput):
    def test_modeling_ar83(self):
        '''
        AR-83: Fix textual work identifier types
        '''
        output = self.run_pipeline('ar83')
        lo = output['model-lo']
        catalog = lo['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,SC-A40']

        self.assertEqual(classified_identifier_sets(catalog), {
            None: {'Sale Catalog SC-A40'},
            'Owner-Assigned Number': {'SC-A40'},
            'STAR Identifier': {'13214', 'SCANDICATS-57'}
        })

        for ident in ('13214', 'SCANDICATS-57'):
            star_ids = [i for i in catalog['identified_by'] if i['content'] == ident]
            self.assertEqual(len(star_ids), 1)
            star_id = star_ids[0]
            self.verifyStarNumber(star_id)

    def verifyStarNumber(self, star_id):
        self.assertIn('assigned_by', star_id)
        assignments = star_id['assigned_by']
        self.assertEqual(len(assignments), 1)
        assignment = assignments[0]
        self.assertIn('carried_out_by', assignment)
        assignors = assignment['carried_out_by']
        self.assertEqual(len(assignors), 1)
        assignor = assignors[0]
        self.assertEqual(assignor['_label'], 'Getty Provenance Index')


if __name__ == '__main__':
    unittest.main()
