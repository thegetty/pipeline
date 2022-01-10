#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR83(TestKnoedlerPipelineOutput):
    '''
    AR-83: Knoedler record enrichment
    '''
    def test_modeling_ar83(self):
        output = self.run_pipeline('ar83')

        texts = output['model-lo']
        row = texts['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#Text,Book,4,Page,29,Row,14']

        self.assertEqual(classified_identifier_sets(row), {
            'Entry Number': {'14'},
            'STAR Identifier': {'69'},
            'Title': {'Knoedler Stock Book 4, Page 29, Row 14'}
        })

        star_ids = [i for i in row['identified_by'] if i['content'] == '69']
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
