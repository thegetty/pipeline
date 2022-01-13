#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR109(TestSalesPipelineOutput):
    def test_modeling_ar109(self):
        '''
        AR-109: Add creator information and timespan to set record creation branch
        '''
        output = self.run_pipeline('ar109')
        sets = output['model-set']
        
        # 1 auction house
        set2 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,D-B2591,0001,1914-06-23-Set']
        self.assertIn('created_by', set2)
        creation2 = set2['created_by']
        parts2 = creation2['part']
        self.assertEqual(len(parts2), 1)
        part2 = parts2[0]
        self.assertIn('carried_out_by', part2)
        actors2 = part2['carried_out_by']
        self.assertEqual(len(actors2), 1)
        self.assertIn('timespan', creation2)
        ts2 = creation2['timespan']
        self.assertEqual(ts2['_label'], '1914-06-23 to 1914-06-24')

        # 1 expert, 1 commissaire
        set1 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,F-2,0236%5BM%5D,1801-03-27-Set']
        self.assertIn('created_by', set1)
        creation1 = set1['created_by']
        parts1 = creation1['part']
        self.assertEqual(len(parts1), 2)
        parts1_comms = [p for p in parts1 if 'Commissaire-priseur' in p['_label']]
        parts1_exps = [p for p in parts1 if 'Expert' in p['_label']]
        self.assertEqual(len(parts1_comms), 1)
        self.assertEqual(len(parts1_exps), 1)
        parts1_comm = parts1_comms[0]
        self.assertIn('carried_out_by', parts1_comm)
        actors1_comm = [a for a in parts1_comm['carried_out_by'] if 'Boileau, Louis-François-Jacques' in a['_label']]
        self.assertEqual(len(actors1_comm), 1)

        parts1_exp = parts1_exps[0]
        self.assertIn('carried_out_by', parts1_exp)
        actors1_exp = [a for a in parts1_exp['carried_out_by'] if 'Constantin, Guillaume-Jean' in a['_label']]
        self.assertEqual(len(actors1_exp), 1)

        self.assertIn('timespan', creation1)
        ts1 = creation1['timespan']
        self.assertEqual(ts1['_label'], '1801-03-23 onwards')


if __name__ == '__main__':
    unittest.main()
