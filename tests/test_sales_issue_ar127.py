#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classification_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR127(TestSalesPipelineOutput):
    def test_modeling_ar127(self):
        '''
        AR-127: Prices on Set Model to Appear on Provenance Activity Model
        '''
        output = self.run_pipeline('ar127')
        activities = output['model-activity']
        sets = output['model-set']

        prov1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-4874,1838-04-24,0004']
        set1 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-4874,0004,1838-04-24-Set']
        self.assertMatchingValuations(prov1, set1, 400)

        set2 = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,D-411,0017,1931-04-27-Set']
        prov2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,D-411,1931-04-27,0017']
        self.assertMatchingValuations(prov2, set2, 150)

    def assertMatchingValuations(self, prov, objset, value):
        dims = [d for d in objset.get('dimension', []) if d.get('type') == 'MonetaryAmount']
        self.assertEqual(len(dims), 1)
        set_dim = dims[0]
        set_cl = classification_sets(set_dim)

        import pprint, json
        aas = [a for a in prov.get('part') if a.get('type') == 'AttributeAssignment' and classification_sets(a) == {'Appraising'}]
        self.assertEqual(len(aas), 1)
        aa = aas[0]
        assignments = aa.get('assigned', [])
        self.assertEqual(len(assignments), 1)
        assignment = assignments[0]
        prov_cl = classification_sets(assignment)
        self.assertEqual(set_cl, prov_cl)
        self.assertEqual(assignment['value'], value)
        self.assertEqual(assignment['value'], set_dim['value'])


if __name__ == '__main__':
    unittest.main()
