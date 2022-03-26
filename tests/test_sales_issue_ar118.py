#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR118(TestSalesPipelineOutput):
    def test_modeling_ar118(self):
        '''
        AR-118: Apply Modelling used buyer modifiers
        '''
        output = self.run_pipeline('ar118')
        groups = output['model-groups']
        activities = output['model-activity']
        people = output['model-person']

        sale1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-276,1817-07-08,0002']
        self.verifyBuyerGroup(groups, people, sale1, {'Mad', 'Wad'})

        sale2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-340,1820-07-19,0291']
        self.verifyBuyerGroup(groups, people, sale2, {'Meert (Bruxelles)', 'Passeniers, Benoît Joseph'})

    def verifyBuyerGroup(self, groups, people, sale, expected):
        parts = [p for p in sale.get('part', []) if p.get('type') == 'TransferOfCustody' if 'transferred_custody_to' in p]
        part = parts[0]
        buyers = part['transferred_custody_to']
        buyer = buyers[0]
        self.assertEqual(buyer['type'], 'Group')
        self.assertIn(buyer['id'], groups)
        group = groups[buyer['id']]

        seen = set()
        for p in people.values():
            for g in p.get('member_of', []):
                if g['id'] == buyer['id']:
                    seen.add(p['_label'])
        self.assertEqual(seen, expected)


if __name__ == '__main__':
    unittest.main()
