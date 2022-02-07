#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR82(TestSalesPipelineOutput):
    def test_modeling_ar82(self):
        '''
        AR-82: Change naming for activity model records
        '''
        output = self.run_pipeline('ar82')
        sales = output['model-sale-activity']
        
        self.verifySaleEventLabel(sales, 'Br-A1201', 'Auction Event Br-A1201 (1779-04-23 to 1779-04-24)')
        self.verifySaleEventLabel(sales, 'F-40', 'Auction Event F-40 (1803)')
        self.verifySaleEventLabel(sales, 'D-A12', 'Auction Event D-A12 (1739)')
        
        self.verifySaleLabel(sales, 'Br-A1201,0065,1779-04-24', 'Auction of Lot Br-A1201 0065 (1779-04-24)')
        self.verifySaleLabel(sales, 'F-40,0017,1803', 'Auction of Lot F-40 0017 (1803)')
        self.verifySaleLabel(sales, 'D-A12,0234,1739', 'Auction of Lot D-A12 0234 (1739)')
        
    def verifySaleEventLabel(self, sales, ident, expected_label):
        sale = sales[f'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,{ident}']
        idents = classified_identifier_sets(sale)
        self.assertEqual(idents, {
            None: {expected_label}
        })

    def verifySaleLabel(self, sales, ident, expected_label):
        sale = sales[f'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,{ident}']
        idents = classified_identifier_sets(sale)
        self.assertDictContainsSubset({
            None: {expected_label}
        }, idents)

if __name__ == '__main__':
    unittest.main()
