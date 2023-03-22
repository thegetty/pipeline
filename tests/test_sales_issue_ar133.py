#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifier_sets
vocab.add_attribute_assignment_check()

class PIRModelingTest_AR133(TestSalesPipelineOutput):
    def test_modeling_ar133(self):
        '''
        AR-133: Two New Fields to be Mapped on to Textual Work from Sales Records
        '''
        output = self.run_pipeline('ar133')
        text_works = output['model-lo']
        activities = output['model-sale-activity']

        text_work1 = text_works['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A139']
        title_page_text = ['Amateur des Arts']
        self.verifyReferences(text_work1, title_page_text, 'title page text'),
        
        activity1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,Br-1544']
        seller = ['Hawkins; Spackman; Jessop; etc.']
        self.verifyReferences(activity1, seller, 'seller description'),

        # Br-991 (Record includes both auc_copy and other_seller)
        activity2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,Br-991']
        sellers =['[Alexis Delahante]' , 'Woodburn; Henry Philip Hope']
        self.verifyReferences(activity2, sellers, 'seller description'),

    def verifyReferences(self, obj, results, key):
        for result in results:
            self.assertIn(result, classified_identifier_sets(obj, 'referred_to_by')[key])

if __name__ == '__main__':
    unittest.main()
