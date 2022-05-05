#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput,classified_identifier_sets
vocab.add_attribute_assignment_check()

class PIRModelingTest_AR133(TestSalesPipelineOutput):
    def test_modeling_ar133(self):
        '''
        AR-133: Two New Fields to be Mapped on to Textual Work from Sales Records
        '''
        title_page_text = {'Amateur des Arts'}
        seller_description = {'Hawkins; Spackman; Jessop; etc.'}
        output = self.run_pipeline('ar133')
        text_works = output['model-lo']
        text_work = text_works['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A139']
        self.assertTrue(ling_object_check(text_work,'title page text',title_page_text))
        activities = output['model-sale-activity']
        activity = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,Br-1544']
        self.assertTrue(ling_object_check(activity,'seller description',seller_description))

def ling_object_check(object,type,values):
    return classified_identifier_sets(object, 'referred_to_by')[type] in values

if __name__ == '__main__':
    unittest.main()
