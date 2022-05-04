#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput
vocab.add_attribute_assignment_check()

class PIRModelingTest_AR133(TestSalesPipelineOutput):
    def test_modeling_ar133(self):
        '''
        AR-133: Two New Fields to be Mapped on to Textual Work from Sales Records
        '''
        title_page_text = {'pg_sell_1_test', 'pg_sell_2_test'}
        seller_description = {'auc_copy_seller_1_test', 'auc_copy_seller_2_test','auc_copy_seller_3_test'}
        output = self.run_pipeline('ar133')
        text_works = output['model-lo']
        text_work = text_works['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#CATALOG,B-A139']
        self.assertTrue(ling_object_check(text_work,'title page text',title_page_text))
        activities = output['model-sale-activity']
        activity = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,B-A139']
        self.assertTrue(ling_object_check(activity,'seller description',seller_description))



def ling_object_check(object,type,values):
    expected_len = len(values)
    got_len = 0
    for item in object['referred_to_by']:
        if ('classified_as' in item) and item['classified_as'][0]['_label']==type:
            if item['content'] in values:
                got_len+=1
    if expected_len==got_len:
        return True
    else:
        return False

if __name__ == '__main__':
    unittest.main()
