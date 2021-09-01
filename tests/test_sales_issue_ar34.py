#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR34(TestSalesPipelineOutput):
    def test_modeling_ar34(self):
        '''
        AR-34: Fix Date Formatting when Month or Day are '00'
        '''
        output = self.run_pipeline('ar34')
        events = output['model-sale-activity']
        event = events['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,Br-2646']
        
        self.verify_zero_month_record(events)
        self.verify_zero_day_record(events)
        self.verify_zero_day_month_record(events)

    def verify_zero_day_month_record(self, events):
        event = events['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,B-202']
        
        ts = event['timespan']
        self.assertEqual(ts['_label'], '1812')
        self.assertEqual(ts['begin_of_the_begin'], '1812-01-01T00:00:00Z')
        self.assertEqual(ts['end_of_the_end'], '1813-01-01T00:00:00Z')
        self.assertEqual(classified_identifiers(ts), {
            None: '1812-00-00'
        })

    def verify_zero_day_record(self, events):
        event = events['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,B-197-A']
        
        ts = event['timespan']
        self.assertEqual(ts['_label'], '1811-09')
        self.assertEqual(ts['begin_of_the_begin'], '1811-09-01T00:00:00Z')
        self.assertEqual(ts['end_of_the_end'], '1811-10-01T00:00:00Z')
        self.assertEqual(classified_identifiers(ts), {
            None: '1811-09-00'
        })

    def verify_zero_month_record(self, events):
        event = events['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,Br-2646']
        
        ts = event['timespan']
        self.assertEqual(ts['_label'], '1825-01-28 to 1825-12-28')
        self.assertEqual(ts['begin_of_the_begin'], '1825-01-28T00:00:00Z')
        self.assertEqual(ts['end_of_the_end'], '1825-12-29T00:00:00Z')
        self.assertEqual(classified_identifiers(ts), {
            None: '1825-00-28'
        })


if __name__ == '__main__':
    unittest.main()
