#!/usr/bin/env python3 -B
import unittest
import os
import os.path
import hashlib
import json
import uuid
import pprint
import inspect
from itertools import groupby
from pathlib import Path
import warnings

from tests import TestSalesPipelineOutput, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class DecimalizationTest(TestSalesPipelineOutput):
    def test_decimalization_conversion(self):
        '''
        Test for decimalization of values 
        '''
        output = self.run_pipeline('ar176')
        self.verify_sales(output)
    

    def verify_sales(self, output):
        activities = output['model-activity']

        prov_entry_curr = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-A411,1747-04-07,0150']

        assignments1 = [p for p in prov_entry_curr.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments1), 1)
        assignment1 = assignments1[0]
        self.assertEqual(assignment1['_label'], 'Bidding valuation of Br-A411 0150 1747-04-07')
        self.assertEqual(classification_sets(assignment1), {'Bidding'})
        prices = assignment1['assigned']
        self.assertEqual(len(prices), 1)
        amount1 = prices[0]
        self.assertEqual(amount1['_label'], '6.83 pounds') # Initital price 6-16-6 with  decimal_conversion: 1 pound = 20 shillings = 240 pence
        self.assertEqual(amount1['value'], 6.825)
        self.assertEqual(len(prices), 1)
        self.assertEqual(amount1['currency']['_label'], 'British Pounds')
        
        prov_entry_curr = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,B-252,1816-04-23,0013a%5BH%5D']

        assignments1 = [p for p in prov_entry_curr.get('part', []) if p['type'] == 'AttributeAssignment']
        self.assertEqual(len(assignments1), 1)
        assignment1 = assignments1[0]
        self.assertEqual(assignment1['_label'], 'Bidding valuation of B-252 0013a[H] 1816-04-23')
        self.assertEqual(classification_sets(assignment1), {'Bidding'})
        prices = assignment1['assigned']
        self.assertEqual(len(prices), 1)
        amount1 = prices[0]
        self.assertEqual(amount1['_label'], '2.01 francs') # Initital price 2-1 with decimal_conversion: 1 francs = 100 centimes 
        self.assertEqual(amount1['value'], 2.01)
        self.assertEqual(len(prices), 1)
        self.assertEqual(amount1['currency']['_label'], 'French Francs')


if __name__ == '__main__':
    unittest.main()
