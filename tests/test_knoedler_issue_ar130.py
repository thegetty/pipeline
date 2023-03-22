#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestKnoedlerPipelineOutput, classified_identifier_sets

from collections import defaultdict

import re

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR130(TestKnoedlerPipelineOutput):
    def test_modeling_ar130(self):
        '''
        AR-130: Correct the provenance activity transaction chain
        '''

        output = self.run_pipeline('ar130')
        activities = output['model-activity']     

        # Group values by knoedler_number
        dictionaryOfLists = defaultdict(list)
        for value in activities.values():
            knoelderStockNumber = re.search("[0-9]+", value["_label"]).group()
            dictionaryOfLists[knoelderStockNumber].append(value["_label"].split(" ")[1])

        # 13373 Unsold,Sold,Sold,Sold,Unsold,Unsold
        # Purchase, Inventorying, Sale, Purchase, Sale, Purchase, Sale, Purchase, Inventorying

        # 1330 Sold, Unsold, Sold
        # Purchase, Sale, Purchase, Inventorying, Sale

        # 1327 Unsold, Sold
        # Purchase, Inventorying, Sale

        # 1307 Unsold, Unsold, Sold
        # Purchase, Inventorying, Inventorying, Sale

        # 1223 Unsold,Sold,Sold
        # Purchase, Inventorying, Sale, Purchase, Sale

        # 1134 Unsold, Sold, Unsold
        # Purchase, Inventorying, Sale, Purchase

        # 1399 Unsold, Unsold, Unsold, Unsold, Unsold, Unsold, Sold
        # Purchase, Inventorying, Inventorying, Inventorying, Inventorying, Inventorying, Inventorying, Sale   

        validationDict = {"13373": ["Purchase", "Inventorying", "Sale", "Purchase", "Sale", "Purchase", "Sale", "Purchase", "Inventorying"],

        "1330" : ["Purchase", "Sale", "Purchase", "Inventorying", "Sale"],

        "1327" : ["Purchase", "Inventorying", "Sale"],

        "1307" : ["Purchase", "Inventorying", "Inventorying", "Sale"],

        "1223" : ["Purchase", "Inventorying", "Sale", "Purchase", "Sale"],

        "1134" : ["Purchase", "Inventorying", "Sale", "Purchase"],

        "1399" : ["Purchase", "Inventorying", "Inventorying", "Inventorying", "Inventorying", "Inventorying", "Inventorying", "Sale"]
        }

        for key in dictionaryOfLists.keys():
            self.assertEqual(dictionaryOfLists[key], validationDict[key])


if __name__ == '__main__':
    unittest.main()
