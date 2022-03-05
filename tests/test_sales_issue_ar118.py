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
        import json, pprint
        print(json.dumps(output, indent=4))


if __name__ == '__main__':
    unittest.main()
