#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestPeoplePipelineOutput

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR129(TestPeoplePipelineOutput):
    def test_modeling_ar129(self):
        '''
        AR-178: Data going into statement nodes being accidentally split on internal semi colons
        '''
        output = self.run_pipeline('ar178')
        
        grp = output['model-groups']
		
        stmnts = grp['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:shared#PERSON,AUTH,Washington%2C%20DC%2C%20USA.%20%20National%20Gallery%20of%20Art']['referred_to_by']
        contents = [stmt.get('content') for stmt in stmnts]
        self.assertIn("Andrew W. Mellon formally offered a bequest for the founding of the national collection in January 1937; on 27 March 1937 an Act of Congress accepted the collection and funds, and approved the construction of a museum on the National Mall.", contents)


if __name__ == '__main__':
    unittest.main()
