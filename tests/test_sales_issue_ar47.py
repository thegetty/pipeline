#!/usr/bin/env python3 -B
import unittest

from cromulent import vocab

from tests import TestSalesPipelineOutput, classified_identifiers

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR47(TestSalesPipelineOutput):
    def test_modeling_ar47(self):
        '''
        AR-47: Assert the auction as the purpose of the assigning of lot numbers.
        '''
        output = self.run_pipeline('ar47')
        sets = output['model-set']
        objects = output['model-object']
        activities = output['model-activity']
        obj = objects['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1895,0002,1792-03-31']
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#PROV,Br-A1895,1792-03-31,0002']

        # Verify that the object is identified with the lot number, and the lot number was assigned with the purpose of the auction
        self.verifyAuctionAssignment(obj, 'Br-A1895')

        # Finally, verify that the set of objects comprising the lot is identified with the lot number, and the lot number was assigned with the purpose of the auction
        objset = sets['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION,Br-A1895,0002,1792-03-31-Set']
        self.verifyAuctionAssignment(objset, 'Br-A1895')

    def verifyAuctionAssignment(self, obj, cno):
        lot_numbers = [i for i in obj['identified_by'] if i['content'] == '0002']
        self.assertEqual(len(lot_numbers), 1)
        lno = lot_numbers[0]
        self.assertIn('assigned_by', lno)
        assignments = lno['assigned_by']
        self.assertEqual(len(assignments), 1)
        assignment = assignments[0]
        self.assertIn('specific_purpose', assignment)
        purposes = assignment['specific_purpose']
        self.assertEqual(len(purposes), 1)
        purpose = purposes[0]
        self.assertEqual(purpose['id'], f'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#AUCTION-EVENT,{cno}')


if __name__ == '__main__':
    unittest.main()