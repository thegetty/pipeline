#!/usr/bin/env python3 -B
import unittest
from contextlib import suppress

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR76(TestKnoedlerPipelineOutput):
    def test_modeling_ar76(self):
        '''
        AR-76: Include Knoedler in Purchase and Sale events
        '''
        output = self.run_pipeline('ar76')
        activities = output['model-activity']

        txs = ['8,224,36', '3,118,4', '5,110,20', '9,172,29']

        handled = 0
        for tx_suffix in txs:
            with suppress(KeyError):
                tx1_out = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,' + tx_suffix]
                self.verifyKnoedlerPresence(tx1_out, 'out')
                handled += 1

            with suppress(KeyError):
                tx1_in = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,' + tx_suffix]
                self.verifyKnoedlerPresence(tx1_in, 'in')
                handled += 1
        self.assertEqual(handled, 7)

    def verifyKnoedlerPresence(self, tx, direction):
        parts = tx['part']
        encounters = [p for p in parts if p['type'] == 'Encounter']
        for encounter in encounters:
            # if this is an Inventorying event
            self.assertEqual(encounter['carried_out_by'][0]['_label'], 'M. Knoedler & Co.')
        
        test_types = [
            ('Payment', ('paid_to', 'paid_from')),
            ('Acquisition', ('transferred_title_from', 'transferred_title_to'))
        ]
        
        for activity_type, activity_props in test_types:
            activity_parts = [p for p in parts if p['type'] == activity_type]
            if len(activity_parts):
                # otherwise, assert that Knoedler was either the payer/payee in each Payment (or equivalent for other types of activities)
                # print(f'{len(activity_parts)} {activity_type} parts for this transation')
                pay_dir = activity_props[0] if direction == 'out' else activity_props[1]
                payment_participant = [p for p in parts if p['type'] == activity_type and p[pay_dir][0]['_label'] == 'M. Knoedler & Co.']
                self.assertEqual(len(payment_participant), 1)
            else:
                # no payment data for this transaction
                # print(f'No {activity_type} data for this transaction')
                pass


if __name__ == '__main__':
    unittest.main()
