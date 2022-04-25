#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classification_sets, classification_tree, classified_identifier_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR91(TestKnoedlerPipelineOutput):
    def test_modeling_ar91(self):
        '''
        AR-91: Model buyer/seller agents
        '''
        output = self.run_pipeline('ar91')
        activities = output['model-activity']
#         import json
#         print(json.dumps(activities, indent=4))
        
        # seller 'for': Clark, Jonas G.
        # selelr 'through': Goupil et Cie.
        # buyer: M. Knoedler & Co.
        tx1 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,1,192,6']
        self.verifyTransaction(tx1, sellers={'Clark, Jonas G.'}, seller_agents={'Goupil et Cie.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})
        
        tx2 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,6,223,30']
        self.verifyTransaction(tx2, sellers={'Gill, James Dwyer'}, seller_agents={'KILTON, W.S.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})
        
        tx3 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,11,195,3']
        self.verifyTransaction(tx3, sellers={'HOWARD, JEAN'}, seller_agents={'DABISH, GRACE'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})

        tx4 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In,3,203,11']
        self.verifyTransaction(tx4, sellers={'Tufts, Edwin'}, seller_agents={'MATTHEWS, N.'}, buyer_agents=set(), buyers={'M. Knoedler & Co.'})

        tx5 = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,11,195,3']
        self.verifyTransaction(tx5, sellers={'M. Knoedler & Co.'}, seller_agents=set(), buyer_agents={'DABISH, GRACE'}, buyers={'HOWARD, JEAN'})
        self.verifyReturnAcquisition(tx5)

    def verifyReturnAcquisition(self, tx):
        # There was a bug that was causing "Returned" transactions to go through the ETL modeling process twice, resulting in multiple Acquisition identifiers
        # This sanity-checks that the return transaction looks right.
        acqs = [p for p in tx['part'] if p.get('type') == 'Acquisition']
        self.assertEqual(len(acqs), 1)
        acq = acqs[0]
        self.assertEqual(
        	classified_identifier_sets(acq),
        	{
        		None: {'Knoedler return of Stock Number A8960 (1966-02-01)'}
        	}
        )
        pass

    def verifyTransaction(self, tx, sellers, seller_agents, buyer_agents, buyers):
        payments = [p for p in tx['part'] if p.get('type') == 'Payment']
        if payments:
            payment = payments[0]
            payee = {p.get('_label') for p in payment['paid_to']}
            payer = {p.get('_label') for p in payment['paid_from']}
            self.assertEqual(payee, sellers)
            self.assertEqual(payer, buyers)

        acqs = [p for p in tx['part'] if p.get('type') == 'Acquisition']
        self.assertEqual(len(acqs), 1)
        acq = acqs[0]
        reciever = {p.get('_label') for p in acq['transferred_title_from']}
        sender = {p.get('_label') for p in acq['transferred_title_to']}
        self.assertEqual(sender, buyers)
        self.assertEqual(reciever, sellers)

        agent_parts = [p for p in acq.get('part', [])]
        agents = {p.get('_label') for part in agent_parts for p in part['carried_out_by']}
        self.assertEqual(agents, seller_agents|buyer_agents)


if __name__ == '__main__':
    unittest.main()
