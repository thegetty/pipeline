#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR87(TestKnoedlerPipelineOutput):
    def test_modeling_ar87(self):
        '''
        AR-87: Do not infer partial payment amounds from fractional share data.
        '''
        output = self.run_pipeline('ar87')
        lo = output['model-lo']
        activities = output['model-activity']
        objects = output['model-object']
        
        sale = activities['tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out,5,190,38']
        parts = sale['part']
        payments = [p for p in parts if p['type'] == 'Payment']
        
        # assert that there is a payment
        self.assertEqual(len(payments), 1)
        payment = payments[0]
        self.assertEqual(payment['_label'], 'Payment for Stock Number 12024 (1910-07-25)')
        paym_parts = payment['part']
        
        # and that the payment is to 3 different recipients
        self.assertEqual(len(payment['paid_to']), 3)
        # but that there is only one known part of the payment
        self.assertEqual(len(paym_parts), 1)
        # that is TO Knoedler
        self.assertEqual(paym_parts[0]['_label'], 'M. Knoedler & Co. share of payment for Stock Number 12024 (1910-07-25)')
        paym_part = paym_parts[0]
        # in the amount of 5,333.60 pounds
        self.assertEqual(paym_part['paid_amount']['_label'], '5,333.60 pounds')

    def test_modeling_ar87_2(self):
        '''
        AR-87: Seller is any of the joint owners.
        '''
        output = self.run_pipeline('ar87')
        activities = output['model-activity']
        
        purchases = [y for x, y in activities.items() if "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,In" in x or "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX-MULTI,In" in x]
        
        purchases_with_sellers_joint_owners = []
        purchases_with_sellers_not_joint_owners = []
        for purchase in purchases:
            acquisition =  [x for x in purchase["part"] if x["type"]== "Acquisition"][0]
            sellers = set([x["id"] for x in acquisition["transferred_title_from"]])
            joint_owners = set([x["id"] for x in acquisition["transferred_title_to"]])
            if sellers.intersection(joint_owners):
                purchases_with_sellers_joint_owners.append(purchase)
            else:
                purchases_with_sellers_not_joint_owners.append(purchase)
        
        self.assertEqual(len(purchases_with_sellers_joint_owners), 4)
        self.assertEqual(len(purchases_with_sellers_not_joint_owners), 2)

        for purchase_with_sellers_joint_owners in purchases_with_sellers_joint_owners:
            # there should be an attribute assignment branch
            self.assertEqual(len(purchase_with_sellers_joint_owners['part']), 4)

            # the attribute assignment branch should contain the total monetary amount information
            attribute_assignment = [x for x in purchase_with_sellers_joint_owners["part"] if x["type"]== "AttributeAssignment"][0]
            self.assertNotEqual(attribute_assignment["assigned"][0].get("value"), None) 

            # attribute assignment branch should be carried out by the joint owners
            acquisition =  [x for x in purchase_with_sellers_joint_owners["part"] if x["type"]== "Acquisition"][0]
            joint_owners = set([x["id"] for x in acquisition["transferred_title_to"]])
            attribute_assignment_actors = [x["id"] for x in attribute_assignment["carried_out_by"]]
            self.assertSetEqual(set(attribute_assignment_actors), set(joint_owners))

            # total payment branch should have only part property
            payment = [x for x in purchase_with_sellers_joint_owners["part"] if x["type"]== "Payment"][0]
            self.assertTrue(len(payment) <= 4)

            # the information about payment receiver and giver should be moved to partial payment
            part_payment = payment["part"][0]
            self.assertTrue({"paid_amount", "paid_from", "paid_to"}.issubset(part_payment.keys()))

        for purchase_with_sellers_not_joint_owners in purchases_with_sellers_not_joint_owners:
            # there should not be an attribute assignment branch
            self.assertEqual(len(purchase_with_sellers_not_joint_owners['part']), 3)

            # total payment branch should have all the properties
            payment = [x for x in purchase_with_sellers_not_joint_owners["part"] if x["type"]== "Payment"][0]
            self.assertTrue({"paid_amount", "paid_from", "paid_to"}.issubset(payment.keys()))

            # the partial payment should contain only the information about knoedler subpayment
            part_payment = payment["part"][0]
            self.assertTrue({"paid_amount", "paid_from"}.issubset(part_payment.keys()))

        '''
        AR-87: Buyer is any of the joint owners.
        '''

        sales = [y for x, y in activities.items() if "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX,Out" in x or "tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:knoedler#TX-MULTI,Out" in x]
        
        sales_with_buyers_joint_owners = []
        sales_with_buyers_not_joint_owners = []
        for sale in sales:
            acquisition =  [x for x in sale["part"] if x["type"]== "Acquisition"][0]
            buyers = set([x["id"] for x in acquisition["transferred_title_to"]])
            joint_owners = set([x["id"] for x in acquisition["transferred_title_from"]])
            if buyers.intersection(joint_owners):
                sales_with_buyers_joint_owners.append(sale)
            else:
                sales_with_buyers_not_joint_owners.append(sale)

        self.assertEqual(len(sales_with_buyers_joint_owners), 3)
        self.assertEqual(len(sales_with_buyers_not_joint_owners), 2)   

        for sale_with_buyers_joint_owners in sales_with_buyers_joint_owners:
            # there should be an attribute assignment branch
            self.assertEqual(len(sale_with_buyers_joint_owners['part']), 3)

            # the attribute assignment branch should contain the total monetary amount information
            attribute_assignment = [x for x in sale_with_buyers_joint_owners["part"] if x["type"]== "AttributeAssignment"][0]
            self.assertNotEqual(attribute_assignment["assigned"][0].get("value"), None) 

            # attribute assignment branch should be carried out by the joint owners
            acquisition =  [x for x in sale_with_buyers_joint_owners["part"] if x["type"]== "Acquisition"][0]
            joint_owners = set([x["id"] for x in acquisition["transferred_title_from"]])
            attribute_assignment_actors = [x["id"] for x in attribute_assignment["carried_out_by"]]
            self.assertSetEqual(set(attribute_assignment_actors), set(joint_owners))

            # total payment branch should have only part property
            payment = [x for x in sale_with_buyers_joint_owners["part"] if x["type"]== "Payment"][0]
            self.assertTrue(len(payment) <= 4)

            # the information about payment receiver and giver should be moved to partial payment
            part_payment = payment.get("part")
            if part_payment:
                self.assertTrue({"paid_amount", "paid_from", "paid_to"}.issubset(part_payment[0].keys()))

        for sale_with_buyers_not_joint_owners in sales_with_buyers_not_joint_owners:
            # there should not be an attribute assignment branch
            self.assertEqual(len(sale_with_buyers_not_joint_owners['part']), 2)

            # total payment branch should have all the properties
            payment = [x for x in sale_with_buyers_not_joint_owners["part"] if x["type"]== "Payment"][0]
            self.assertTrue({"paid_amount", "paid_from", "paid_to"}.issubset(payment.keys()))

            # the partial payment should contain only the information about knoedler subpayment
            part_payment = payment.get("part")
            if part_payment:
                self.assertTrue({"paid_amount", "paid_to"}.issubset(part_payment[0].keys()))        


if __name__ == '__main__':
    unittest.main()
