#!/usr/bin/env python3 -B
import unittest

from tests import TestKnoedlerPipelineOutput, classified_identifiers, classification_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR86(TestKnoedlerPipelineOutput):
    def test_modeling_ar86(self):
        '''
        AR-86: Improve modeling of unsold purchases and inventorying
        '''
        output = self.run_pipeline('ar86')
        activities = output['model-activity']

        # There are three entries for this object (with transaction types "unsold", "unsold", and "sold").
        # From these entries, we should see:
        #   - 1 purchase event ("unsold" but with seller information)
        #   - 2 inventorying event ("unsold" but no seller information, and also "sold" with no seller information)
        #   - 1 sale event ("sold")

        k_purchases = [a for a in activities.values() if a.get('_label', '').startswith('Knoedler Purchase of Stock Number 5323')]
        self.assertEqual(len(k_purchases), 1)

        k_inventorying = [a for a in activities.values() if a.get('_label', '').startswith('Knoedler Inventorying of Stock Number 5323')]
        self.assertEqual(len(k_inventorying), 2)

        k_sales = [a for a in activities.values() if a.get('_label', '').startswith('Knoedler Sale of Stock Number 5323')]
        self.assertEqual(len(k_sales), 1)

        self.assertEqual(len(activities), 4)

        # Also, the two inventorying events should have price information representing Knoedler's evaluated worth of the object
        # In this case, both inventorying activities resulted in the same evaluated amount (1250 francs).
        for act in k_inventorying:
            assignments = [a for a in act.get('part', []) if a.get('_label', '').startswith('Evaluated worth of Stock Number 5323')]
            self.assertEqual(len(assignments), 1)
            assignment = assignments[0]

            self.assertTrue(assignment['_label'].startswith('Evaluated worth of Stock Number 5323'))
            self.assertEqual(assignment['assigned_property'], 'dimension')
            self.assertEqual(assignment['assigned'][0]['_label'], '1,250.00 francs')
            self.assertEqual(assignment['assigned_to']['_label'], '[Language of fair title info from Sales Book 7, 1892-1900, f.36]')
            self.assertEqual(classification_sets(assignment['assigned'][0]), {'Valuation'})


if __name__ == '__main__':
    unittest.main()
