#!/usr/bin/env python3 -B
import unittest

from tests import TestSalesPipelineOutput, classification_sets, classification_tree, classified_identifier_sets
from cromulent import vocab

vocab.add_attribute_assignment_check()

class PIRModelingTest_AR164(TestSalesPipelineOutput):
    def test_modeling_ar164(self):
        '''
        AR-164: Create Concordance for Visual Work Subject, Style, Entity Depicted for Sales Recores
        '''
        output = self.run_pipeline('ar164')
        visual_items = output['model-visual-item']


        import pprint
        pp = pprint.PrettyPrinter(indent=1)
        for k in visual_items.keys():
            print(k)

        expected_multiple_class_aat = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1575,0044%5Ba%5D,1787-04-24-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0018,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1928,0457,1937-04-06-VisItem' : {
                'https://vocab.getty.edu/aat/300386044'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1708,0092,1936-03-10-VisItem' : {
                'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1050,0038,1933-05-06-VisItem' : {
                'https://vocab.getty.edu/aat/300015637'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2732,0032,1942-05-08-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3532,0861,1920-12-13-VisItem' : {
                'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-677,0566,1932-03-15-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-977,0720,1933-02-21-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2156,0607,1913-02-10-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2894,1163,1943-04-01-VisItem' : {
                'https://vocab.getty.edu/aat/300055866', 'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1613,0341,1935-10-23-VisItem' : {
                'https://vocab.getty.edu/aat/300139140', 'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0123,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300139140', 'https://vocab.getty.edu/aat/300124520'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0161,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0102,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1200,1677,1908-06-30-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1266,0114%5Bb%5D,1908-11-25-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B573,1699,1905-02-23-VisItem' : {
                'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B573,1699,1905-02-23-VisItem' : {
                'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B830,0110,1906-10-09-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B448,0141,1904-03-16-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424', 'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1364,1531,1934-09-07-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055985', 'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2095,0034,1912-11-21-VisItem' : {
                'https://vocab.getty.edu/aat/300139140'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0010,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0015,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300015637'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A208,0004,1715-02-02-VisItem' : {
                'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0003,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0001,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093', 'https://vocab.getty.edu/aat/300410510'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1628,0073,1788-04-02-VisItem' : {
                'https://vocab.getty.edu/aat/300015638',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A505-A,0023,1755-11-25-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055985',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A505-A,0044,1755-11-25-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055985', 'https://vocab.getty.edu/aat/300410510'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2732,0027,1942-05-08-VisItem' : {
                'https://vocab.getty.edu/aat/300015638',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5207,0057,1928-02-28-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5252,0055,1928-04-17-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055985',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1817,0054,1936-09-11-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5484,0409,1928-12-19-VisItem' : {
                'https://vocab.getty.edu/aat/300033898',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5496,0027,1929-02-05-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055985', 'https://vocab.getty.edu/aat/300410510'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2728,0078,1916-06-21-VisItem' : {
                'https://vocab.getty.edu/aat/300386045',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5296,0070,1928-05-22-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300055866'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B4136,0184,1924-04-15-VisItem' : {
                'https://vocab.getty.edu/aat/300386045'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0289%5Bb%5D,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093', 'https://vocab.getty.edu/aat/300410510'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0313,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5207,0024,1928-02-28-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5496,0005,1929-02-05-VisItem' : {
                'https://vocab.getty.edu/aat/300015636'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3571,0169,1921-02-22-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2051,0164,1912-10-24-VisItem' : {
                'https://vocab.getty.edu/aat/300386045', 'https://vocab.getty.edu/aat/300234093'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1501,0042,1910-03-15-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3528,0050,1920-12-07-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2271,0083,1913-05-07-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3843,0098,1922-05-29-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5496,0064,1929-02-05-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5037,0540,1927-06-22-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B587,0053,1905-03-22-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5296,0020,1928-05-22-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5615,0100,1929-05-29-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B667,0883,1905-10-17-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3184,0350,1919-04-14-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3212,0047,1919-05-26-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B301,0001,1903-03-16-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2137,0021,1796-01-22-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1881,0056%5Bb%5D,1792-03-09-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2324,0037,1798-06-08-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2449,0052,1799-07-12-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1091,0078,1777-01-31-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1435,0029,1785-01-28-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2424,0481,1799-05-24-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A52,0067,1690-09-04-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546', 'https://vocab.getty.edu/aat/300015424',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1117,0032,1777-05-03-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A860,0033,1771-03-09-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1256,0063,1780-11-30-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424', 'https://vocab.getty.edu/aat/300235692', 'https://vocab.getty.edu/aat/300117546'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A727,0015,1765-11-20-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1127,0097,1777-09-12-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1160,0058,1778-05-15-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1091,0076,1777-01-31-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A822,0024,1769-10-26-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300139140',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1151,0014,1778-03-28-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300033898', 'https://vocab.getty.edu/aat/300055985'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A611,0074,1761-03-18-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A716,0084,1765-04-04-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1173,0007,1778-09-21-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A727,0040,1765-11-21-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A934,0067,1773-02-27-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2073,0041,1795-01-VisItem' : {
                'https://vocab.getty.edu/aat/300015636', 'https://vocab.getty.edu/aat/300015424'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1091,0015,1777-02-01-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1197,0021,1779-03-18-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A541,%5BA%5D0032,1758-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1153,0060,1778-04-11-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A689,0025,1764-04-19-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1002,0027,1774-12-15-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A745,0066,1766-03-15-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2008,0109,1793-12-19-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A690,0041,1764-05-09-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1265,0029,1781-03-23-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1722,0033,1790-02-19-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A846,0001,1770-07-18-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2581,0056%5BH%5D,1800-12-09-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2424,0478,1799-05-24-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2581,0056%5BH%5D,1800-12-09-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A233,0244,1722-11-17-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1454,0082,1785-04-28-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1604,0028,1788-01-25-VisItem' : {
                'https://vocab.getty.edu/aat/300015636',
            }
        }

        expected_repr_aat = {
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1050,0038,1933-05-06-VisItem' : {
                'https://vocab.getty.edu/aat/300386296'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-2732,0032,1942-05-08-VisItem' : {
                'https://vocab.getty.edu/aat/300189808'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B3532,0861,1920-12-13-VisItem' : {
                'https://vocab.getty.edu/aat/300189808'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0161,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300025450'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1153,0102,1908-04-08-VisItem' : {
                'https://vocab.getty.edu/aat/300004792'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1200,1677,1908-06-30-VisItem' : {
                'https://vocab.getty.edu/aat/300185692'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B1266,0114%5Bb%5D,1908-11-25-VisItem' : {
                'https://vocab.getty.edu/aat/300222725'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B830,0110,1906-10-09-VisItem' : {
                'https://vocab.getty.edu/aat/300189808'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A649-A,0015,1763-VisItem' : {
                'https://vocab.getty.edu/aat/300386296'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A208,0004,1715-02-02-VisItem' : {
                'https://vocab.getty.edu/aat/300189808'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A505-A,0023,1755-11-25-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5207,0057,1928-02-28-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5252,0055,1928-04-17-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-1817,0054,1936-09-11-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B2728,0078,1916-06-21-VisItem' : {
                'https://vocab.getty.edu/aat/300185692',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B4136,0184,1924-04-15-VisItem' : {
                'https://vocab.getty.edu/aat/300025450'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,D-B5496,0005,1929-02-05-VisItem' : {
                'https://vocab.getty.edu/aat/300008057'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2137,0021,1796-01-22-VisItem' : {
                'https://vocab.getty.edu/aat/300008057',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1881,0056%5Bb%5D,1792-03-09-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2324,0037,1798-06-08-VisItem' : {
                'https://vocab.getty.edu/aat/300189808', 'https://vocab.getty.edu/aat/300008057'
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2449,0052,1799-07-12-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1091,0078,1777-01-31-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1435,0029,1785-01-28-VisItem' : {
                'https://vocab.getty.edu/aat/300189808', 'https://vocab.getty.edu/aat/300008057',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A2424,0481,1799-05-24-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1117,0032,1777-05-03-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A860,0033,1771-03-09-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A727,0015,1765-11-20-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1127,0097,1777-09-12-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1160,0058,1778-05-15-VisItem' : {
                'https://vocab.getty.edu/aat/300189808', 'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1091,0076,1777-01-31-VisItem' : {
                'https://vocab.getty.edu/aat/300004792', 'https://vocab.getty.edu/aat/300008057',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1151,0014,1778-03-28-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A611,0074,1761-03-18-VisItem' : {
                'https://vocab.getty.edu/aat/300189808',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A716,0084,1765-04-04-VisItem' : {
                'https://vocab.getty.edu/aat/300185692',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A1173,0007,1778-09-21-VisItem' : {
                'https://vocab.getty.edu/aat/300185692',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A727,0040,1765-11-21-VisItem' : {
                'https://vocab.getty.edu/aat/300263552',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A934,0067,1773-02-27-VisItem' : {
                'https://vocab.getty.edu/aat/300004792',
            },
            'tag:getty.edu,2019:digital:pipeline:REPLACE-WITH-UUID:sales#OBJ,Br-A233,0244,1722-11-17-VisItem' : {
                'https://vocab.getty.edu/aat/300185692',
            },
        }

        for id, aat_urls in expected_multiple_class_aat.items():
            for aat_url in aat_urls:
                self.assertIn(aat_url, classification_sets(visual_items[id], key='id'))

        for id, aat_urls in expected_repr_aat.items():
            for aat_url in aat_urls:
                self.assertIn(aat_url, classification_sets(visual_items[id], classification_key="represents_instance_of_type", key='id'))

if __name__ == '__main__':
    unittest.main()
