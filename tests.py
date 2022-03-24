import unittest
import os
import numpy as np
import pandas as pd
from datetime import datetime
# Own modules:
from DataAccess import WriteData
from Transformations import Transform, Normalize
from Helper_functions import HelperFunctions


class TestWriteData(unittest.TestCase):

    def test_list_to_textfile(self):
        wd = WriteData()
        input_list = ['Row1\n', 'Row2\n', 'Row3', 'Row4']
        expected_outcome = "Row1\nRow2\nRow3Row4"
        filename = 'list_to_textfile_test.txt'
        try:
            wd.list_to_textfile(input_list, filename)
            with open(filename, 'r') as f:
                outcome = f.read()
        finally:
            os.remove(filename)

        self.assertEqual(expected_outcome, outcome)


class TestTransformations(unittest.TestCase):

    def test_transform_quantity(self):
        tr = Transform(50)
        input_values = [(34, 'KPS'), (0.01, 'KCS'), (2, 'MK'), (400, 'MG')]
        expected_results = [(34, 'KG'), (0.01, 'KG'), (2, 'MK'), (None, None)]
        results = [(tr.transform_quantity(quantity, uom)) for quantity, uom in input_values]

        self.assertEqual(expected_results, results)

    def test_transform_rate(self):
        tr = Transform(50)
        input_values = [(700, 'KG/HAR'), (0.45, 'KG/M2'), (200, 'MK/HAR'), (0.23, 'MK/M2'), (0.24, 'MK/MK')]
        expected_results = [(0.07, 'KG/M2'), (0.45, 'KG/M2'), (0.02, 'MK/M2'), (0.23, 'MK/M2'), (None, None)]
        results = [(tr.transform_rate(rate, uom)) for rate, uom in input_values]

        self.assertEqual(expected_results, results)

    def test_transform_area(self):
        tr = Transform(50)
        input_values = [(323, 'M2'), (0.123, 'HAR'), (1, 'ACR'), (300, 'cm2')]
        expected_results = [323, 1230, 4046.8564224, None]
        results = [tr.transform_area(area, uom) for area, uom in input_values]

        self.assertEqual(expected_results, results)


class TestHelperFunctions(unittest.TestCase):

    def test_is_empty(self):
        """
        This function checks the is_empty() function.

        """
        hf = HelperFunctions(loglevel='ERROR')
        input_values = [1, 'not empty', '', ' ', 'a', np.nan, 'N/A']
        expected_output = [False, False, True, True, False, True, True]
        output = [hf.is_empty(i) for i in input_values]
        self.assertEqual(expected_output, output)

    def test_to_date(self):
        """
        This function tests the to_date() function.

        """
        input_dates = ['01/12/2021', '12/06/1998', '01/31/1900', None, 1]
        expected_output = [datetime.strptime('01/12/2021', '%m/%d/%Y'), datetime.strptime('12/06/1998', '%m/%d/%Y'),
                           datetime.strptime('01/31/1900', '%m/%d/%Y'), None, None]
        hf = HelperFunctions(loglevel='ERROR')
        output = [hf.to_date(i) for i in input_dates]

        self.assertEqual(expected_output, output)

    def test_to_string(self):
        """
        This function test the to_string() function.

        """
        expected_output = ['01/12/2021', '12/06/1998', '01/31/1900', None]
        input_dates = [datetime.strptime(i, '%m/%d/%Y') if i is not None else None for i in expected_output]
        hf = HelperFunctions(loglevel='ERROR')
        output = [hf.to_string(i) for i in input_dates]

        self.assertEqual(expected_output, output)

    def test_replace_empty_entries(self):
        """
        This function tests the replace_empty_entries() function.

        """
        # Create a test df:
        column1 = ['value1', 'value2', '', ' ', np.nan, 'value3', ' ']
        df = pd.DataFrame()
        df['column1'] = column1
        # Set expected output:
        expected_output = ['value1', 'value2', np.nan, np.nan, np.nan, 'value3', np.nan]
        # Initiate the class and run the function:
        hf = HelperFunctions(loglevel='ERROR')
        df_out = hf.replace_empty_entries('column1', df)
        output = df_out['column1'].tolist()
        # Compare:
        self.assertEqual(expected_output, output)

    def test_list_to_querystring(self):
        """
        This function tests the list_to_querystring() function.

        """
        list1 = [1234, 'hallo', '', None, 1, 'bla', 456, ' ']
        expected_output = "'1234', 'hallo', '1', 'bla', '456'"
        # Initiate the class and run the function:
        hf = HelperFunctions(loglevel='ERROR')
        output = hf.list_to_querystring(list1)
        self.assertEqual(expected_output, output)

    def test_return_earliest_date(self):
        """
        This function tests the return_earliest_date() function.

        """
        datelists = [['12/15/2008', '12/16/2008', '11/30/2008'],
                     ['01/31/2018', '01/31/2021', '01/31/2028'],
                     ['03/03/2016', '12/17/2018', '01/01/2015'],
                     ['', np.nan, None]]
        expected_output = ['11/30/2008', '01/31/2018', '01/01/2015', None]
        # Initiate the class and run the function:
        hf = HelperFunctions(loglevel='ERROR')
        output = [hf.return_earliest_date(datelist) for datelist in datelists]
        self.assertEqual(expected_output, output)

    def test_return_latest_date(self):
        """
        This function tests the return_latest_date() function.

        """
        datelists = [['12/15/2008', '12/16/2008', '11/30/2008'],
                     ['01/31/2018', '01/31/2021', '01/31/2028'],
                     ['03/03/2016', '12/17/2018', '01/01/2015'],
                     ['', np.nan, None]]
        expected_output = ['12/16/2008', '01/31/2028', '12/17/2018', None]
        # Initiate the class and run the function:
        hf = HelperFunctions(loglevel='ERROR')
        output = [hf.return_latest_date(datelist) for datelist in datelists]
        self.assertEqual(expected_output, output)

    def test_transform_date(self):
        """
        This function tests the transform_date() function.

        """
        datelist = ['2021-03-22T14:37:28Z', '2021-05-25T08:20:25Z', '05/26/2021', '05/26/21', '05-26-2021', '05-26-21']
        expected_output = ['03/22/2021', '05/25/2021', '05/26/2021', '05/26/2021', '05/26/2021', '05/26/2021']
        # Initiate the class and run the function:
        hf = HelperFunctions(loglevel='ERROR')
        output = [hf.transform_date(i) for i in datelist]
        self.assertEqual(expected_output, output)


class TestNormalize(unittest.TestCase):

    def test_normalize_list(self):
        nm = Normalize(loglevel='CRITICAL')
        input_lists = [[1, 4, 6, 7],
                       [-1, 4, 6, 7],
                       [1, 4, 6, 16]]
        expected_outcome = [[0.083, 0.500, 0.778, 0.917],
                            [],
                            [0.057, 0.229, 0.343, 0.914]]
        output = [nm.normalize_list(i, 10) for i in input_lists]
        self.assertEqual(expected_outcome, output)


if __name__ == '__main__':
    unittest.main()
