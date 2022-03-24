#!/usr/bin/env python
""" Optimization.py: A collection of classes to optimize processes. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

# Imports
import time
# Own modules:
from Logging import Logger


class Optimize:
    """
    This class deals with all situations where processes need to be optimized.
    For example, when optimizing SQL code and 2 queries need to be compared to check whether they return the same
    output as expected.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('DataAccess.ReadData', loglevel).logger

    def compare_sql_queries(self, sql1, sql2, tr_connected):
        """
        This runs 2 slq queries and checks whether the returned values are the same for both queries. It also
        logs the time taken for both queries. This function can be used to verify that 2 queries indeed return the
        same values when optimizing SQL code.

        :param sql1: sql query 1
        :param sql2: sql query 2
        :param tr_connected: An initiated instance of the Teradata class of DataAccess.py
        :return: True if the queries have the same result, False otherwise., df1 and df2
        """
        # Run query 1:
        time0 = time.time()
        df1 = tr_connected.retrieve_dataframe(sql1)
        duration1 = time.time() - time0
        # Run query 2:
        time0 = time.time()
        df2 = tr_connected.retrieve_dataframe(sql2)
        duration2 = time.time() - time0
        self.logger.info('SQL query 1 took {} seconds ; SQL query 2 took {} seconds.'.format(duration1, duration2))
        # Sort both dataframes on all columns to ensure that they are ordered in the same way:
        df1.sort_values(by=list(df1.columns), inplace=True, ignore_index=True)
        df2.sort_values(by=list(df2.columns), inplace=True, ignore_index=True)
        # Compare the dataframes using the assert_frame_equal:
        if df1.equals(df2):
            return True, df1, df2
        else:
            return False, df1, df2
