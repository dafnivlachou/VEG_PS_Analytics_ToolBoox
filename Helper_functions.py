import pandas as pd
import numpy as np
from datetime import datetime
import collections
# Own modules:
from Logging import Logger


class HelperFunctions:
    """
    This class deals contains some generic helper functions that are used in multiple projects.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('Helper_functions.HelperFunctions', loglevel).logger

    @staticmethod
    def is_empty(entry):
        """
        This function checks whether a entry is empty or not. Entries considered empty are:

        - NULL (np.nan, as checked using pd.isna())
        - ''
        - ' ' (only a space)

        It returns True if the entry is empty, False otherwise.

        :return: True or False
        """
        # u"\u00A0" is to also include the 'NBSP' character (non breaking space)
        empty_list = ['', ' ', 'N/A', u"\u00A0"]
        if entry not in empty_list and not pd.isna(entry):  # not empty
            return False
        else:
            return True

    def replace_empty_entries(self, column_name, df):
        """
        This function replaces empty values (as defined in is_empty() ) with np.nan.
        This prevents issues further down the line, where a space for example is not seen as being empty.

        :param df: pandas dataframe
        :param column_name: the column to replace the empty values
        :return: a pandas dataframe with the column replaced.
        """
        new_values = []
        for row in df.itertuples(index=False):
            value = row[df.columns.get_loc(column_name)]
            if self.is_empty(value):
                new_values.append(np.nan)
            else:
                new_values.append(value)
        df[column_name] = new_values  # replace the column

        return df

    @staticmethod
    def to_date(stringdate):
        """
        This function takes a stringdate in the form DD/MM/YYYY and returns it as a date object.

        :param stringdate: date as a string in the form: DD/MM/YYYY
        :return: date object
        """
        out = None
        if type(stringdate) is str:
            out = datetime.strptime(stringdate, '%m/%d/%Y')
        return out

    @staticmethod
    def to_string(dateobject):
        """
        This function takes a dateobject and returns it as a string in the from DD/MM/YYYY.

        :param dateobject: a date object
        :return: a string in the form DD/MM/YYYY
        """
        out = None
        if dateobject is not None:
            out = dateobject.strftime('%m/%d/%Y')
        return out

    @staticmethod
    def truncate_date(dateobject):
        """
        This function takes a dateobject and replaces the 'day' part with 1. This makes it easier to compare dates
        based on month and year only.

        '28-12-2021' would become '01-12-2021'

        :param dateobject: a date object
        :return: the same dateobject with the day replaced by 1
        """
        out = None
        if dateobject is not None:
            out = dateobject.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return out

    def list_to_querystring(self, inputlist):
        """
        This function takes a list as input and returns it as a string that can be used in an SQL query.
        It will surround each entry in the list with single quotes.
        The input list [123, 345, 456] will thus be returned as:

         '123', '345', '456'

        Which can subsequently be used to replace a keyword in a query string to retrieve for example data for a
        set of batch numbers.

        :param inputlist: a list to transform to a querystring
        :return: a query string
        """
        query_string = ""
        for i in inputlist:
            if not self.is_empty(i):  # ignore empty entries
                query_string = query_string + "'{}', ".format(i)
        # Remove the last character because we don't want to end in a comma:
        query_string = query_string[:-2]
        return query_string

    def return_earliest_date(self, datelist):
        """
        This function takes a list of datestrings (MM/DD/YYYY) and returns the earliest date as datestring (MM/DD/YYYY).
        The input list is allowed to have empty values (None, np.nan, ''). It will return None if no dates are
        included in the input list.

        :param datelist: a list of datestrings (MM/DD/YYYY)
        :return: earliest date as string (MM/DD/YYYY)
        """
        # remove None values:
        datelist = [i for i in datelist if not self.is_empty(i)]
        if len(datelist) > 0:
            # First, convert all dates in the list to datetime objects:
            dates = [self.to_date(i) for i in datelist]
            # Identify the earliest date and transform that to string again:
            min_date = min(dates)
            min_date = self.to_string(min_date)
        else:
            # There are no dates in the list, return None.
            min_date = None
        return min_date

    def return_latest_date(self, datelist):
        """
        This function takes a list of datestrings (MM/DD/YYYY) and returns the latest date as datestring (MM/DD/YYYY).
        The input list is allowed to have empty values (None, np.nan, ''). It will return None if no dates are
        included in the input list.

        :param datelist: a list of datestrings (MM/DD/YYYY)
        :return: latest date as string (MM/DD/YYYY)
        """
        # remove None values:
        datelist = [i for i in datelist if not self.is_empty(i)]
        if len(datelist) > 0:
            # First, convert all dates in the list to datetime objects:
            dates = [self.to_date(i) for i in datelist]
            # Identify the latest date and transform that to string again:
            max_date = max(dates)
            max_date = self.to_string(max_date)
        else:
            # There are no dates in the list, return None.
            max_date = None
        return max_date

    @staticmethod
    def transform_date(date_entry=str):
        """
        This function transforms various string dateformats to 'MM/DD/YYYY' as a dateformat.

        Formats supported:
        - 2021-05-25T08:20:25Z
        - 05/26/2021 (supported for mixed use cases)
        - 05/26/21
        - 05-26-2021
        - 05-26-21

        Note, don't send datestrings if they are formatted DD-MM-YYYY or DD/MM/YYYY because this will lead to errors.

        It returns None if it isn't able to tranform the datestring.

        :param date_entry: a string date
        :return: a string date in the form (MM/DD/YYYY)
        """
        if len(date_entry.split('T')) > 1:  # Equal to first format
            datepart = date_entry.split('T')[0].split('-')
            return_date = '{}/{}/{}'.format(datepart[1], datepart[2], datepart[0])
        elif len(date_entry.split('/')) > 2:  # Equal to second/third format
            datepart = date_entry.split('/')
            if len(datepart[2]) == 2:
                datepart[2] = '20' + datepart[2]  # assumes 21st century
            return_date = '{}/{}/{}'.format(datepart[0], datepart[1], datepart[2])
        elif len(date_entry.split('-')) > 2:  # Equal to fourth/fifth format
            datepart = date_entry.split('-')
            if len(datepart[2]) == 2:
                datepart[2] = '20' + datepart[2]  # assumes 21st century
            return_date = '{}/{}/{}'.format(datepart[0], datepart[1], datepart[2])
        else:
            return_date = None
        return return_date

    @staticmethod
    def identify_duplicates(list_input=list):
        """
        This function takes a list as input and returns a list with the values that are duplicated.

        """
        output = [item for item, count in collections.Counter(list_input).items() if count > 1]
        return output
