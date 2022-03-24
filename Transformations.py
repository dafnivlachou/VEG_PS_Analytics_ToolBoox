#!/usr/bin/env python
""" Transformations.py: A collection of classes to transform data. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

import time
import os
import pandas as pd
# Own modules
from Logging import Logger
from DataAccess import ReadData


class Transform:

    """
    This class is used to transform measurements to a common output unit of measurement (Harmonize) so that
    observations can be compared to each other. For example, all area measurements to m2.

    Source: http://t3.apptrix.com/syteline/Language/en-US/fields/i/iso_um_ums.htm

    """

    acre = 4046.8564224
    hectare = 10000

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('Transformations.Transform', loglevel).logger

    def transform_quantity(self, quantity, uom):
        """
        This function transforms the quantity (in weight or number) to a common output. For the weight it is transformed
        to KG and for the number it is transformed to MK, where 1 MK = 1000 seeds.
        It returns the transformed data and the output UOM.

        NOTE: KPS is 'KG Paid Seed' and KCS is 'KG Clean Seed', but both in KG.
        NOTE: Function is sort of useless for now, will prove more valuable if other uoms are added.

        :param quantity: input quantity
        :param uom: input uom
        :return: transformed quantity, output uom
        """
        uom_supported = ['KPS', 'KCS', 'MK']
        uom_out = uom
        if uom not in uom_supported:
            self._uom_not_supported(self.transform_quantity.__name__, uom, uom_supported)
            return None, None
        if uom == 'KPS' or uom == 'KCS':
            uom_out = 'KG'
        return quantity, uom_out

    def transform_rate(self, rate, uom):
        """
        This function transforms the rate (weight per area or number per area) to a common output. For the weight it
        is transformed to KG/M2 and for the number it is transformed to MK/M2, where 1 MK = 1000 seeds.
        It returns the transformed data and the output UOM.

        NOTE: KG/M2 and MK/M2 are accepted as input but are returned without transformation.

        :param rate: input rate
        :param uom: input uom
        :return: transformed rate, output uom
        """
        uom_supported = ['KG/HAR', 'KG/M2', 'MK/HAR', 'MK/M2']
        uom_out = uom
        if uom not in uom_supported:
            self._uom_not_supported(self.transform_rate.__name__, uom, uom_supported)
            return None, None
        if uom == 'KG/HAR':
            rate = rate / self.hectare
            uom_out = 'KG/M2'
        if uom == 'MK/MK' or uom == 'MK/HAR':
            rate = rate / self.hectare
            uom_out = 'MK/M2'
        return rate, uom_out

    def transform_area(self, area, uom):
        """
        This function takes an area as input and returns the area in M2.

        NOTE: It accepts M2 as input but will in that case return the input value as output value.

        :param area: input area
        :param uom: input uom
        :return: area in m2
        """
        uom_supported = ['M2', 'HAR', 'ACR']
        if uom not in uom_supported:
            self._uom_not_supported(self.transform_area.__name__, uom, uom_supported)
            return None
        if uom == 'HAR':
            area = area * self.hectare
        elif uom == 'ACR':
            area = area * self.acre
        return area

    def _uom_not_supported(self, name, uom, uom_supported):
        self.logger.error("{}: uom {} currently not supported, supported uom's are: {}".format(name, uom,
                                                                                               uom_supported))


class Exchange:
    """
    Class to transform one currency to another. This class retrieves the current exchange rates when it is initiated.
    The 'min_date' parameter (YYYY-MM-DD) is used to determine how far back it should retrieve exchange rates for
    (default = 2015-01-01).

    'td' is a Teradata connection object  (DataAccess -> Teradata)

    """

    def __init__(self, td, min_date='2015-01-01', loglevel='INFO'):
        self.logger = Logger('Transformations.Exchange', loglevel).logger
        self.rd = ReadData(loglevel=loglevel)
        self.td = td
        exchange_df = self._retrieve_exchange_rates(min_date)
        # Create a subset for to EUR and to USD so we only need to do that once:
        self.to_usd = exchange_df[(exchange_df['TO_CRNCY_CD'] == 'USD')]
        self.to_eur = exchange_df[(exchange_df['TO_CRNCY_CD'] == 'EUR')]

    def _retrieve_exchange_rates(self, min_date):
        """
        This function retrieves the current exchange rates for a subset of currencies to USD from the Teradata
        database. It also retrieves the conversion from USD to EUR. It returns the data as a pandas dataframe.

        :param min_date: the earliest date for which we need exchange rate information in the form YYYY-MM-DD
        :return: pandas dataframe with exchange rates.
        """
        time0 = time.time()
        self.logger.info('Retrieving exchange rates.')
        # Read the query:
        sql_file = os.path.dirname(os.path.abspath(__file__)) + '/Queries/exchange_rates.sql'
        # Create a replace dictionary for the query:
        replace_dict = {'$DATE1$': min_date}
        sql = self.rd.read_sql(sql_file, replace_dict)
        # Run the query:
        df = self.td.retrieve_dataframe(sql)
        # Format the dates, replace the year 9999 to 2200 because pandas cannot deal with the year 9999:
        df['VALID_FROM_DT'] = pd.to_datetime(df['VALID_FROM_DT'], format='%m/%d/%Y')
        df['VALID_TO_DT'] = df['VALID_TO_DT'].replace(['12/31/9999'], '12/31/2200')
        df['VALID_TO_DT'] = pd.to_datetime(df['VALID_TO_DT'], format='%m/%d/%Y')
        self.logger.info('Exchange rates retrieved in {} seconds.'.format(time.time() - time0))
        return df

    def convert_to_usd(self, amount, uom, exchange_date):
        """
        This function transforms the input 'amount' to USD.

        :param amount: the currency amount
        :param uom: the current unit of measurement (EUR/USD etc.)
        :param exchange_date: the date to use to determine the exchange rate (MM/DD/YYYY)
        :return: the amount in USD
        """
        if uom == 'USD':
            usd = amount
        else:
            # Identify the exchange rate from the uom to USD:
            sub_exchange_df = self.to_usd[(self.to_usd['FROM_CRNCY_CD'] == uom) &
                                          (self.to_usd['VALID_FROM_DT'] <= exchange_date) &
                                          (self.to_usd['VALID_TO_DT'] >= exchange_date)]
            exchange_rate = sub_exchange_df['EXCHG_RT_VAL'].tolist()[0]  # take first value, should only be one
            if len(sub_exchange_df) > 1:
                self.logger.warning('Multiple possible USD exchange rates found for uom {} and exchange date {}.'
                                    .format(uom, exchange_date))
            usd = amount * exchange_rate
        return usd

    def convert_to_eur(self, amount, uom, exchange_date):
        """
        This function transforms the input 'amount' to EUR.

        NOTE: it transforms the amount to USD first (if not euro already) and then transforms the USD amount to EUR.

        :param amount: the currency amount
        :param uom: the current unit of measurement (EUR/USD etc.)
        :param exchange_date: the date to use to determine the exchange rate (MM/DD/YYYY)
        :return: the amount in EUR
        """
        if uom == 'EUR':
            eur = amount
        else:
            usd = self.convert_to_usd(amount, uom, exchange_date)
            sub_eur = self.to_eur[(self.to_eur['VALID_FROM_DT'] <= exchange_date) &
                                  (self.to_eur['VALID_TO_DT'] >= exchange_date)]
            eur_exchange_rt = sub_eur['EXCHG_RT_VAL'].tolist()[0]  # take first value, should only be one
            if len(sub_eur) > 1:
                self.logger.warning('Multiple possible EUR exchange rates found for uom {} and exchange date {}.'
                                    .format(uom, exchange_date))
            eur = usd * eur_exchange_rt
        return eur


class Normalize:
    """
    A class to normalize data.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('Transformations.normalize', loglevel).logger

    def normalize_list(self, inputlist, buffer_percentage):
        """
        This function normalizes the values in a list. It takes the min and max values of the list. It then moves
        the min value lower by <buffer_percentage> of the range and the max up by the same.
        This is done to allow future measurements that fall slightly outside of the current range to still be included.

        It then normalizes all values in the list using the formula:

            normalized_value = (current_value - min_value) / range

        An example, for the lists [1, 4, 6, 7], the min = 1, the max = 7. This gives us a range of 6.
        With a buffer percentage of 10%, the new minimum = 1 - 0.1 * 6 = 0.4.
        The new max = 7 + 0.1 * 6 = 7.6
        The adjusted range is then 7.6 - 0.4 = 7.2

        We then normalize the list with min = 0.4, max = 7.6 and range = 7.2

        (1 - 0.4) / 7.2 = 0.083
        (4 - 0.4) / 7.2 = 0.500
        (6 - 0.4) / 7.2 = 0.778
        (7 - 0.4) / 7.2 = 0.917

        The output list = [0.083, 0.500, 0.800, 0.917]

        NOTE: only positive values!

        :param inputlist: a list of input values (floats/integers)
        :param buffer_percentage: percentage to buffer the range as integer, where 10 would be 10%.
        :return: list of normalized values
        """
        min_val = min(inputlist)
        max_val = max(inputlist)
        range_values = max_val - min_val
        if min_val < 0:
            self.logger.error('This function cannot process lists with negative values.')
            output_list = []
        else:
            buffer_size = (buffer_percentage / 100) * range_values
            # Update min, max and range values using the buffer. If min value is below 0, set to 0 instead.
            min_val = min_val - buffer_size
            if min_val < 0:
                min_val = 0
            max_val = max_val + buffer_size
            range_values = max_val - min_val
            # Normalize values:
            output_list = [round((i - min_val) / range_values, 3) for i in inputlist]

        return output_list
