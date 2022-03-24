#!/usr/bin/env python
""" DataAccess.py: A collection of classes to interact with data/files. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

# Imports
from datetime import datetime
import time
import os
import zipfile
import matplotlib.pyplot as plt
import pandas as pd
from sqlalchemy import create_engine
# Own modules:
from Logging import Logger


class ReadData:
    """
    This class deals with all interactions where reading data from some file is needed.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('DataAccess.ReadData', loglevel).logger

    def read_csv_to_df(self, filelocation, sep=',', dtype_dict=None):
        """
        This function reads a csv file to a pandas dataframe and returns that dataframe.

        :param filelocation: full path to the file location of the csv-file.
        :param sep: separator (OPTIONAL; default = ',')
        :param dtype_dict: (OPTIONAL) Dictionary of datatypes with {'column_name': 'dtype'}
        :return: pandas dataframe
        """
        if dtype_dict is not None:
            df = pd.read_csv(filelocation, sep=sep, dtype=dtype_dict)
        else:
            df = pd.read_csv(filelocation, sep=sep)
        self.logger.info('{} read to dataframe.'.format(filelocation))
        return df

    def read_pickle_to_df(self, filelocation):
        """
        This function reads a pickled dataframe from file to df.

        :param filelocation: full path to the file location of the csv-file.
        :return: pandas dataframe
        """
        df = pd.read_pickle(filelocation)
        self.logger.info('{} read to dataframe.'.format(filelocation))
        return df

    def read_zip_to_df(self, filelocation):
        """
        This function unpacks a zipfile and checks the extension to determine whether it should read it to a dataframe
        as csv/xlsx/pk1.
        It then removes the unpacked file again.

        NOTE: it assumes default settings when reading files. So, comma-separated csv and sheet0 for an excel file.
        If you have different settings or multiple sheets, unpack the zipfile in your own script first and then call
        the read function separately.

        :param filelocation: full path to the file location of the csv-file.
        :return: pandas dataframe
        """
        # Unpack to current workdirectory:
        with zipfile.ZipFile(filelocation, 'r') as zp:
            # Get filename of the file in the zip:
            if len(zp.namelist()) == 1:
                filename = zp.namelist()[0]
            else:
                self.logger.error('Zipfile {} contains multiple files, cannot be used here.'.format(filelocation))
                exit()
            zp.extractall(os.getcwd())
        # Check the extension of the filename to see which one should be used to read it to a dataframe:
        extension = filename.split('.')[-1]
        fileloc = '{}/{}'.format(os.getcwd(), filename)
        if extension == 'csv':
            df = self.read_csv_to_df(fileloc)
        elif extension == 'xlsx':
            df = self.read_excel_to_df(fileloc)
        elif extension == 'pk1':
            df = self.read_pickle_to_df(fileloc)
        else:
            self.logger.error('Filetype {} not supported.'.format(extension))
            df = None
        # Remove unpacked file:
        os.remove(fileloc)
        return df

    def read_excel_to_df(self, filelocation, sheet_number=0, skiprows=0):
        """
        This functions reads a excel file to a pandas dataframe and returns that dataframe.
        NOTE: it does not interpret the data type of the columns but simply takes the datatype as stored in excel!

        :param filelocation: full path to the file location of the excel-file.
        :param sheet_number: which sheet should be read, 0 = 1st sheet, 1  second sheet etc.
            Can also be string: 'sheet1' (OPTIONAL; default = 0)
        :param skiprows: Whether to skip rows, can be a list of integers [1,4,6] with which rows to skip or a single
            integer, in which case it will skip the first n rows. Can be used to ignore the 3 empty lines of a report.
        :return: pandas dataframe
        """
        df = pd.read_excel(filelocation, sheet_number, skiprows=skiprows, dtype=object)
        self.logger.info('{} sheet {} read to dataframe.'.format(filelocation, sheet_number))
        return df

    @staticmethod
    def read_sql(filelocation, replace_dict=None):
        """
        This function reads an sql file to a variable so that it can be run with for example
        Teradata.retrieve_dataframe().
        It reads the file line by line and returns a string variable of the query.
        The function can also replace values in the query using a dictionary. This is useful if the query date needs
        to be changed dynamically for example. Given the dictionary:
        dict = { '01-01-1900': '01-01-2018' }

        All occurances of the date 01-01-1900 in the query will be replaced by 01-01-2018.

        :param filelocation: full path to the file location of the sql query
        :param replace_dict: a dictionary of keys in the query that need to be replaced with another value (OPTIONAL)
        :return: SQL string query
        """
        sql = ''''''
        with open(filelocation, 'r') as f:
            for line in f:
                sql = sql + line
        # Replace all values in the query that need to be replaced:
        if replace_dict is not None:
            for key in replace_dict:
                sql = sql.replace(key, replace_dict[key])
        return sql


class WriteData:
    """
    This class deals with all interactions where writing something to a file is needed.

    """

    def __init__(self, loglevel='INFO'):
        self.logger = Logger('DataAccess.WriteData', loglevel).logger

    def save_plot(self, fig, location, dpi=300):
        """
        This function saves a matplotlib figure to disk.

        :param fig: matplotlib fig instance
        :param location: full path to the save location
        :param dpi: (OPTIONAL, default=300) the dpi to use for saving the image.
        """
        fig.savefig(location, dpi=dpi)
        plt.close()  # close the fig so it doesn't interfere with potential subsequent plots
        self.logger.info('Plot saved to {}'.format(location))

    def list_to_textfile(self, input_list, location):
        """
        This function writes a list to textfile. It loops through all entries in the [input_list] and writes them to the
        textfile which is saved at  [location].

        :param input_list: a list of strings to write to file
        :param location: full path where to save the textfile
        """
        with open(location, 'w') as f:
            for i in input_list:
                f.write(i)
        f.close()
        self.logger.info('Data from list written to {}'.format(location))

    def save_df(self, df, location, name, sort_by=None, split_by=None, filetype='csv'):
        """
        This function saves a pandas dataframe to a csv/xlsx/pk1 file. It can optionally sort the dataframe before
        exporting. It can also split the dataframe by a column before exporting and export a single file per column
        level. If exporting by column level, the complete file will also be written. Index is not written (unless
        filetype = pk1).

        Note that when reading/writing, pk1 files are much faster.

        :param df: the dataframe to be saved
        :param location: the location on the disk where to save the file (full path)
        :param name: the name to use for the file without file extension (so no .csv)
        :param sort_by: (OPTIONAL) by which column the dataframe needs to be sorted before saving
        :param split_by: (OPTIONAL) the dataframe will be split by this column and a separate csv file will be created
        :param filetype: (OPTIONAL) which file type needs to be saved, 'csv', 'xlsx' or 'pk1', default = csv.
         for each level of the column. The name will be appended by the level.
        """
        filetypes = ['csv', 'xlsx', 'pk1']
        if filetype in filetypes:
            if sort_by is not None:
                df.sort_values(by=[sort_by], inplace=True)
            if filetype == 'csv':
                df.to_csv('{}/{}.csv'.format(location, name), index=False)
            elif filetype == 'xlsx':
                writer = pd.ExcelWriter('{}/{}.xlsx'.format(location, name), engine='xlsxwriter')
                df.to_excel(writer, sheet_name='Sheet1', index=False)  # send df to writer
                worksheet = writer.sheets['Sheet1']  # pull worksheet object
                for idx, col in enumerate(df):  # loop through all columns
                    series = df[col]
                    if isinstance(series, pd.Series):
                        max_len = max((
                            series.astype(str).map(len).max(),  # len of largest item
                            len(str(series.name))  # len of column name/header
                        )) + 1  # adding a little extra space
                    else:
                        max_len = 100
                    # Set the maximum width to 100 to prevent excessive wide columns:
                    if max_len > 100:
                        max_len = 100
                    worksheet.set_column(idx, idx, max_len)  # set column width
                writer.save()
            if split_by is not None:
                values = df[split_by].unique().tolist()
                for value in values:
                    subdf = df[(df[split_by] == value)]  # subset df for column level
                    savename = '{0}/{1}_{2}.'.format(location, name, value)
                    if filetype == 'csv':
                        subdf.to_csv(savename + filetype, index=False)
                    elif filetype == 'xlsx':
                        subdf.to_excel(savename + filetype, index=False)
            elif filetype == 'pk1':
                df.to_pickle('{}/{}.pk1'.format(location, name))
        else:
            self.logger.error("Filetype '{}' currently not supported, choose csv, xlsx or pk1.".format(filetype))


class SQL:

    logger = None
    cus = None
    con = None

    def execute_query(self, sql_query):
        """
        This function executes an sql query (without returning anything).

        :param sql_query: the query to execute (string)
        """
        self.logger.info('Executing the SQL query: \n{}'.format(sql_query))
        # Execute and commit the query:
        self.cus.execute(sql_query)
        self.con.commit()

    def select_query(self, sql_query):
        """
        This function runs the given sql_query and returns the results of that query.
        The results can be looped through using a for-loop.

        :param sql_query: an SQL query string in the form "select * from DB where x = 5;"
        :return: a list of results, where each database row is a single entry in the results list.
        """
        self.logger.info('Running the SQL query: "{0}"'.format(sql_query))
        # Excecute the query:
        self.cus.execute(sql_query)
        # Retrieve the result:
        result = self.cus.fetchall()
        return result

    def retrieve_dataframe(self, sql_query):
        """
        This function runs the given sql_query and reads it to a pandas dataframe. It returns this pandas dataframe.

        :param sql_query: an SQL query string in the form "select * from DB where x = 5;"
        :return: a pandas dataframe containing the results of the query
        """
        self.logger.info('Loading data to a pandas dataframe using the SQL query: "{0}"'.format(sql_query))
        df = pd.read_sql(sql_query, self.con)
        return df


class Teradata(SQL):
    """
    This class provides an interaction layer between python and Teradata. It only supports reading from Teradata as
    I and in all likelyhood others have read-only rights to the database.

    .. note::
        Before using this python module, make sure that the ODBC connections have been set up properly on your system.
        For Windows 'ODBC Data Sources' should be installed:

        Source: https://downloads.teradata.com/download/connectivity/odbc-driver/windows

        Next, for each resource a datasource should be created.

    """
    # Optional library support modules (aka: only needed for this class):
    import pyodbc

    # Pandas visualization options:
    pd.set_option('display.max_columns', None)  # print all columns of dataframe
    pd.set_option('display.width', 1000)  # use more width of the screen when printing dataframes

    def __init__(self, host_name, uid, pwd, driver_name=None, loglevel='INFO'):
        self.logger = Logger('DataAccess.Teradata', loglevel).logger

        if driver_name is None:
            # Find the Teradata driver installed
            driver_list = self.pyodbc.drivers()
            teradata_drivers = [i for i in driver_list if 'Teradata' in i]
            if len(teradata_drivers) != 0:
                driver_name = teradata_drivers[0]  # simply take the first drivername
            else:
                self.logger.error('No Teradata driver found. Install the ODBC driver for Windows first.')
                self.logger.error('https://downloads.teradata.com/download/connectivity/odbc-driver/windows')
                exit(1)
        link = 'DRIVER={0};DBCNAME={1};UID={2};PWD={3}'.format(driver_name, host_name, uid, pwd)
        # Create a connection:
        self.con = self.pyodbc.connect(link)
        # Define Cursor:
        self.cus = self.con.cursor()
        self.logger.info('Successfully created a connection to {0} in Teradata for user {1}.'.format(host_name, uid))


class Postgres(SQL):
    """
    This class provides an interaction layer between python and PostgreSQL.
    Compared to the Teradata connection we need one additional parameter: database
    This parameter tells to which database it should connect.

    Initiate the class in this way:

    pg = Postgres(host_name='localhost', uid='postgres', pwd='password', database='climate', loglevel='INFO')

    """
    # Optional library support modules (aka: only needed for this class):
    import pyodbc

    # Pandas visualization options:
    pd.set_option('display.max_columns', None)  # print all columns of dataframe
    pd.set_option('display.width', 1000)  # use more width of the screen when printing dataframes

    def __init__(self, host_name, uid, pwd, database, driver_name=None, loglevel='INFO'):
        self.logger = Logger('DataAccess.Postgres', loglevel).logger

        if driver_name is None:
            # Find the Teradata driver installed
            driver_list = self.pyodbc.drivers()
            drivers = [i for i in driver_list if 'PostgreSQL ODBC Driver(UNICODE)' in i]
            if len(drivers) != 0:
                driver_name = drivers[0]  # simply take the first drivername
            else:
                self.logger.error('PostgreSQL ODBC Driver(UNICODE) driver not found. Install the ODBC driver for '
                                  'Windows first.')
                exit(1)
        self.driver_name = driver_name
        self.host_name = host_name
        self.uid = uid
        self.pwd = pwd
        self.database = database
        # Connect:
        self.connect(database)

    def connect(self, database='postgres'):
        """
        This function creates a connection to the database. It connects to a specific database if given and to postgres
        otherwise (useful if we want to drop a database for example).

        :param database: database to connect to (default=postgres).
        """
        link = 'DRIVER={0};DBCNAME={1};UID={2};PWD={3};DATABASE={4}'.format(self.driver_name, self.host_name, self.uid,
                                                                            self.pwd, database)
        # Create a connection:
        self.con = self.pyodbc.connect(link, autocommit=True)
        self.engine = create_engine('postgresql+psycopg2://{0}:{1}@{2}:5432/{3}'.format(self.uid, self.pwd,
                                                                                        self.host_name, database))
        # Define Cursor:
        self.cus = self.con.cursor()
        self.logger.info('Successfully created a connection to {0}:{1} in Postgres for user {2}.'.format(self.host_name,
                                                                                                         database,
                                                                                                         self.uid))

    def dataframe_to_db(self, df, tablename, schema_name='public', mode='append', postgis=False):
        """
        This function sends a pandas dataframe to a database table. It uses the append mode by default, the other
        options are 'fail' and 'replace'.
        The index is ignored.

        :param df: the pandas dataframe to send the the database
        :param tablename: the name of the table where the data needs to be saved.
        :param schema_name: the schema to use (default='public')
        :param mode: the mode to send the data to the database ('append'/'replace'/'fail', default='append')
        :param postgis: whether to send a geopandas dataframe (with geometry) or not (default=False).
        """
        if not postgis:
            df.to_sql(tablename, self.engine, schema=schema_name, if_exists=mode, index=False)
        else:
            df.to_postgis(tablename, self.engine, schema=schema_name, if_exists=mode, index=False)
        self.logger.info('Dataframe was send to the database. Schema={}, table={}'.format(schema_name, tablename))

    def create_database(self, db_name):
        """
        This function drops a database if it exists. Then, it creates the database.

        :param db_name: the name to give the database.
        """
        # First, re-connect but then to the postgres database instead, so that we can drop and can re-create the db.
        self.connect()
        # Drop the database and re-create it:
        sql = '''DROP DATABASE IF EXISTS {};'''.format(db_name)
        self.cus.execute(sql)
        sql = '''CREATE DATABASE {};'''.format(db_name)
        self.cus.execute(sql)
        # Next, re-connect to the newly created database:
        self.connect(db_name)


class SharePoint:
    """
    This class provides the ability to retrieve SharePoint list data, given that an MS FLow endpoint has been created
    and the created URL makes use of a client_id and client_secret to authenticate. It should also use min_id and max_id
    to retrieve a range of data from the list.

    For information on the proxy settings, check the Logging -> Email documentation.

    """
    # Optional library support modules (aka: only needed for this class):
    import requests

    def __init__(self, url, client_id, client_secret, username, password, loglevel='INFO'):
        self.logger = Logger('DataAccess.Sharepoint', loglevel).logger
        self.url = url
        self.client_id = client_id
        self.client_secret = client_secret
        # Set the proxy settings to use:
        proxy_string = 'http://{}:{}@10.11.24.70:8080'.format(username, password)
        self.proxies = {'https': proxy_string}

    def retrieve_sp_list(self, current_min=0):
        """
        This function retrieves all data from a SharePoint list and returns it as a list of dictionaries.
        It retrieves the data in blocks of 100 entries, starting from ID=0 and ending when no new entries are found.

        It will check starting from ID=0, however, many lists do not contain the first IDs anymore. Therefore, the
        first so many calls might not retrieve any data. For example, if the first ID is 512, then the first 5 calls
        won't return any results. This function will continue until the number of total results is at least 1 and once
        results have been found, it will stop when no new results are retrieved in a call.
        A safeguard has been built in to prevent an endless loop, if the first 25 calls give no results, it will quite.

        WARNING!: If there is a gap in the list data of more than 100 entries, not all data will be retrieved!
        For example, if you have IDs 200-1000 and 1200-2500, only IDs 200-1000 will be retrieved.

        :param current_min: can be used to start from a different id than 0.
        :return: the SharePoint list data as a list of dictionaries
        """
        entries = []
        number_results = 0
        retrieve = True
        failures = 0
        min_id = current_min
        while retrieve:
            current_max = current_min + 101  # set max to 101 entries higher so that it retrieves 100 entries
            entries = entries + self.retrieve_sp_list_data(current_min, current_max)  # add the entries to the save list
            if number_results != 0 and number_results == len(entries):
                # If no new results are retrieved anymore, break the while loop.
                retrieve = False
            # Here a bit of code to prevent an endless loop in case of an empty list:
            if len(entries) == 0:
                # Set the min_id to the current_max - 1 so make sure it knows where to start next time.
                min_id = current_max - 1
                failures += 1
            if failures == 25:
                retrieve = False
            # Get the number of results:
            number_results = len(entries)
            self.logger.info('Processed ID-range {}:{}, total entries retrieved so far: {}'.format(current_min,
                                                                                                   current_max,
                                                                                                   number_results))
            current_min = current_max - 1  # update current minimum, the -1 prevents skipping one
        return entries, min_id

    def retrieve_sp_list_data(self, min_id, max_id):
        """
        This function retrieves SharePoint list data for a specific range, from min_id to max_id.
        It depends on the correct setup of an MS FLow endpoint to the requested list. Depending on the setup, it can
        retrieve a maximum number of entries per run. Default = 100.

        :param min_id: the lowest ID to retrieve (integer)
        :param max_id: the highest ID to retrieve (integer)
        :return: SharePoint list data for specified range as a list of dictionaries
        """
        # Replace the values for client_id, client_secret, min_id and max_id in the URL:
        url = self.url.replace('{clientID}', self.client_id)
        url = url.replace('{clientSecret}', self.client_secret)
        url = url.replace('{min_id}', str(min_id))
        url = url.replace('{max_id}', str(max_id))
        try:
            # This needs to run when at location (in the company)
            resp = self.requests.get(url, proxies=self.proxies)
        except Exception:
            # This needs to run when outside the company on the VPN
            resp = self.requests.get(url)
        results = []
        if resp.status_code == 200:  # get status code, 200 is successfull
            # The returned results are given as a list of dictionaries but need to be evaluated as such
            res = resp.text.replace('null', '""')  # This line replaces null values with an empty string, else error
            results = eval(res)
        else:
            self.logger.error('Get failed, code = {}'.format(resp.status_code))
        return results

    def retrieve_sp_excelfile(self, sheets):
        """
        This function retrieves a sharepoint excel file from sharepoint. It stores it temporarily as excel on the disk
        in the current working directory, reads the file again with pandas, removes the temporary excel file again, then
        it returns the pandas dataframe.

        :param sheets: a list of tuples with (sheetname, skiprows) where skiprows is the number of rows to skip
         before reading the header.
        :return: a list of pandas dataframes with the data from the sharepoint excel, 1 df per sheet in the excel.
        """
        # Set the filepath for the temp save location:
        temp_file_loc = os.getcwd() + '/temp.xlsx'
        # Retrieve the file:
        status_code, change_dt = self.retrieve_file(temp_file_loc)
        # Check response code:
        if status_code == 200:  # successfull
            # Read the excel file to a pandas df (one for each sheet):
            df_list = []
            for sheet, skip in sheets:
                df = pd.read_excel(temp_file_loc, sheet_name=sheet, skiprows=skip)
                # Drop empty rows and columns (only if a row or column only contains NA):
                df.dropna(how='all', axis=1, inplace=True)
                df.dropna(how='all', axis=0, inplace=True)
                df_list.append(df)
            # Remove the temporary file again:
            try:
                os.remove(temp_file_loc)
            except Exception as ex:
                self.logger.error('Error removing temp file again: {}'.format(ex))
        else:
            self.logger.error('Get failed, code = {}'.format(status_code))
            df_list, change_dt = None, None  # Set return_val to None
        return df_list, change_dt

    def retrieve_file(self, filepath, filename=None):
        """
        This function retrieves a file from SharePoint as defined in the Flow. The 'filepath' points to a location
        where to save the retrieved file. The content of the downloaded file needs to match the extension used
        in the filepath, otherwise the file will not be readable (so xlsx for excel and zip for a zipfile etc.).

        The filename is optional. It can be ignored if the MS Flow has been created specifically for a single file.

        :param filepath: full path to save the file including extension (C:/Users/Documents/example.xlsx)
        :param filename: filename of the file to retrieve from SharePoint (zipfile1.zip)
        """
        # Replace the values for client_id and client_secret in the URL:
        url = self.url.replace('{clientID}', self.client_id)
        url = url.replace('{clientSecret}', self.client_secret)
        if filename is not None:
            url = url.replace('{fileName}', filename)
        # Retrieve the excelfile from sharepoint:
        try:
            resp = self.requests.get(url, proxies=self.proxies)
        except Exception:
            resp = self.requests.get(url)
        # Check response code:
        if resp.status_code == 200:  # successfull
            # Get the modification date of the file and then convert to local time:
            change_utc = datetime.strptime(resp.headers['moddate'], '%Y-%m-%dT%H:%M:%SZ')
            offset = datetime.fromtimestamp(time.time()) - datetime.utcfromtimestamp(time.time())
            file_change = (change_utc + offset).strftime('%d/%m/%Y %H:%M')
            # Write the data to an temporary excel:
            with open(filepath, "wb") as f:
                f.write(resp.content)
            f.close()
        else:
            self.logger.error('Get failed, code = {}'.format(resp.status_code))
            file_change = None
        return resp.status_code, file_change

    def retrieve_sp_updatetime(self, filename):
        """
        This function retrieves the update date/time for a SharePoint file.
        It depends on the correct setup of an MS FLow endpoint to the requested sharepoint site.

        :param filename: the filename on the sharepoint (example.xlsx)
        :return: Update date/time for the input file as a datetime object.
        """
        # Replace the values for client_id, client_secret in the URL:
        url = self.url.replace('{clientID}', self.client_id)
        url = url.replace('{clientSecret}', self.client_secret)
        url = url.replace('{fileName}', filename)
        try:
            resp = self.requests.get(url, proxies=self.proxies)
        except Exception:
            resp = self.requests.get(url)
        # Check response code:
        if resp.status_code == 200:  # successfull
            # Get the modification date of the file and then convert to local time:
            change_utc = datetime.strptime(resp.headers['moddate'], '%Y-%m-%dT%H:%M:%SZ')
            offset = datetime.fromtimestamp(time.time()) - datetime.utcfromtimestamp(time.time())
            file_change = (change_utc + offset)
        else:
            self.logger.error('Get failed, code = {}'.format(resp.status_code))
            file_change = None
        return resp.status_code, file_change


class SendData:
    """
    This class provides the ability to send data using MS Flow.
    It takes the MS Flow url, client_id, client_secret as input, which are settings created when the workflow was
    created at emea.flow.microsoft.com

    It also needs the CWID username and password (used for windows) in order to also function properly when working
    from within the company network.

    """
    # Optional library support modules (aka: only needed for this class):
    import requests

    def __init__(self, url, client_id, client_secret, username, password, loglevel='INFO'):
        self.logger = Logger('DataAccess.Sharepoint', loglevel).logger
        self.url = url
        self.client_id = client_id
        self.client_secret = client_secret
        # Set the proxy settings to use:
        proxy_string = 'http://{}:{}@10.11.24.70:8080'.format(username, password)
        self.proxies = {'https': proxy_string}
        # Initiate the WriteData class:
        self.wd = WriteData(loglevel=loglevel)

    def file_to_msflow(self, filepath, filename):
        """
        This function reads the file at the 'filepath' and sends it to MS Flow using a POST request.
        It then processes the request as defined in the MS Flow (send by email, save to SharePoint etc.).

        It uses 'filename' as the filename, therefore, the filename in the filepath doesn't need to be the same
        as the one in 'filename'.

        :param filepath: the full path to the file to be read / send.
        :param filename: how the attached file should be called including extension (Datafile.xlsx).
        """
        # First read the file:
        data = open(filepath, 'rb').read()
        if self.client_id is None:
            self.logger.error('Cannot connect to sharepoint because client_id is empty.')
        if self.client_secret is None:
            self.logger.error('Cannot connect to sharepoint because client_secret is empty.')
        if filename is None:
            self.logger.error('Cannot connect to sharepoint because filename is empty.')
        # Replace the values for client_id and client_secret in the URL:
        url = self.url.replace('{clientID}', self.client_id)
        url = url.replace('{clientSecret}', self.client_secret)
        # Replace the 'fileContent' with the base64 encoded excel file:
        url = url.replace('{fileName}', filename)
        try:
            resp = self.requests.post(url, data, proxies=self.proxies)
        except Exception:
            resp = self.requests.post(url, data)
        if resp.status_code == 200:  # get status code, 200 is successfull
            self.logger.info('File to SharePoint request accepted, file {} has been send.'.format(filename))
        else:
            self.logger.error('Sending file to SharePoint failed, code = {}'.format(resp.status_code))

    def df_to_msflow(self, df, filename, zipit=False):
        """
        This function first saves a df to a tempfile locally and then sends it to msflow including the filename.
        It can process 'csv', 'xlsx' or 'pk1' files.

        :param df: the dataframe to send to MS Flow
        :param filename: the filename it should have in MS Flow (output.xlsx)
        :param zipit: True/False, if True it will zip the file and then send it to MS Flow.
        """
        # Determine to which filetype it should be saved (xlsx, csv or pk1) based on the extension.
        extension = filename.split('.')[-1]
        basefilename = filename.split('.')[0]
        if extension in ['xlsx', 'csv', 'pk1']:
            time0 = time.time()
            self.wd.save_df(df, os.getcwd(), basefilename, filetype=extension)
            self.logger.info('Time taken to save df temporary to disk = {} seconds.'.format(time.time() - time0))
            tempfile_loc = os.getcwd() + '/' + basefilename + '.' + extension
            if not zipit:
                self.file_to_msflow(tempfile_loc, filename)
            else:
                self.zip_to_msflow(tempfile_loc)
            os.remove(tempfile_loc)
        else:
            self.logger.error('Filetype {} is not supported, exiting..'.format(extension))
            exit()

    def zip_to_msflow(self, filepath):
        """
        This function packs the file at the input 'filepath' into a zip file and then sends it to MS Flow using
        file_to_msflow().

        The filename for the zipfile will be the same as the filename of the input 'filepath'.

        :param filepath: the full path to the file to zip and send to MS flow.
        """
        # First get the basepath and filename from the full path:
        basepath = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        # Get the basefilename (without extension):
        basefilename = filename.split('.')[0]
        # Zip the file to reduce filesize to send:
        zip_filename = basefilename + '.zip'
        zipfilepath = '{}/{}'.format(basepath, zip_filename)
        with zipfile.ZipFile(zipfilepath, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            zf.write(filepath, os.path.basename(filepath))
        # Send to SharePoint:
        self.file_to_msflow(zipfilepath, zip_filename)
        # Remove local zipfile again:
        os.remove(zipfilepath)
