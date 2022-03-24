#!/usr/bin/env python
"""
log_db_performance.py:
A simple script that runs the same SQL query and writes the duration of the data retrieval to a
logfile. This can be used to check at what times the database is fast and when it is slow.
It can be scheduled using windows task scheduler.

 """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

# Imports:
import argparse
import time
import datetime
import os

# Own modules:
from DataAccess import Teradata, ReadData
from Visualization import Plotting

# Retrieve settings:
parser = argparse.ArgumentParser(description='Settings for log_db_performance')
parser.add_argument('-w', '--work_dir', help='Directory to save temp/output files (Full path)')
parser.add_argument('-l', '--log_level', help='Set log level (DEBUG/INFO/ERROR)')
parser.add_argument('-m', '--host_name', help='Name of Teradata host (BIWP01 for Production for example)')
parser.add_argument('-u', '--user', help='Teradata username (CWID)')
parser.add_argument('-p', '--password', help='Teradata password')
settings = parser.parse_args()

# Initiate the modules:
td = Teradata(settings.host_name, settings.user, settings.password, loglevel=settings.log_level)
rd = ReadData(loglevel=settings.log_level)
pl = Plotting(loglevel=settings.log_level)

# Create a simply query:
sql = '''SELECT TOP 10000 * FROM VEG.VEG_MTRL'''

# Create the work_dir if it doesn't exist:
try:
    os.makedirs(settings.work_dir)
except FileExistsError:
    # Directory already exists
    pass

# Set the logfile and plotfile location:
logfile_location = settings.work_dir + '/sql_duration_log.txt'
plotfile_location = settings.work_dir + '/sql_duration_plot.png'

# Determine the value of the counter by reading the logfile if it exists, otherwise the index = 0.
try:
    df_index = rd.read_csv_to_df(logfile_location)
    index = df_index['Index'].max() + 1  # set the index to the maximum value + 1
except FileNotFoundError:
    # file doesn't exist yet, create and set index to 0
    lf = open(logfile_location, 'a')
    lf.write('Index,Date/Time,Duration_in_seconds\n')  # write header
    lf.close()
    index = 0


def time_query(sql_in, index_in, logfile):
    """
    This function runs the input sql query and times how long it took. It writes the current date/time and the duration
    of the query execution to the logfile.

    :param sql_in: SQL query to run
    :param index_in: the index of the line to write
    :param logfile: the logfile to write the duration to (will be appended)
    """
    time0 = time.time()  # start time
    df = td.retrieve_dataframe(sql_in)
    runtime = time.time() - time0  # duration in seconds
    now = datetime.datetime.now().strftime("%d-%m-%Y %H:%M")
    f = open(logfile, 'a')
    f.write('{},{},{}\n'.format(index_in, now, runtime))
    f.close()


def plot_durations(logfile, plot_file):
    """
    This function plots the logfile as a scatterplot to visualize the duration through time.

    :param logfile: location of the logfile to plot
    :param plot_file: save location for the plot
    """
    df_in = rd.read_csv_to_df(logfile)
    fig = pl.scatter_plot('Date/Time', 'Duration_in_seconds', df_in, 'Date/time', 'Duration query execution in seconds',
                          x_rotation=45, labelsize=5)
    fig.savefig(plot_file, dpi=300)


# Run the functions:
time_query(sql, index, logfile_location)
plot_durations(logfile_location, plotfile_location)
