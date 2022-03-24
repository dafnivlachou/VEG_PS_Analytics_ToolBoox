#!/usr/bin/env python
""" Logging.py: A class to standardize logging. """

__author__ = "Chris Jacobs"
__version__ = "0.1"
__email__ = "chris.jacobs@bayer.com"

import logging


class Logger:
    """
    This classed is used to standardize logging. It takes the name of the logging object and the level.
    Levels can be given as string or integer, both lower and uppercase strings are accepted.
    The filename parameter is optional, when given, logs will be written to file instead of printing them to screen.

    Levels:

    - 'NOTSET' / 0
    - 'DEBUG' / 10
    - 'INFO' / 20
    - 'WARNING' / 30
    - 'ERROR' / 40
    - 'CRITICAL' / 50

    To create a logger call:
     `logger = Logger('logging_name', 'INFO', filename='c:/mylocation/logfile.txt').logger`

    """

    loglevel_dict = {
        'NOTSET': logging.NOTSET,
        0: logging.NOTSET,
        'DEBUG': logging.DEBUG,
        10: logging.DEBUG,
        'INFO': logging.INFO,
        20: logging.INFO,
        'WARNING': logging.WARNING,
        30: logging.WARNING,
        'ERROR': logging.ERROR,
        40: logging.ERROR,
        'CRITICAL': logging.CRITICAL,
        50: logging.CRITICAL
    }

    def __init__(self, name_logger, logging_level, filename=None, filemode='a'):
        if filename is not None:
            # log to both file and std. out
            handlers = [logging.FileHandler(filename, mode=filemode), logging.StreamHandler()]
            logging.basicConfig(format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                                datefmt='%d-%b-%y %H:%M:%S',
                                handlers=handlers)
        else:
            logging.basicConfig(format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
                                datefmt='%d-%b-%y %H:%M:%S')
        self.logger = logging.getLogger(name_logger)
        # Check whether the logging_level is a string or integer, if string, make sure it is uppercase.
        if isinstance(logging_level, str):
            logging_level = logging_level.upper()

        self.logger.setLevel(self.loglevel_dict[logging_level])


class Email:
    """
    Class created to send emails / push notifications automatically from Python scripts.
    This class doesn't contain logging because it is called before the main script normally. If this class were
    to contain a logging object, it will interfere with the main script logging. Might be able to fix this in
    the future.

    This class contains proxy settings. These need to be set in order to be able to run from within the company network.
    The reason is that the requests package is not able to deal with https requests when running over a proxy and by
    providing the proxy settings, it runs it through a tunnel preventing the proxy from trying to verify the
    certificates. The information was found here:

    https://lukasa.co.uk/2013/07/Python_Requests_And_Proxies/
    https://stackoverflow.com/questions/34025964/python-requests-api-using-proxy-for-https-request-get-407-proxy-authentication-r

    And the proxy IP was found on Slack. Note that it needs the user ID and password to verify the identity.

    **NOTE: It seems that it doesn't work with a secondary CWID, so use the primary one.**

    """
    # Optional library support modules (aka: only needed for this class):
    import requests

    def __init__(self, url, client_id, client_secret, username, password):
        self.url = url
        self.client_id = client_id
        self.client_secret = client_secret
        # Set the proxy settings to use:
        proxy_string = 'http://{}:{}@10.11.24.70:8080'.format(username, password)
        self.proxies = {'https': proxy_string}

    def send_error_email(self, process):
        """
        This function sends an email using a Microsoft Flow using a get request.
        It also sends a push notification to the MS Flow app.
        The MS Flow can be created on 'https://emea.flow.microsoft.com'.

        :param process: A string that contains an error message to send in the email.
        """
        # Replace the values for client_id, client_secret and process:
        url = self.url.replace('{clientID}', self.client_id)
        url = url.replace('{clientSecret}', self.client_secret)
        url = url.replace('{process}', process)
        # Run the request:
        try:
            # This needs to run when at location (in the company)
            resp = self.requests.get(url, proxies=self.proxies)
        except Exception:
            # This needs to run when outside the company on the VPN
            resp = self.requests.get(url)
