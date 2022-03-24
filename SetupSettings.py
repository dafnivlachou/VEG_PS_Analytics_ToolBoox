import argparse


class CreateSettings:
    """
    This class enables the creation of a default settings object using create_parser().
    This can then be extended using the other functions.

    Example implementation:

    .. code-block:: python

      from SetupSettings import CreateSettings


      class Settings:

          def __init__(self):
              # setup a settings object:
              cs = CreateSettings()
              self.parser = cs.create_parser('Settings for SmartHomeFarm')
              self.parser = cs.add_teradata_settings(self.parser)
              self.parser = cs.add_psql_settings(self.parser)
              # Add project specific settings:
              self.parser.add_argument('-t', '--target', help='target to run (prepare/?/?)')
              self.parser.add_argument('--subtarget', help='Subtagert to run (weather/production)')

          def parse_arguments(self):
              settings = self.parser.parse_args()
              return settings

    """

    def __init__(self):
        pass

    @staticmethod
    def create_parser(descr='Default settings object'):
        """
        This function creates the basic parser with arguments that are always added.

        Arguments added are:
         - -l / --log_level (default=INFO)
         - -w / --work_dir

        """
        parser = argparse.ArgumentParser(description=descr)
        parser.add_argument('-l', '--log_level', help='Set log level (DEBUG/INFO/ERROR)', default='INFO')
        parser.add_argument('-w', '--work_dir', help='Full path to workdir (saving temp files)')
        return parser

    @staticmethod
    def add_teradata_settings(parser):
        """
        This function extends an existing parser with options to connect to teradata.

        Arguments added are:
         - -m / --host_name (default=BIWP01)
         - -u / --user
         - -p / --password

        """
        parser.add_argument('-m', '--host_name', help='Name of Teradata host (default=BIWP01)',
                            default='BIWP01')
        parser.add_argument('-u', '--user', help='Teradata username (CWID)')
        parser.add_argument('-p', '--password', help='Teradata password')
        return parser

    @staticmethod
    def add_psql_settings(parser):
        """
        This function extends an existing parser with options to connect to a postgres database.

        Arguments added are:
         - --psql_host (default=localhost)
         - --psql_user (default=postgres)
         - --psql_password
         - --psql_database (default=postgres)

        """
        parser.add_argument('--psql_host', help='Hostname to which to connect to the postgres database '
                                                '(default=localhost)', default='localhost')
        parser.add_argument('--psql_user', help='Username used for the postgres database (default=postgres)',
                            default='postgres')
        parser.add_argument('--psql_password', help='Password used for the postgres database')
        parser.add_argument('--psql_database', help='Database name to create a connection to (default=postgres).',
                            default='postgres')
        return parser
