__all__ = [
    "add_default_arguments", "add_default_services", "HeaderlessCsvWriter"
]

import os
import fs
import bonobo

from sqlalchemy import create_engine
from fs.sshfs import SSHFS

from dateutil import parser as dateparser
import datetime

import requests
from requests.auth import HTTPBasicAuth

from bonobo.config import use_context


@use_context
class HeaderlessCsvWriter(bonobo.CsvWriter):
    def write(self, file, context, *values, fs):
        context.setdefault('lineno', 0)
        fields = context.get_input_fields()

        if not context.lineno:
            context.writer = self.writer_factory(file)

            if fields:
                context.lineno += 1

        return super(HeaderlessCsvWriter, self).write(
            file, context, *values, fs=fs)

    __call__ = write


def valid_date(s):
    try:
        return dateparser.parse(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def add_default_services(services, options):
    services['mysql'] = create_engine(
        'mysql+mysqldb://localhost/aws', echo=False)

    services['redshift'] = create_engine(
        options['redshift'].format(
            host=options['redshift_host'],
            port=options['redshift_port'],
            name=options['redshift_name'],
            username=options['redshift_username'],
            password=options['redshift_password']),
        echo=False)

    services['vertica'] = create_engine(
        options['vertica'].format(
            host=options['vertica_host'],
            port=options['vertica_port'],
            name=options['vertica_name'],
            username=options['vertica_username'],
            password=options['vertica_password']),
        echo=False)

    if options['local']:
        services['sftp'] = fs.open_fs("file:///tmp/etl")
        services['centerstone'] = fs.open_fs("file:///tmp/etl")
    else:
        services['sftp'] = fs.open_fs(
            "ssh://%s@%s" % (options['sftp_username'], options['sftp_host']))
        # Bug workaround to sftp-only server
        services['sftp']._platform = "Linux"
        services['centerstone'] = fs.open_fs(
            "ssh://MozillaBrickFTP@ftp.asset-fm.com:/Out/"),

    if options['use_cache']:
        from requests_cache import CachedSession
        services['servicenow'] = CachedSession('http.cache')
        services['workday'] = CachedSession('http.cache')
    else:
        services['servicenow'] = requests.Session()
        services['workday'] = requests.Session()

    services['servicenow'].headers = {'User-Agent': 'Mozilla/ETL/v1'}
    services['servicenow'].auth = HTTPBasicAuth(options['sn_username'],
                                                options['sn_password'])
    services['servicenow'].headers.update({'Accept-encoding': 'text/json'})

    services['workday'].headers = {'User-Agent': 'Mozilla/ETL/v1'}
    services['workday'].auth = HTTPBasicAuth(options['wd_username'],
                                             options['wd_password'])
    services['workday'].headers.update({'Accept-encoding': 'text/json'})

    # Set a file suffix for non-prod jobs
    if options['environment'] == "prod":
        options['suffix'] = ""
    else:
        options['suffix'] = '.' + options['environment']

    return


def add_default_arguments(parser):
    parser.add_argument(
        '--vertica-username',
        type=str,
        default=os.getenv('VERTICA_USERNAME', 'tableau'))
    parser.add_argument(
        '--vertica-password',
        type=str,
        default=os.getenv('VERTICA_PASSWORD', 'tableau'))
    parser.add_argument(
        '--vertica-name',
        type=str,
        default=os.getenv('VERTICA_NAME', 'metrics'))
    parser.add_argument(
        '--vertica-port', type=str, default=os.getenv('VERTICA_PORT', '5433'))
    parser.add_argument(
        '--vertica-host',
        type=str,
        default=os.getenv('VERTICA_HOST', 'vsql.dataviz.allizom.org'))
    parser.add_argument(
        '--redshift-username',
        type=str,
        default=os.getenv('REDSHIFT_USERNAME', 'etl_edw'))
    parser.add_argument(
        '--redshift-password',
        type=str,
        default=os.getenv('REDSHIFT_PASSWORD', 'tableau'))
    parser.add_argument(
        '--redshift-name',
        type=str,
        default=os.getenv('REDSHIFT_NAME', 'edw-dev-v1'))
    parser.add_argument(
        '--redshift-port',
        type=str,
        default=os.getenv('REDSHIFT_PORT', '5439'))
    parser.add_argument(
        '--redshift-host',
        type=str,
        default=os.getenv(
            'REDSHIFT_HOST',
            'mozit-dw-dev.czbv3z9khmhv.us-west-2.redshift.amazonaws.com'))

    parser.add_argument(
        '--sftp-host',
        type=str,
        default=os.getenv('SFTP_HOST', 'mozilla.brickftp.com'))

    parser.add_argument(
        '--sftp-username',
        type=str,
        default=os.getenv('SFTP_USERNAME', 'moz-etl'))

    parser.add_argument(
        '--wd-username', type=str, default=os.getenv('WD_USERNAME', 'ISU-WPR'))

    parser.add_argument(
        '--wd-password', type=str, default=os.getenv('WD_PASSWORD'))

    parser.add_argument(
        '--wd-tenant',
        type=str,
        default=os.getenv('WD_TENANT', 'vhr_mozilla_preview'))

    parser.add_argument(
        '--engine',
        action='store',
        nargs='*',
        default=os.getenv('BOOMI_ENGINE', 'mysql').split(','))

    parser.add_argument(
        '--vertica',
        type=str,
        required=False,
        default=
        "vertica+vertica_python://{username}:{password}@{host}:{port}/{name}")

    parser.add_argument(
        '--redshift',
        type=str,
        required=False,
        default="postgresql://{username}:{password}@{host}:{port}/{name}")

    parser.add_argument(
        '--environment', type=str, required=False, default="stage")

    parser.add_argument(
        '--now',
        required=False,
        default=datetime.datetime.now(),
        type=valid_date)

    parser.add_argument(
        '--dry-run', action='store_true', default=os.getenv('DRY_RUN', False))

    parser.add_argument(
        '--local', action='store_true', default=os.getenv('LOCAL', False))

    parser.add_argument('--use-cache', action='store_true', default=False)
    parser.add_argument('--sn-username', type=str, default='mozvending'),
    parser.add_argument(
        '--sn-password', type=str, default=os.getenv("SN_PASSWORD")),
