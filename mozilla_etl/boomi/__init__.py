__all__ = ["add_default_arguments", "add_default_services"]

import os
import fs

from sqlalchemy import create_engine
from fs.sshfs import SSHFS

from dateutil import parser as dateparser
import datetime

import requests
from requests.auth import HTTPBasicAuth


def valid_date(s):
    try:
        return dateparser.parse(s)
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def add_default_services(services, **options):
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

    services['sftp'] = fs.open_fs(
        "ssh://%s@%s" % (options['sftp_username'], options['sftp_host']))

    # Bug workaround to sftp-only server
    services['sftp']._platform = "Linux"

    if options['use_cache']:
        from requests_cache import CachedSession
        services['servicenow'] = CachedSession('http.cache')
    else:
        services['servicenow'] = requests.Session()

    services['servicenow'].headers = {'User-Agent': 'Mozilla/ETL/v1'}
    services['servicenow'].auth = HTTPBasicAuth(options['sn_username'],
                                                options['sn_password'])
    services['servicenow'].headers.update({'Accept-encoding': 'text/json'})

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

    #action='store' with nargs='*'.

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
        '--now',
        required=False,
        default=datetime.datetime.now(),
        type=valid_date)

    parser.add_argument('--use-cache', action='store_true', default=False)
    parser.add_argument('--sn-username', type=str, default='mozvending'),
    parser.add_argument(
        '--sn-password', type=str, default=os.getenv("SN_PASSWORD")),
