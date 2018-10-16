__all__ = ["add_default_arguments", "add_default_services"]

import os
import fs

from sqlalchemy import create_engine
from fs.sshfs import SSHFS


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

    services['sftp'] = SSHFS(
        options['sftp_host'],
        user=options['sftp_username'],
        timeout=10,
        keepalive=10,
        compress=True)
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
