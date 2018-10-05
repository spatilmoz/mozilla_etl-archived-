import bonobo
import bonobo_sqlalchemy
import requests

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from requests.auth import HTTPBasicAuth

from sqlalchemy import create_engine

SN_TEST_URL = 'https://mozilla.service-now.com/alm_license.do?JSONv2&sysparm_query=model%3Da669b9840ffa4200f67ab65be1050e49'


@use('servicenow')
def extract_accounts(servicenow):
    """Placeholder, change, rename, remove... """
    yield from servicenow.get(SN_TEST_URL).json().get('records')


# Simplify fields
def transform(account):
    """Bleh"""

    yield {
        'account_name': account['u_account_name'],
        'account_id': int(account['u_account_number'].strip() or 0),
        'requester': account['u_requester'],
        'cost_center': account['cost_center'],
    }


# We don't want accounts with no valid:
# - u_account_number
# - cost_center
#
def valid_aws_account(account):
    if not account['account_id']:
        return

    if not account['cost_center']:
        return

    return NOT_MODIFIED


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        extract_accounts, transform, valid_aws_account, _name="main")

    graph.add_chain(
        bonobo.JsonWriter('aws_accounts.json'),
        _input="main",
    )

    graph.add_chain(
        bonobo.UnpackItems(0),
        bonobo.CsvWriter('aws_accounts.csv'),
        _input=valid_aws_account,
    )

    graph.add_chain(
        bonobo.UnpackItems(0),
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name='aws_accounts',
            discriminant=('account_id', ),
            engine='db'),
        _input=valid_aws_account,
    )
    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

    if options['use_cache']:
        from requests_cache import CachedSession
        servicenow = CachedSession('http.cache')
    else:
        servicenow = requests.Session()

    servicenow.headers = {'User-Agent': 'Mozilla/ETL/v1'}
    servicenow.auth = HTTPBasicAuth(options['sn_username'],
                                    options['sn_password'])
    servicenow.headers.update({'Accept-encoding': 'text/json'})

    return {
        'servicenow':
        servicenow,
        'db':
        create_engine('sqlite:///test.sqlite', echo=False),
        'vertica':
        create_engine(
            options['vertica'].format(
                username=options['vertica_username'],
                password=options['vertica_password']),
            echo=False)
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    parser.add_argument('--use-cache', action='store_true', default=False)
    parser.add_argument('--sn-username', type=str, default='mozvending'),
    parser.add_argument('--sn-password', type=str, required=True),

    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, required=True),

    parser.add_argument(
        '--vertica',
        type=str,
        required=False,
        default=
        "vertica+vertica_python://{username}:{password}@vsql.dataviz.allizom.org:5433/metrics"
    )

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
