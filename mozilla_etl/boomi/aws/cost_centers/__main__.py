import bonobo
import bonobo_sqlalchemy
import requests
import os

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
        'linked_account_number': int(account['u_account_number'].strip() or 0),
        'requester': account['u_requester'],
        'cost_center': account['cost_center'],
    }


# We don't want accounts with no valid:
# - u_account_number
# - cost_center
#
def valid_aws_account(account):
    if not account['linked_account_number']:
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

    split_dbs = bonobo.noop

    graph.add_chain(
        extract_accounts,
        transform,
        valid_aws_account,
        bonobo.UnpackItems(0),
        split_dbs,
        _name="main")

    for engine in list(set(options['engine'])):
        graph.add_chain(
            bonobo_sqlalchemy.InsertOrUpdate(
                table_name=options['table_name'] + options['table_suffix'],
                discriminant=('linked_account_number', ),
                engine=engine),
            _input=split_dbs)

    return graph


def get_services(**options):
    return {}


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    if not __package__:
        from os import sys, path
        top = path.dirname(
            path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
        sys.path.append(top)

        me = []
        me.append(path.split(path.dirname(path.abspath(__file__)))[1])
        me.insert(
            0,
            path.split(path.dirname(path.dirname(path.abspath(__file__))))[1])
        me.insert(
            0,
            path.split(
                path.dirname(
                    path.dirname(path.dirname(path.abspath(__file__)))))[1])

        __package__ = '.'.join(me)

    from ... import add_default_arguments, add_default_services

    parser = bonobo.get_argument_parser()

    add_default_arguments(parser)

    parser.add_argument(
        '--table-name',
        type=str,
        default=os.getenv('BOOMI_TABLE', 'dim_aws_accounts'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)
        bonobo.run(get_graph(**options), services=services)
