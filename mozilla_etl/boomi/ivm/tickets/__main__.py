import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import use, use_context, use_raw_input

from bonobo.constants import NOT_MODIFIED

import urllib.request
import http.client

http.client.HTTPConnection.debuglevel = 1

SN_TEST_URL = 'https://mozilla.service-now.com/u_mozilla_vending_webservice.do?JSONv2&sysparm_action=insertMultiple'


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    STMT = """
select a.badgeid AS badgeid, b.user_id AS user_id, a.employee_id AS employee_id, a.email AS email, 
b.item_description AS item_description, b.item_number AS item_number , b.transaction_date AS transaction_date,
b.transaction_id AS transaction_id, b.description AS description, '' AS drawer_id, b.quantity AS quantity
from  ivm b , (select badgeid,email, employee_id from f_employee group by badgeid,email ,employee_id) a
where  b.user_id = a.badgeid
and b.transaction_date = (select current_date -2);
"""

    graph.add_chain(
        bonobo_sqlalchemy.Select(STMT, engine='redshift'),
        trim_employ_id,
        invalid_badge_id,
        invalid_email,
        format_payload,
        create_ticket,
        bonobo.UnpackItems(0),
        bonobo.PrettyPrinter(),
    )

    return graph


import requests
from requests.auth import HTTPBasicAuth
import json


@use('servicenow')
def create_ticket(row, servicenow):
    resp = servicenow.post(SN_TEST_URL, data=json.dumps(row))

    yield from resp.json().get('records')


def trim_employ_id(badgeid, user_id, employee_id, email, item_description,
                   item_number, transaction_date, transaction_id, description,
                   drawer_id, quantity):
    yield (badgeid, user_id, employee_id.strip(), email, item_description,
           item_number, transaction_date, transaction_id, description,
           drawer_id, quantity)


def invalid_badge_id(badgeid, user_id, employee_id, email, item_description,
                     item_number, transaction_date, transaction_id,
                     description, drawer_id, quantity):
    if badgeid != 0:
        return NOT_MODIFIED


def invalid_email(badgeid, user_id, employee_id, email, item_description,
                  item_number, transaction_date, transaction_id, description,
                  drawer_id, quantity):
    if email != '':
        return NOT_MODIFIED


def format_payload(badgeid, user_id, employee_id, email, item_description,
                   item_number, transaction_date, transaction_id, description,
                   drawer_id, quantity):
    yield {
        "u_badgenumber": badgeid,
        "u_employeeid": employee_id,
        "u_ldapaccount": email,
        "u_transactionid": transaction_id,
        "u_datetimevended": str(transaction_date),
        "u_product": item_description,
        "u_vendingmachineid": description,
        "u_itemnumber": item_number,
    }


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
        'servicenow': servicenow,
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    if not __package__:
        from os import sys, path
        top = path.dirname(
            path.dirname(path.dirname(path.dirname(path.abspath(__file__)))))
        sys.path.append(top)
        __package__ = "boomi.ivm.tickets"

    from ... import add_default_arguments, add_default_services

    parser = bonobo.get_argument_parser()

    add_default_arguments(parser)
    parser.add_argument('--use-cache', action='store_true', default=False)
    parser.add_argument('--sn-username', type=str, default='mozvending'),
    parser.add_argument('--sn-password', type=str, required=False),

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, **options)
        bonobo.run(get_graph(**options), services=services)
