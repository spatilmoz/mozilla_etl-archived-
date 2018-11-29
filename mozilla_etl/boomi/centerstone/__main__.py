import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import Service, use, use_no_input, use_context, use_context_processor
from bonobo.config.functools import transformation_factory
from bonobo.constants import NOT_MODIFIED

from dateutil import parser as dateparser

import re
import io
import csv

import requests
from requests.auth import HTTPBasicAuth

from lxml import etree

import untangle

import pprint
import fs
import datetime

from zeep import Client
from zeep.wsse.username import UsernameToken
import zeep.exceptions

OFFICE_IDS = {
    'San Francisco': 'SF',
    'Mountain View': 'MV',
    'Portland': 'PDX',
    'London': 'LON',
    'Vancouver': 'YVR',
    'Toronto': 'TOR',
    'Paris': 'PAR',
    'Berlin': 'BER',
    'Auckland': 'AKL',
    'Bejing': 'BJ',
    'Taipei': 'TPE',
}

WORKDAY_BASE_URL = 'https://wd2-impl-services1.workday.com/'
WORKDAY_API_VERSION = 'v26.0'
WD_DESK_ID_QUERY = 'ccx/service/customreport2/{tenant}/ISU_RAAS/WPR_Worker_Space_Number?format=csv'

_cache = {
    'wpr': {},
}


def cache(self, context):
    yield _cache


import re


@use_context_processor(cache)
def cache_wpr(cache, row):
    wpr_cache = cache['wpr']

    if row['Employee_ID'] not in wpr_cache:
        wpr_cache[row['Employee_ID']] = row
    else:
        yield {
            "error":
            "Employee %s has more than one record!" % row['Employee_ID']
        }

    return NOT_MODIFIED


@use('workday')
@use('workday_url')
@use('workday_tenant')
def get_wd_desk_ids(workday, workday_url, workday_tenant):
    """Retrieve Business Units from WorkDay"""
    resp = workday.get(workday_url +
                       WD_DESK_ID_QUERY.format(tenant=workday_tenant))

    print('%r' % resp)

    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)

    for row in data:
        yield dict(zip(headers, row))


def get_wd_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    split_dbs = bonobo.noop

    graph.add_chain(get_wd_desk_ids, cache_wpr, _name="main")

    return graph


def split_tabs(row):
    yield dict(
        zip([
            'EmployeeID', 'OfficeLocation', 'WorkLocation_CS',
            'WorkLocation_WD', 'SeatID'
        ], row.split("\t")))


def mismatch(row):
    if row['wpr']['WPR_Desk_Number'] != row['SeatID']:
        return NOT_MODIFIED


def join_wpr(row):
    if row['EmployeeID'] in _cache['wpr']:
        row['wpr'] = _cache['wpr'].get(row['EmployeeID'])
        yield row


def prefix_desk_ids(row):
    desk_id = row['SeatID']
    if desk_id[:1].isdigit():
        if row['WorkLocation_CS'] in OFFICE_IDS:
            row['SeatID'] = "{office_prefix}{seat_id}".format(
                office_prefix=OFFICE_IDS[row['WorkLocation_CS']],
                seat_id=desk_id)
        return row
    else:
        return NOT_MODIFIED


def regular_employee(self, row):
    return row['wpr']['Worker_Type'] == "Employee"


def temp_employee(self, row):
    return row['wpr']['Worker_Type'] == "Contingent Worker"


def odd_employee(*args):
    return not (temp_employee(*args) or regular_employee(*args))


#factory = wsdl_client.type_factory('bsvc')


@use('workday_soap')
def update_employee_record(row, workday_soap):
    factory = workday_soap.type_factory('bsvc')

    employee_type = None

    if row['wpr']['Worker_Type'] == "Contingent Worker":
        employee_type = "Contingent_Worker_ID"
    else:
        employee_type = "Employee_ID"

    print("XXX: [%s] Updating seat %s to %s for %s %s" %
          (row['EmployeeID'], row['wpr']['WPR_Desk_Number'], row['SeatID'],
           row['wpr']['First_Name'], row['wpr']['Last_Name']))

    bus_param = factory.Business_Process_ParametersType(
        Auto_Complete=True, Run_Now=True)

    custom_id_type_primary = factory.Custom_ID_TypeObjectIDType(
        "CUSTOM_ID_TYPE-3-24",
        factory.Custom_ID_TypeReferenceEnumeration("Custom_ID_Type_ID"))

    type_ref_primary = factory.Custom_ID_TypeObjectType(
        ID=[custom_id_type_primary])

    custom_id_data_primary = factory.Custom_ID_DataType(
        ID=row['SeatID'],
        ID_Type_Reference=type_ref_primary,
        Issued_Date=datetime.datetime.now())

    custom_id_primary = factory.Custom_IDType(
        Custom_ID_Data=custom_id_data_primary, Delete=False)

    custom_id_type_secondary = factory.Custom_ID_TypeObjectIDType(
        "CUSTOM_ID_TYPE-3-25",
        factory.Custom_ID_TypeReferenceEnumeration("Custom_ID_Type_ID"))
    type_ref_secondary = factory.Custom_ID_TypeObjectType(
        ID=[custom_id_type_secondary])

    custom_id_data_secondary = factory.Custom_ID_DataType(
        ID="",
        ID_Type_Reference=type_ref_secondary,
        Issued_Date=datetime.datetime.now())

    custom_id_secondary = factory.Custom_IDType(
        Custom_ID_Data=custom_id_data_secondary, Delete=False)

    custom = factory.Custom_Identification_DataType([custom_id_primary], True)

    emp_type = factory.WorkerObjectIDType(
        row['EmployeeID'], factory.WorkerReferenceEnumeration(employee_type))

    worker = factory.WorkerObjectType(emp_type)

    change_id = factory.Change_Other_IDs_Business_Process_DataType(
        Worker_Reference=worker, Custom_Identification_Data=custom)

    resp = None

    try:
        resp = workday_soap.service.Change_Other_IDs(
            version=WORKDAY_API_VERSION,
            Business_Process_Parameters=bus_param,
            Change_Other_IDs_Data=change_id,
        )

    # Known bug with empty SOAP Body in responses
    except IndexError as e:
        resp = {
            'Status': 'Nothing to do',
        }
    except zeep.exceptions.Fault as e:
        raise e

    if resp:
        yield resp


def get_cs_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    split_employees = bonobo.noop

    graph.add_chain(
        bonobo.FileReader(
            path='HrExport.txt',
            fs='centerstone',
            encoding='latin-1',
            eol="\r\n"),
        split_tabs,
        prefix_desk_ids,
        join_wpr,
        mismatch,
        split_employees,
        _name="main")

    # Process regular employees
    if options['dry_run']:
        update = ()
    else:
        update = (update_employee_record, )

    graph.add_chain(
        bonobo.PrettyPrinter(),
        *update,
        bonobo.PrettyPrinter(),
        _input=split_employees)

    # Dump out outlier employees
    #graph.add_chain(
    #    bonobo.Filter(filter=odd_employee),
    #    bonobo.UnpackItems(0),
    #    bonobo.PrettyPrinter(),
    #    _input=split_employees)
    #
    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

    wsdl_client = Client(
        options['wd_base_url'] +
        'ccx/service/{tenant}/Human_Resources/'.format(
            tenant=options['wd_tenant']) + WORKDAY_API_VERSION + '?wsdl',
        wsse=UsernameToken(
            "%s@%s" % (options['wd_username'], options['wd_tenant']),
            options['wd_password'],
            use_digest=False),
    )

    wsdl_client.set_ns_prefix('bsvc', 'urn:com.workday/bsvc')

    return {
        'workday_url': options['wd_base_url'],
        'workday_tenant': options['wd_tenant'],
        'workday_soap': wsdl_client,
    }


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

    from .. import add_default_arguments, add_default_services

    parser = bonobo.get_argument_parser()

    add_default_arguments(parser)

    parser.add_argument(
        '--wd-base-url',
        type=str,
        default=os.getenv('WD_BASE_URL', WORKDAY_BASE_URL))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        print("# Running Workday deskid cache")
        bonobo.run(get_wd_graph(**options), services=services)

        print("# Running consolidation")
        bonobo.run(get_cs_graph(**options), services=services)
