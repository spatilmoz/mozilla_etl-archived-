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
WD_DESK_ID_QUERY = 'ccx/service/customreport2/vhr_mozilla_preview/ISU_RAAS/WPR_Worker_Space_Number?format=csv'

_cache = {
    'desk_ids': {},
    'employee_type': {},
}


def cache(self, context):
    yield _cache


import re


@use_context_processor(cache)
def cache_desk_id(cache, row):
    desk_id_cache = cache['desk_ids']

    #desk_number =  row['WPR_Desk_Number']
    #find = re.search('(\D+)?(\d+)', desk_number)
    #if find:
    #    desk_number = find.group(2)

    if row['Employee_ID'] not in desk_id_cache:
        desk_id_cache[row['Employee_ID']] = row['WPR_Desk_Number']
    else:
        yield {
            "error":
            "Employee %s has more than one record!" % row['Employee_ID']
        }

    return NOT_MODIFIED


@use_context_processor(cache)
def cache_employee_type(cache, row):
    employee_type_cache = cache['employee_type']

    employee_type_cache[row['Employee_ID']] = row['Worker_Type']

    return NOT_MODIFIED


@use('workday')
def get_wd_desk_ids(workday):
    """Retrieve Business Units from WorkDay"""
    resp = workday.get(WORKDAY_BASE_URL + WD_DESK_ID_QUERY)

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

    graph.add_chain(
        get_wd_desk_ids,
        cache_desk_id,
        cache_employee_type,
        _name="main")

    return graph


def split_tabs(row):
    yield dict(
        zip([
            'EmployeeID', 'OfficeLocation', 'WorkLocation_CS',
            'WorkLocation_WD', 'SeatID'
        ], row.split("\t")))


def mismatch(row):
    if row['wd_desk_id'] != row['SeatID']:
        return NOT_MODIFIED


def join_desk_ids(row):
    row['wd_desk_id'] = _cache['desk_ids'].get(row['EmployeeID'], None)

    yield row


# Employees not known to WorkDay default to regular 'Employee'
def join_employee_type(row):
    row['wd_employee_type'] = _cache['employee_type'].get(
        row['EmployeeID'], 'Employee')

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
    return row['wd_employee_type'] == "Employee"


def temp_employee(self, row):
    return row['wd_employee_type'] == "Contingent Worker"


def odd_employee(*args):
    return not (temp_employee(*args) or regular_employee(*args))


#factory = wsdl_client.type_factory('bsvc')


@use('workday_soap')
def update_employee_record(row, workday_soap):
    factory = workday_soap.type_factory('bsvc')

    employee_type = None

    if row['wd_employee_type'] == "Contingent Worker":
        employee_type = "Contingent_Worker_ID"
    else:
        employee_type = "Employee_ID"

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
        ID=None,
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
        resp = {
            'Status': 'Failed',
            'Exception': str(e),  
        }

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
        join_desk_ids,
        join_employee_type,
        mismatch,
        split_employees,
        _name="main")

    # Process regular employees
    graph.add_chain(
        update_employee_record,
        bonobo.PrettyPrinter(),
        _input=split_employees)

    # Dump out outlier employees
    graph.add_chain(
        bonobo.Filter(filter=odd_employee),
        bonobo.UnpackItems(0),
        bonobo.PrettyPrinter(),
        _input=split_employees)

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
        workday = CachedSession('http.cache')
    else:
        workday = requests.Session()

    workday.headers = {'User-Agent': 'Mozilla/ETL/v1'}
    workday.auth = HTTPBasicAuth(options['wd_username'],
                                 options['wd_password'])
    workday.headers.update({'Accept-encoding': 'text/json'})

    wsdl_client = Client(
        WORKDAY_BASE_URL + 'ccx/service/vhr_mozilla_preview/Human_Resources/' +
        WORKDAY_API_VERSION + '?wsdl',
        wsse=UsernameToken(
            "%s@vhr_mozilla_preview" % (options['wd_username']),
            options['wd_password'],
            use_digest=False),
    )

    wsdl_client.set_ns_prefix('bsvc', 'urn:com.workday/bsvc')

    return {
        'workday': workday,
        'workday_soap': wsdl_client,
        'centerstone':
        fs.open_fs("ssh://MozillaBrickFTP@ftp.asset-fm.com:/Out/"),
        #'centerstone': fs.open_fs('.'),
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
        '--wd-username', type=str, default='ServiceBus_IntSysUser')
    parser.add_argument(
        '--wd-password', type=str, default=os.getenv('WD_PASSWORD'))

    parser.add_argument(
        '--table-name', type=str, default=os.getenv('BOOMI_TABLE', 'ivm_etl'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, **options)

        print("# Running Workday deskid cache")
        bonobo.run(get_wd_graph(**options), services=services)

        print("# Running consolidation")
        bonobo.run(get_cs_graph(**options), services=services)
