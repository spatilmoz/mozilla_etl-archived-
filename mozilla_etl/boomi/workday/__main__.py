import bonobo
import bonobo_sqlalchemy
import requests

import io
import csv
import fs

from bonobo.config import Configurable, Service, ContextProcessor, use, use_context
from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from requests.auth import HTTPBasicAuth

from sqlalchemy import create_engine

WORKDAY_BASE_URL = 'https://services1.myworkday.com'
COST_CENTERS_QUERY = '/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Business_Units_Feed?Organizations%21WID=4f414049b78141f3981464563b36ba46!7f8db47cd30d4cdfa5670e37ee0df3ad&Include_Subordinate_Organizations=1&format=csv&bom=true'
BU_QUERY = '/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Supervisory_Orgs_Feed?format=csv&bom=true'

@use('workday')
def get_cost_centers(workday):
    """Retrieve cost centers from WorkDay"""
    resp = workday.get(WORKDAY_BASE_URL  + COST_CENTERS_QUERY)
    
    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)
        
    for row in data:
        yield dict(zip(headers, row))

@use('workday')
def get_business_units(workday):
    """Retrieve Business Units from WorkDay"""
    resp = workday.get(WORKDAY_BASE_URL  + BU_QUERY)
    
    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)
        
    for row in data:
        yield dict(zip(headers, row))

import collections
def centerstone_remap(row):
    dict = collections.OrderedDict()
    dict['Product_Line'] = row['Cost_Center_Hierarchy']
    dict['Cost_Center'] = row['Cost_Center']
    dict['Name'] = row['Cost_Center']
    dict['Manager_Name'] = row['Manager']
    dict['Type (Cost Center)'] = "Cost Center"
    dict['Cost_Center_Number'] = row['Cost_Center_ID']
   
    yield dict
    
def get_bu_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_business_units,
        bonobo.Limit(2),
        bonobo.PrettyPrinter(),
        bonobo.count,
    )
    
    return graph

def get_costcenter_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        get_cost_centers,
        centerstone_remap,
        bonobo.PrettyPrinter(),
        bonobo.UnpackItems(0),
        # Can't skip the header, but must
        bonobo.CsvWriter('CostCenterLevel2.txt.new', delimiter="\t", fs="brickftp"),
        bonobo.count,
        _name="main")

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

    return {
        'workday':
        workday,
        'brickftp': fs.open_fs("ssh://mozilla.brickftp.com/etl/centerstone/downloads/"),
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
    parser.add_argument('--wd-username', type=str, default='ServiceBus_IntSysUser'),
    parser.add_argument('--wd-password', type=str, default='MozIT2018!', required=False),

    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, required=False),

    parser.add_argument(
        '--vertica',
        type=str,
        required=False,
        default=
        "vertica+vertica_python://{username}:{password}@vsql.dataviz.allizom.org:5433/metrics"
    )

    with bonobo.parse_args(parser) as options:
        services   = get_services(**options)
        costcenter_g = get_costcenter_graph(**options)
        bu_g = get_bu_graph(**options)
        
        # Run CostCenter process
        bonobo.run(costcenter_g, services=services)
        
        # Run Business Unit process
        bonobo.run(bu_g, services=services)
        
