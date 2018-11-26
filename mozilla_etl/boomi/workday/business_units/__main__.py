import bonobo
import requests

import io
import csv
import fs

from bonobo.config import Configurable, Service, ContextProcessor, use, use_context, use_context_processor
from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from requests.auth import HTTPBasicAuth

WORKDAY_BASE_URL = 'https://wd2-impl-services1.workday.com'
COST_CENTERS_QUERY = '/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Business_Units_Feed?Organizations%21WID=4f414049b78141f3981464563b36ba46!7f8db47cd30d4cdfa5670e37ee0df3ad&Include_Subordinate_Organizations=1&format=csv&bom=true'
BU_QUERY = '/ccx/service/customreport2/vhr_mozilla/ISU_RAAS/intg__Supervisory_Orgs_Feed?format=csv&bom=true'

_cache = {}


def cache(self, context):
    yield _cache


@use('workday')
def get_cost_centers(workday):
    """Retrieve cost centers from WorkDay"""
    resp = workday.get(WORKDAY_BASE_URL + COST_CENTERS_QUERY)

    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)

    for row in data:
        yield dict(zip(headers, row))


@use('workday')
def get_business_units(workday):
    """Retrieve Business Units from WorkDay"""
    resp = workday.get(WORKDAY_BASE_URL + BU_QUERY)

    stream = io.StringIO(resp.content.decode("utf-8-sig"))

    data = csv.reader(stream)

    headers = next(data)

    for row in data:
        yield dict(zip(headers, row))


import collections


def centerstone_CostCenter_remap(row):
    dict = collections.OrderedDict()
    dict['Product_Line'] = row['Cost_Center_Hierarchy']
    dict['Cost_Center'] = row['Cost_Center']
    dict['Name'] = row['Cost_Center']
    dict['Manager_Name'] = row['Manager']
    dict['Type (Cost Center)'] = "Cost Center"
    dict['Cost_Center_Number'] = row['Cost_Center_ID']

    yield dict


@use_context_processor(cache)
def join_cost_centers(cache, row):
    if row['Cost_Center'] in cache:
        row['Cost_Center_Details'] = cache[row['Cost_Center']]
        yield row
    else:
        # print("Encountered record without a known cost center %r" % row)
        raise ValueError("Encountered record without a known cost center", row)


def centerstone_BU_SupOrg_Merge_remap(row):
    dict = collections.OrderedDict()
    dict['Team'] = row['Organization']
    dict['Team_Manager'] = row['manager']
    dict['Cost_Center'] = row['Cost_Center_Details']['Cost_Center']
    dict['Cost_Center_ID'] = row['Cost_Center_Details']['Cost_Center_ID']
    dict['Coster_Center_Manager'] = row['Cost_Center_Details']['Manager']
    dict['Product_Line'] = row['Cost_Center_Details']['Cost_Center_Hierarchy']
    dict['Product_Line_Manager'] = row['Cost_Center_Details']['CCH_Manager']
    dict['HRBP'] = row['Cost_Center_Details']['HRBP']

    yield dict


def centerstone_BussUnit_remap(row):
    dict = collections.OrderedDict()
    dict['Cost_Center'] = row['Cost_Center']
    dict['Cost_Center_Number'] = row['Cost_Center_ID']
    dict['Cost_Center_Manager'] = row['Coster_Center_Manager']
    dict['Product_Line'] = row['Product_Line']
    dict['Product_Line_Manager'] = row['Product_Line_Manager']
    dict['Team'] = row['Team']
    dict['Team_Manager'] = row['Team_Manager']

    yield dict


def productLineLevel1_remap(row):
    dict = collections.OrderedDict()
    dict['Product_Line'] = row['Product_Line']
    dict['Name'] = row['Product_Line']
    dict['Manager_Name'] = row['Product_Line_Manager']
    dict['Type'] = 'Product Line'

    yield dict


_product_line_cache = {}


def product_line_cache(self, context):
    yield _product_line_cache


@use_context_processor(product_line_cache)
def unique_product_line(cache, row):
    product_line = row['Product_Line']

    if product_line not in cache:
        cache[product_line] = True
        yield NOT_MODIFIED


def teamLevel3_remap(row):
    dict = collections.OrderedDict()
    dict['Cost_Center'] = row['Cost_Center']
    dict['Product_Line'] = row['Product_Line']
    dict['Team'] = row['Team']
    dict['Name'] = row['Team']
    dict['Manager_Name'] = row['Team_Manager']
    dict['Type'] = 'Team'

    yield dict


def get_bu_graph(**options):
    graph = bonobo.Graph()
    graph.add_chain(
        get_business_units,
        join_cost_centers,
        centerstone_BU_SupOrg_Merge_remap,
        centerstone_BussUnit_remap,
    )

    graph.add_chain(
        #bonobo.Limit(3),
        #bonobo.PrettyPrinter(),
        productLineLevel1_remap,
        unique_product_line,
        bonobo.UnpackItems(0),
        bonobo.PrettyPrinter(),
        bonobo.CsvWriter(
            '/etl/centerstone/downloads/ProductLineLevel1.txt.bonobo',
            lineterminator="\n",
            delimiter="\t",
            fs="sftp"),
        _input=centerstone_BussUnit_remap)
    graph.add_chain(
        teamLevel3_remap,
        bonobo.UnpackItems(0),
        bonobo.CsvWriter(
            '/etl/centerstone/downloads/TeamLevel3.txt.bonobo',
            lineterminator="\n",
            delimiter="\t",
            fs="sftp"),
        _input=centerstone_BussUnit_remap)

    return graph


@use_context_processor(cache)
def cache_cost_centers(cache, row):
    cache[row['Cost_Center']] = row
    return NOT_MODIFIED


def get_costcenter_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        get_cost_centers,
        cache_cost_centers,
        centerstone_CostCenter_remap,
        #bonobo.PrettyPrinter(),
        bonobo.UnpackItems(0),
        # Can't skip the header, but must
        bonobo.CsvWriter(
            '/etl/centerstone/downloads/CostCenterLevel2.txt.bonobo',
            lineterminator="\n",
            delimiter="\t",
            fs="sftp"),
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
        'workday': workday,
    }


# The __main__ block actually execute the graph.
import os
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

    parser.add_argument('--wd-username', type=str, default='ISU-WPR')
    parser.add_argument(
        '--wd-password', type=str, default=os.getenv('WD_PASSWORD'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, **options)

        costcenter_g = get_costcenter_graph(**options)
        bu_g = get_bu_graph(**options)

        # Run CostCenter process
        print("# Running CostCenter process")
        bonobo.run(costcenter_g, services=services)

        # Run Business Unit process
        print("# Running Business Unit process")
        bonobo.run(bu_g, services=services)
