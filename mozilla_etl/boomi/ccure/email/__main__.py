import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import use, use_context, use_raw_input, use_context_processor

from bonobo.constants import NOT_MODIFIED

from dateutil.relativedelta import relativedelta

id_cache = {}


def _cache(self, context):
    yield id_cache


@use_context_processor(_cache)
def cache(badge_id_cache, badge_id, empty1, last_name, empty2, first_name,
          *args):

    if last_name.lower() not in badge_id_cache:
        badge_id_cache[last_name.lower()] = dict()

    if first_name.lower() not in badge_id_cache[last_name.lower()]:
        badge_id_cache[last_name.lower()][first_name.lower()] = set()

    badge_id_cache[last_name.lower()][first_name.lower()].add(badge_id)

    return NOT_MODIFIED


def badge_active(badge_id, empty1, last_name, empty2, first_name, empty3,
                 issued_on, empty4, disabled, *args):

    if disabled == 'True':
        return

    return NOT_MODIFIED


def get_cache_graph(**options):
    """

    This graphs builds a cache of badges from ccure
    
    :return: bonobo.Graph

    """

    graph = bonobo.Graph()

    graph.add_chain(
        bonobo.CsvReader(
            '/etl/ccure/uploads/BadgeID/ccure_BadgeID_AllButVendor.txt',
            fields=('badge_id', 'empty1', 'last_name', 'empty2', 'first_name',
                    'empty3', 'issued_on', 'empty4', 'disabled', 'empty5',
                    'valid_until', 'empty6', 'flag2', 'empty7', 'flag3',
                    'empty8', 'flag4'),
            delimiter='|',
            fs='brickftp'),
        badge_active,
        cache,
    )

    return graph


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """

    graph = bonobo.Graph()

    split_dbs = bonobo.noop

    graph.add_chain(
        bonobo.CsvReader(
            '/etl/metrics-insights/workday-users.csv', fs='brickftp'),
        employee_active, find_badge_id, bonobo.UnpackItems(0), split_dbs)

    for engine in list(set(options['engine'])):
        graph.add_chain(
            bonobo_sqlalchemy.InsertOrUpdate(
                table_name=options['table_name'],
                discriminant=('badgeid', ),
                buffer_size=10,
                engine=engine),
            _input=split_dbs)

    return graph


def employee_active(employee_id=None,
                    Last_Name=None,
                    First_Name=None,
                    Preffered_Last_Name=None,
                    Preferred_First_Name=None,
                    Hire_Date=None,
                    Email=None,
                    Cost_Center_Name=None,
                    Cost_Center_Number=None,
                    Cost_Center_Hierarchy=None,
                    Employee_Type=None,
                    Employee_Status=None,
                    Business_Title=None,
                    Work_Location=None,
                    Manager=None,
                    Supervisory_Organization=None,
                    manager_level_02=None,
                    Manager_s_Manager_Supervisory_Organization=None,
                    manager_level_03=None,
                    manager_level_04=None,
                    manager_level_05=None,
                    termination_date=None):
    """Filter out employees that are NOT active"""

    if Employee_Status == 'Active':
        return NOT_MODIFIED


@use_context_processor(_cache)
def find_badge_id(badge_id_cache,
                  employee_id=None,
                  Last_Name=None,
                  First_Name=None,
                  Preffered_Last_Name=None,
                  Preferred_First_Name=None,
                  Hire_Date=None,
                  Email=None,
                  Cost_Center_Name=None,
                  Cost_Center_Number=None,
                  Cost_Center_Hierarchy=None,
                  Employee_Type=None,
                  Employee_Status=None,
                  Business_Title=None,
                  Work_Location=None,
                  Manager=None,
                  Supervisory_Organization=None,
                  manager_level_02=None,
                  Manager_s_Manager_Supervisory_Organization=None,
                  manager_level_03=None,
                  manager_level_04=None,
                  manager_level_05=None,
                  termination_date=None):

    plname = badge_id_cache.get(Preffered_Last_Name.lower(), {})
    lname = badge_id_cache.get(Last_Name.lower(), {})

    lname.update(plname)

    pfname = lname.get(Preferred_First_Name.lower(), set())
    fname = lname.get(First_Name.lower(), set())

    fname.update(pfname)

    for badge_id in fname:
        yield {
            'employee_id': employee_id,
            'first_name': First_Name.title(),
            'last_name': Last_Name.title(),
            'badgeid': badge_id,
            'email': Email,
        }


import json


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

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
        default=os.getenv('BOOMI_TABLE', 'f_employee_etl'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        g1 = get_cache_graph(**options)
        print("# Running card_id cache")
        bonobo.run(g1, services=services)

        g2 = get_graph(**options)
        print("# Runing employee mapping")
        bonobo.run(g2, services=services)
