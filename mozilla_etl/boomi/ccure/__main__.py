import bonobo
import bonobo_sqlalchemy
import requests
import os
import fs

from bonobo.config import use
from bonobo.constants import NOT_MODIFIED
from requests.auth import HTTPBasicAuth

from sqlalchemy import create_engine

from dateutil import parser as dateparser

def timestamp(admitted, blank1, timestamp, blank2, name, card_id, location):
    parsed_date = dateparser.parse(timestamp)
    
    yield (admitted, blank1, parsed_date, blank2, name, card_id, location)
 
import re
def card_id(admitted, blank1, timestamp, blank2, name, card_id, location):
    find = re.search('\(Card: (\d+)\)', admitted)
    
    if find:
        card_id = find.group(1)
        yield (admitted, blank1, timestamp, blank2, name, card_id, location)

def map_fields(admitted, blank1, timestamp, blank2, name, card_id, location):
    yield {
        'activitydate':timestamp,
        'badgeid':card_id,
        'username':name,
        'location':location,
    }

def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()
    graph.add_chain(
        # Admitted 'Mu, Daosheng' (Card: 50066)   at 'TPE 4th FL Fire Stair West' (IN).||9/1/2016 12:00:05 AM||Mu, Daosheng||TPE 4th FL Fire Stair West

        bonobo.CsvReader('uploads/BadgeID/Daily Journal Export.txt',
                         delimiter='|',
                         fields=('Admitted','blank1','Timestamp','blank2','Name','card_id','Location'),
                         fs='brickftp'
                         ),
        timestamp,
        card_id,
        map_fields,
        bonobo.UnpackItems(0),
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name='ccure_activity',
            discriminant=('activitydate', 'badgeid','username', 'location',),
            engine='mysql'),

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

    return {
        'mysql':
        create_engine('mysql+mysqldb://localhost/aws', echo=False),
        'brickftp':
        fs.open_fs("ssh://mozilla.brickftp.com/etl/ccure"),
        'vertica':
        create_engine(
            options['vertica'].format(
                host=options['vertica_host'],
                username=options['vertica_username'],
                password=options['vertica_password']),
            echo=False)
    }


# The __main__ block actually execute the graph.
if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, required=False),
    parser.add_argument('--vertica-host', type=str, default='vsql.dataviz.allizom.org')

    parser.add_argument(
        '--vertica',
        type=str,
        required=False,
        default=
        "vertica+vertica_python://{username}:{password}@{host}:5433/metrics"
    )

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
