import bonobo
import bonobo_sqlalchemy
import os

from dateutil import parser as dateparser

import re


def timestamp(admitted, blank1, timestamp, blank2, name, card_id, location):
    parsed_date = dateparser.parse(timestamp)

    yield (admitted, blank1, parsed_date, blank2, name, card_id, location)


def card_id(admitted, blank1, timestamp, blank2, name, card_id, location):
    find = re.search('\(Card: (\d+)\)', admitted)

    if find:
        card_id = find.group(1)
        return (admitted, blank1, timestamp, blank2, name, card_id, location)


def map_fields(admitted, blank1, timestamp, blank2, name, card_id, location):
    yield {
        'activitydate': timestamp,
        'badgeid': card_id,
        'username': name,
        'location': location,
    }


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    split_dbs = bonobo.noop

    graph.add_chain(
        bonobo.CsvReader(
            options['input_file'],
            delimiter='|',
            fields=('Admitted', 'blank1', 'Timestamp', 'blank2', 'Name',
                    'card_id', 'Location'),
            fs='brickftp'),
        timestamp,
        card_id,
        map_fields,
        bonobo.UnpackItems(0),
        split_dbs,
        _name="main")

    for engine in list(set(options['engine'])):
        graph.add_chain(
            bonobo_sqlalchemy.InsertOrUpdate(
                table_name=options['table_name'] + options['table_suffix'],
                discriminant=(
                    'activitydate',
                    'badgeid',
                    'username',
                    'location',
                ),
                engine=engine),
            _input=split_dbs)

    return graph


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
        '--input-file',
        type=str,
        default=os.getenv(
            'BOOMI_INPUT_FILE',
            'etl/ccure/uploads/BadgeID/Daily Journal Export.txt'))
    parser.add_argument(
        '--table-name',
        type=str,
        default=os.getenv('BOOMI_TABLE', 'ccure_activity_etl'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)
        bonobo.run(get_graph(**options), services=services)
