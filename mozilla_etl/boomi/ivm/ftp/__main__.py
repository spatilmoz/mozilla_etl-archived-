import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import Service, use, use_no_input, use_context
from bonobo.config.functools import transformation_factory
from bonobo.constants import NOT_MODIFIED

from dateutil import parser as dateparser

import re

from lxml import etree

import untangle

import pprint
import fs

MAX_DESCRIPTION_LENGTH = 8

@transformation_factory
def GetOrderXML(glob=[], prefix="/etl/ivm"):

    @use_context
    @use_no_input
    @use('sftp')
    def _GetOrderXML(context, sftp):

        for file in sftp.filterdir(prefix, files=glob):
            if file.is_file:
                with sftp.open(os.path.join(prefix, file.name)) as fp:
                    file = untangle.parse(fp)
        
                    for transaction in file.NewDataSet.transaction:
                        emit = {}
                        for element in transaction.get_elements():
                            emit[element._name.title()] = element.cdata
                    
                        yield emit
    return _GetOrderXML

@transformation_factory        
def ParseDates(fields):
    fields = list(fields)
    
    def _ParseDates(row):
        modified = False
        for key in fields:
            if key in row:
                date = dateparser.parse(row[key])
                if date:
                    row[key] = date.date()
                    modified = True
        
        if modified:
            yield row
        else:
            yield NOT_MODIFIED
        
    return _ParseDates

def truncate_description(row):
    if len(row['Vendingmachines_Descr']) > MAX_DESCRIPTION_LENGTH:
        row['Vendingmachines_Descr'] = row['Vendingmachines_Descr'][:MAX_DESCRIPTION_LENGTH]
        return row
    else:
        return NOT_MODIFIED

def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    split_dbs = bonobo.noop

    graph.add_chain(
        GetOrderXML(prefix="/etl/ivm", glob=['Mozilla_Corporation{timestamp:%Y_%m_%d}*.xml'.format(timestamp=options['now'])]),
        ParseDates(['Transactionlog_Tranenddatetime']),
        truncate_description,
        bonobo.UnpackItems(0),
        bonobo.Rename(
            transaction_date='Transactionlog_Tranenddatetime',
            item_number='Transactionlog_Itemnumber',
            transaction_id='Transactionlog_Tlid',
            item_description='Transactionlog_Itemdesc'
        ),
        bonobo.Rename(
            user_id='Transactionlog_User',
            quantity='Transactionlog_Qty',
            transaction_code='Transactionlog_Transcode',
            description='Vendingmachines_Descr',
        ),
        split_dbs,
        
        _name="main")

#insert into ivm (description, transaction_id, item_number, item_description, user_id, quantity, transaction_date, transaction_code) values
     

    for engine in list(set(options['engine'])):
        graph.add_chain(
            bonobo_sqlalchemy.InsertOrUpdate(
                table_name=options['table_name'],
                discriminant=(
                    'transaction_id',
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
        '--table-name',
        type=str,
        default=os.getenv('BOOMI_TABLE', 'ivm_etl'))

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, **options)
        bonobo.run(get_graph(**options), services=services)
