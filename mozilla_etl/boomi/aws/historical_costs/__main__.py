import argparse

import datetime
from dateutil import parser as dateparser
from dateutil.relativedelta import relativedelta

import bonobo_sqlalchemy

import boto3

import bonobo
from bonobo.config import use, use_context, use_raw_input, use_context_processor
from bonobo.constants import NOT_MODIFIED

from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker


# We should lowercase fields and all here
@use_context
class AwsBillingReader(bonobo.CsvReader):
    def reader_factory(self, file):
        reader = super(AwsBillingReader, self).reader_factory(file)
        # Discard useless header amazon message
        next(reader)
        return reader

    def __call__(self, file, context, *, fs):
        reader = self.reader_factory(file)

        if not context.output_type:
            # Initial row contains the column headers
            self.fields = next(reader)

            # Lowercase them all
            self.fields = list(map(lambda x: x.lower(), *self.fields))

            # Convert ':' to SQL safe '_'s
            self.fields = list(
                map(lambda x: x.replace(':', '_'), *self.fields))

            context.set_output_fields(*self.fields)

        return super(AwsBillingReader, self).__call__(file, context, fs=fs)


@use_raw_input
def filter_summary(bag):
    row = bag._asdict()

    if row['recordtype'] == 'LinkedLineItem':
        yield {
            'total_cost': row['totalcost'],
            'productname': row['productname'],
            'date': row['billingperiodenddate'],
            'linkedaccountid': row['linkedaccountid'],
        }


def _lookup_sk(self, context, database):
    """Context processor to perform database lookups"""
    yield {
        'ctx':
        context,
        'session':
        sessionmaker(bind=database)(),
        'aws_accounts':
        Table(
            "dim_aws_accounts",
            MetaData(),
            autoload=True,
            autoload_with=database),
        'date':
        Table("dim_date", MetaData(), autoload=True, autoload_with=database),
    }


@use('database')
@use_context_processor(_lookup_sk)
def lookup_account_sk(context, row, database):
    session = context['session']
    table = context['aws_accounts']

    instance = session.query(table).filter_by(
        linked_account_number=row['linkedaccountid']).one_or_none()
    if instance and instance.account_name_key:
        return {
            **row,
            'account_name_sk': instance.account_name_key,
            'account_name': instance.account_name,
        }
    else:
        print("XXX: Can't find account sk for %s" % row['account_name'])
        context['ctx'].error(
            "Couldn't find account sk for %s" % row['account_name'], level=0)


@use('database')
@use_context_processor(_lookup_sk)
def lookup_date_sk(context, row, database):
    session = context['session']
    table = context['date']

    instance = session.query(table).filter_by(date=row['date']).one_or_none()
    if instance and instance.date_key:
        return {
            **row,
            'date_sk': instance.date_key,
        }
    else:
        context['ctx'].error(
            "Couldn't find date sk for %s" % row['date'], level=0)


@use_context
@use_raw_input
def invalid_entries(context, row):

    mydict = row._asdict()

    if not context.output_type:
        context.set_output_fields(mydict.keys())

    if not row.get('linkedaccountid'):
        context.error("Skipping row without linked account id", level=0)
        return

    if not row.get('invoicedate'):
        context.error("Skipping row without invoicedate", level=0)
        return

    yield NOT_MODIFIED


@use_context
@use_raw_input
def fix_numbers(context, bag):

    row = bag._asdict()

    if not context.output_type:
        context.set_output_fields(row.keys())

    # Fix numbrers, yuck
    if bag.get('blendedrate') == "":
        bag = bag._replace(blendedrate=0)

    if bag.get('rateid') == "":
        bag = bag._replace(rateid=0)

    yield bag


@use_context
@use_raw_input
def parse_dates(context, bag):

    row = bag._asdict()

    keys = row.keys()

    if not context.output_type:
        context.set_output_fields(keys)

    for key in keys:
        if "date" in key and row[key] != "":
            parsed_date = dateparser.parse(row[key])
            if parsed_date:
                row[key] = parsed_date.date()
            else:
                context.error(
                    "Could not parse date %s for key %s" % (row[key], key),
                    level=0)
                return

    yield tuple(row.values())


def _summarize_costs(self, context):
    summary = {}
    yield summary

    for account, dates in summary.items():
        for date, products in dates.items():
            for product, cost in products.items():
                context.send({
                    'date_sk': date,
                    'productname': product,
                    'account_name_sk': account,
                    'total_cost': cost,
                })


@use_context_processor(_summarize_costs)
def summarize_costs(context, row):
    info = context

    account = row['account_name_sk']
    if not account in info:
        info[account] = {}

    info = info[account]

    date = row['date_sk']
    if not date in info:
        info[date] = {}

    info = info[date]

    product = row['productname']
    if not product in info:
        info[product] = 0

    info[product] += float(row['total_cost'])


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    graph.add_chain(
        bonobo.CsvWriter('billing.csv'),
        bonobo.JsonWriter('billing.json'),
        invalid_entries,
        fix_numbers,
        parse_dates,
        #bonobo.PrettyPrinter(),
        filter_summary,
        #bonobo.PrettyPrinter(),
        lookup_account_sk,
        lookup_date_sk,
        summarize_costs,
        bonobo.UnpackItems(0),
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name='fact_itsm_aws_historical_cost_bonobo',
            discriminant=(
                'productname',
                'date_sk',
                'account_name_sk',
            ),
            engine='database'),
        _name="main",
        _input=None,
    )

    now = options['now']

    # Go to beginning of month
    now += relativedelta(day=1, hour=0, minute=0, second=0, microsecond=0)

    when = now
    for log in range(0, options['months']):
        when = when + relativedelta(months=-1)
        tstamp = when.strftime("%Y-%m")
        print("# %d Processing %s" % (log, tstamp))
        if options['limit']:
            _limit = (bonobo.Limit(options['limit']), )
        else:
            _limit = ()

        graph.add_chain(
            AwsBillingReader(
                '%s-aws-cost-allocation-%s.csv' % (options['aws_account_id'],
                                                   tstamp),
                fs='s3',
                skip=1),
            *_limit,
            _output="main",
        )

    graph.add_chain(
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name=options['table'],
            discriminant=('invoiceid', 'linkedaccountid', 'payeraccountid',
                          'recordid'),
            engine='database'),
        _input=parse_dates,
    )

    return graph


def get_services(**options):
    """
    This function builds the services dictionary, which is a simple dict of names-to-implementation used by bonobo
    for runtime injection.

    It will be used on top of the defaults provided by bonobo (fs, http, ...). You can override those defaults, or just
    let the framework define them. You can also define your own services and naming is up to you.

    :return: dict
    """

    services = {
        'mysql':
        create_engine('mysql+mysqldb://localhost/aws', echo=False),
        's3':
        bonobo.open_fs('s3://mozilla-programmatic-billing'),
        'redshift':
        create_engine(
            'redshift+psycopg2://etl_edw@mozit-dw-dev.czbv3z9khmhv.us-west-2.redshift.amazonaws.com/edw-dev-v1',
            echo=False),
        'vertica':
        create_engine(
            options['vertica_dsn'].format(
                host=options['vertica_host'],
                username=options['vertica_username'],
                password=options['vertica_password']),
            echo=False)
    }

    services['database'] = services[options['database']]

    return services


def get_aws_account_id():
    client = boto3.client("sts")
    account_id = client.get_caller_identity()["Account"]
    return account_id


def valid_date(s):
    try:
        return dateparser.parse(s, "%Y-%m-%d")
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def cleanup(engine, now, months, table):
    # Duplicated logic from above
    cutoff = now + relativedelta(
        day=1, hour=0, minute=0, second=0, microsecond=0)

    for log in range(0, months):
        cutoff = cutoff + relativedelta(months=-1)

    with engine.connect() as connection:
        res = connection.execute(
            "SELECT count(1) from %s where invoicedate < '%s'" %
            (table, cutoff.date()))
        print(
            "# Cleanup %s rows at %s" % (res.fetchall()[0][0], cutoff.date()))
        res = connection.execute(
            "DELETE from %s where invoicedate < '%s'" % (table, cutoff.date()))


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    VERTICA_DSN = "vertica+vertica_python://{username}:{password}@{host}:5433/metrics"
    REDSHIFT_DSN = "redshift+psycopg2://{username}@{host}/{database}"

    parser.add_argument('--use-cache', action='store_true', default=False)
    parser.add_argument(
        '--aws_account_id', type=int, default=get_aws_account_id())
    parser.add_argument('--months', type=int, default=2)
    parser.add_argument('--limit', type=int, default=False)
    parser.add_argument(
        '--table', type=str, default='ods_itsm_aws_monthly_cost_bonobo')
    parser.add_argument('--cleanup', dest='cleanup', action='store_true')
    parser.add_argument('--no-cleanup', dest='cleanup', action='store_false')
    parser.set_defaults(cleanup=True)
    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, default=False)
    parser.add_argument(
        '--vertica-host', type=str, default='vsql.dataviz.allizom.org')
    parser.add_argument('--vertica-dsn', type=str, default=VERTICA_DSN)

    parser.add_argument('--database', type=str, default='mysql')

    parser.add_argument(
        '--now',
        required=False,
        default=datetime.datetime.now(),
        type=valid_date)

    with bonobo.parse_args(parser) as opt:
        svcs = get_services(**opt)

        bonobo.run(get_graph(**opt), services=svcs)
        if opt['cleanup']:
            cleanup(svcs['database'], opt['now'], opt['months'], opt['table'])
