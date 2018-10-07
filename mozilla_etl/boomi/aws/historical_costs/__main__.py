import bonobo
import bonobo_sqlalchemy
import requests
import boto3
import sys

from bonobo.config import Configurable, Service, ContextProcessor, use, use_context, use_raw_input, use_context_processor, use_no_input
from bonobo.constants import NOT_MODIFIED
from bonobo.errors import ValidationError
from requests.auth import HTTPBasicAuth
from dateutil.relativedelta import relativedelta
from bonobo.config.functools import transformation_factory
from dateutil import parser as dateparser
from datetime import *
from bonobo.util.objects import ValueHolder

from sqlalchemy import create_engine


def _cleanup(self, context):
    cleanup = yield ValueHolder(0)
    #value = cleanup.get()
    print("XXX At this point is a cleanup possibiity")
    context.send(1)
    context.send(1)
    context.send(1)


@use_no_input
@use_context_processor(_cleanup)
def auto_cleanup(cleanup):
    return


class Cleanup(Configurable):
    engine = Service('sqlalchemy.engine')  # type: str

    def __call__(self, *args, **kwargs):
        return NOT_MODIFIED

    @use(engine)
    def test(self, engine):
        print("XXX Test %s %s" % self.engine, engine)

    def __del__(self):
        print("XXX Deleting")
        self.test()
        #engine = Service(self.engine).resolve()
        #connection = engine.connect()
        #connection.execute("SELECT 1")
        return


# We should lowercase fields and all here
@use_context
class AwsBillingReader(bonobo.CsvReader):
    def reader_factory(self, file):
        #print("XXX: Ovreloaded reader factory")
        reader = super(AwsBillingReader, self).reader_factory(file)
        # Discard useless header amazon message
        headers = next(reader)
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


#sqlite> CREATE TABLE fact_itsm_aws_historical_cost (
#  ...>     date_sk integer NOT NULL,
#  ...>     account_name_sk integer NOT NULL,
#  ...>     total_cost double precision NOT NULL,
#  ...>     productname character varying(60)
@use_raw_input
def filter_summary(row):
    mydict = row._asdict()

    if mydict['recordtype'] == 'LinkedLineItem':
        yield {
            'total_cost': mydict['totalcost'],
            'productname': mydict['productname'],
            'date_sk': mydict['billingperiodenddate'],
            'account_name_sk': mydict['linkedaccountid'],
        }


@use_context
@use_raw_input
def parse_dates(context, row):
    mydict = row._asdict()

    keys = mydict.keys()

    if not context.output_type:
        context.set_output_fields(keys)

    for key in keys:
        if "date" in key and mydict[key] != "":
            parsed_date = dateparser.parse(mydict[key])
            if parsed_date:
                mydict[key] = parsed_date.date()
                #print("XXX: %s is a %s" % (key, type(parsed_date)))
            else:
                print("XXX: Could not parse date %s for key %s" % (mydict[key],
                                                                   key))
                context.error(
                    "XXX: Could not parse date %s for key %s" % (mydict[key],
                                                                 key),
                    level=0)
                return

    # Fix numbrers, yuck
    if mydict['blendedrate'] == "":
        mydict['blendedrate'] = "0"

    # Fix numbrers, yuck
    if mydict['rateid'] == "":
        mydict['rateid'] = "0"

    if not mydict['linkedaccountname']:
        #print("XXX: Skipping row without linked account name %s" % mydict)
        context.error(
            "XXX: Skipping row without linked account name %s" % mydict,
            level=0)
        return

    if not mydict['invoicedate']:
        #print("XXX: Skipping row without invoice date %s" % mydict)
        context.error("XXX: Skipping row without invoice date %s", level=0)
        return

    yield tuple(mydict.values())


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    graph.add_chain(
        #bonobo.noop,
        bonobo.JsonWriter('billing.json'),
        bonobo.CsvWriter('billing.csv'),
        parse_dates,
        # Summary part
        filter_summary,
        bonobo.UnpackItems(0),
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name='fact_itsm_aws_historical_cost',
            discriminant=(
                'productname',
                'date_sk',
                'account_name_sk',
            ),
            buffer_size=10,
            engine='mysql'),
        bonobo.count,
        auto_cleanup,
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
        graph.add_chain(
            AwsBillingReader(
                '%s-aws-cost-allocation-%s.csv' % (options['aws_account_id'],
                                                   tstamp),
                fs='s3',
                skip=1),
            _output="main",
        )

    graph.add_chain(
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name=options['table'],
            discriminant=('invoiceid', 'linkedaccountid', 'payeraccountid',
                          'recordid'),
            buffer_size=10,
            engine='mysql'),
        bonobo.count,
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

    return {
        'mysql':
        create_engine('mysql+mysqldb://localhost/aws', echo=False),
        #create_engine('sqlite:///test.sqlite', echo=False),
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
        print("XXX: Cleanup %s rows at %s" % (res.fetchall()[0][0],
                                              cutoff.date()))
        res = connection.execute(
            "DELETE from %s where invoicedate < '%s'" % (table, cutoff.date()))


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    vertica_dsn = "vertica+vertica_python://{username}:{password}@{host}:5433/metrics"

    parser.add_argument(
        '--aws_account_id', type=int, default=get_aws_account_id())
    parser.add_argument('--months', type=int, default=2)
    parser.add_argument(
        '--table', type=str, default='ods_itsm_aws_monthly_cost')
    parser.add_argument('--cleanup', dest='cleanup', action='store_true')
    parser.add_argument('--no-cleanup', dest='cleanup', action='store_false')
    parser.set_defaults(cleanup=True)
    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, required=True),
    parser.add_argument(
        '--vertica-host', type=str, default='vsql.dataviz.allizom.org'),
    parser.add_argument('--vertica-dsn', type=str, default=vertica_dsn)

    parser.add_argument(
        '--now', required=False, default=datetime.now(), type=valid_date)

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)

        bonobo.run(get_graph(**options), services=services)
        if options['cleanup']:
            cleanup(services['mysql'], options['now'], options['months'],
                    options['table'])
