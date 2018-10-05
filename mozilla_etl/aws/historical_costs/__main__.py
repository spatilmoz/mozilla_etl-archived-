import bonobo
import bonobo_sqlalchemy
import requests
import boto3
import sys

from bonobo.config import use, use_context, use_raw_input, use_context_processor
from bonobo.constants import NOT_MODIFIED
from bonobo.errors import ValidationError
from requests.auth import HTTPBasicAuth
import time
import datetime
from dateutil import parser as dateparser

from sqlalchemy import create_engine


# We should lowercase fields and all here
@use_context
class AwsBillingReader(bonobo.CsvReader):
    #def __init__(self, *args, **kwargs):
    #        print("XXX: Overloaded -_init__")
    #        bonobo.CsvReader.__init__(self, *args, **kwargs)

    def reader_factory(self, file):
        #print("XXX: Ovreloaded reader factory")
        reader = super(AwsBillingReader, self).reader_factory(file)
        # Discard useless header
        headers = next(reader)
        return reader

    def __call__(self, file, context, *, fs):
        reader = self.reader_factory(file)

        if not context.output_type:
            self.fields = next(reader)
            self.fields = list(map(lambda x: x.lower(), *self.fields))
            self.fields = list(
                map(lambda x: x.replace(':', '_'), *self.fields))
            #print("XXX: Setting fields to %s" % (self.fields))
            context.set_output_fields(*self.fields)

        #print("XXX: Overloaded read method")
        #print("XXX: Fields are %s" % self.fields)

        return super(AwsBillingReader, self).__call__(file, context, fs=fs)


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
                mydict[key] = dateparser.parse(mydict[key])
            else:
                print("XXX: Could not parse date %s for key %s" % (mydict[key],
                                                                   key))
                context.error(sys.exc_info(), level=0)
                return

    # Fix numbrers, yuck
    if mydict['blendedrate'] == "":
        mydict['blendedrate'] = "0"

    if not mydict['linkedaccountname']:
        print("XXX: Skipping row without linked account name %s" % mydict)
        context.error(sys.exc_info(), level=0)
        return

    if not mydict['invoicedate']:
        print("XXX: Skiupping row wit4hout invoice date %s" % mydict)
        context.error(sys.exc_info(), level=0)
        return

    yield tuple(mydict.values())


def get_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph()

    graph.add_chain(
        bonobo.noop,
        bonobo.JsonWriter('billing.json'),
        _name="main",
        _input=None,
    )

    now = options['now']

    # Get the last day of last month by taking the first day of this month
    # and subtracting 1 day.
    current = datetime.date(now.tm_year, now.tm_mon, 1) - datetime.timedelta(1)
    first = current.replace(day=1)

    last = datetime.date(first.year, first.month, 1) - datetime.timedelta(1)
    previous = last.replace(day=1)

    print("# Processing %s and %s" % (first, previous))

    graph.add_chain(
        parse_dates,
        bonobo_sqlalchemy.InsertOrUpdate(
            table_name='ods_itsm_aws_monthly_cost',
            discriminant=('invoiceid', 'linkedaccountid', 'payeraccountid',
                          'recordtype', 'recordid'),
            engine='db'),
        bonobo.count,
        _input=bonobo.noop,
    )

    graph.add_chain(
        AwsBillingReader(
            '%s-aws-cost-allocation-%s.csv' % (options['aws_account_id'],
                                               first.strftime("%Y-%m")),
            fs='s3',
            skip=1),
        #bonobo.Limit(1),
        _output="main",
    )
    graph.add_chain(
        AwsBillingReader(
            '%s-aws-cost-allocation-%s.csv' % (options['aws_account_id'],
                                               previous.strftime("%Y-%m")),
            fs='s3',
            skip=1),
        #bonobo.Limit(1),
        _output="main",
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
        'servicenow':
        servicenow,
        'db':
        create_engine('sqlite:///test.sqlite', echo=False),
        's3':
        bonobo.open_fs('s3://mozilla-programmatic-billing'),
        'redshift':
        create_engine(
            'redshift+psycopg2://etl_edw@mozit-dw-dev.czbv3z9khmhv.us-west-2.redshift.amazonaws.com/edw-dev-v1',
            echo=False),
        'vertica':
        create_engine(
            options['vertica'].format(
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


if __name__ == '__main__':
    parser = bonobo.get_argument_parser()

    vertica_dsn = "vertica+vertica_python://{username}:{password}@vsql.dataviz.allizom.org:5433/metrics"

    parser.add_argument(
        '--aws_account_id', type=int, default=get_aws_account_id())
    parser.add_argument('--vertica-username', type=str, default='tableau')
    parser.add_argument('--vertica-password', type=str, required=True),
    parser.add_argument(
        '--vertica', type=str, required=False, default=vertica_dsn)
    parser.add_argument(
        '--now', required=False, default=time.localtime(), type=valid_date)

    with bonobo.parse_args(parser) as options:
        bonobo.run(get_graph(**options), services=get_services(**options))
