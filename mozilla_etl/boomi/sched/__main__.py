import bonobo
import bonobo_sqlalchemy
import os

from bonobo.config import Service, use, use_no_input, use_context, use_context_processor, use_raw_input
from bonobo.config.functools import transformation_factory
from bonobo.constants import NOT_MODIFIED

from dateutil import parser as dateparser

import re
import io
import csv

import requests
from requests.auth import HTTPBasicAuth

from googleapiclient.discovery import build
from httplib2 import Http
from oauth2client import file, client, tools

# If modifying these scopes, delete the file token.json.
SCOPES = 'https://www.googleapis.com/auth/spreadsheets.readonly'

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1FWaNy_3PGSXOUSYnBpP2NLlWOhAogT8bIE3kU7iY1mw'
SAMPLE_RANGE_NAME = 'A4:X'

SCHED_CONFERENCE = 'mozlandodecember2018'
SCHED_API_KEY = os.getenv('SCHED_API_KEY')

_cache = {}


def cache(self, context):
    yield _cache


def get_sched():
    for event in requests.get(
            "https://{conference}.sched.com/api/session/list?api_key={api_key}&format=json&custom_data=Y"
            .format(conference=SCHED_CONFERENCE,
                    api_key=SCHED_API_KEY)).json():
        yield event


def get_sheet():
    store = file.Storage('token.json')
    creds = store.get()

    if not creds or creds.invalid:
        flow = client.flow_from_clientsecrets('credentials.json', SCOPES)
        creds = tools.run_flow(flow, store)

    service = build('sheets', 'v4', http=creds.authorize(Http()))

    # Call the Sheets API
    sheet = service.spreadsheets()
    result = sheet.values().get(
        spreadsheetId=SAMPLE_SPREADSHEET_ID,
        range=SAMPLE_RANGE_NAME).execute()
    for value in result.get('values'):
        if len(value) < 24:
            value.extend([""] * (24 - len(value)))
        yield tuple(value)


@use_raw_input
@use_context_processor(cache)
def cache_sheet(cache, row):
    if row.get('last_modified'):
        event_key = row.get('event_key')
        cache[event_key] = row._asdict()
        yield NOT_MODIFIED


@use_raw_input
@use_context_processor(cache)
def modified_events(cache, row):
    event_key = row.get('event_key')
    google_event = cache[event_key]

    if google_event['last_modified'] == "Y":

        event = row._asdict()
        event['google'] = google_event

        yield event


def sync_event(event):

    return NOT_MODIFIED


def get_services(**options):
    return {}


def get_sched_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph(
        get_sched,
        bonobo.UnpackItems(0),
        modified_events,
        sync_event,
        bonobo.UnpackItems(0),
        bonobo.PrettyPrinter(),
        bonobo.count,
    )

    return graph


def get_sheet_graph(**options):
    """
    This function builds the graph that needs to be executed.

    :return: bonobo.Graph

    """
    graph = bonobo.Graph(
        get_sheet,
        bonobo.SetFields(fields=[
            "last_modified",
            "event_key",
            "name",
            "active",
            "eventstarttime",
            "eventendtime",
            "event_start",
            "event_end",
            "event_type",
            "event_subtype",
            "seats",
            "description",
            "speakers",
            "vmoderators",
            "vartists",
            "sponsors",
            "exhibitors",
            "volunteers",
            "venue",
            "address",
            "media_url",
            "custom3",
            "audience1",
            "audience2",
        ]),
        cache_sheet,
        bonobo.count,
    )

    return graph


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

    with bonobo.parse_args(parser) as options:
        services = get_services(**options)
        add_default_services(services, options)

        bonobo.run(get_sheet_graph(**options), services=services)

        bonobo.run(get_sched_graph(**options), services=services)
