"""
SIMBS Waivers
Chris Luginbuhl

Gathers future events from Eventbrite API, gets an event selection from the user,
and checks to see if those users have a current waiver using the Waiverforever API

"""

import requests
import credentials
from tqdm import tqdm
from loguru import logger
from collections import namedtuple
from datetime import datetime, timedelta
from credentials import GOOGLE_SHEETS_API_KEY, SPREADSHEET_ID

EVENTBRITE_BASE_URL='https://www.eventbriteapi.com/v3'
EVENTBRITE_ORG_ENDPOINT='/organizations/'
EVENTBRITE_EVENTS_ENDPOINT='/events/'  # can append event ID to this
EVENTBRITE_AUTH_ENDPOINT='/users/me/?token=' # add token to this
EVENTBRITE_ORDERS_ENDPOINT='/orders?status=all_not_deleted'
EVENTBRITE_TIME_FORMAT = "%Y-%m-%dT%H:%M:%S"
WAIVERFOREVER_SEARCH_ENDPOINT='https://api.waiverforever.com/openapi/v1/waiver/search'
WAIVER_VALIDITY = 365     # waiver validity period in days

logger.add("file_{time}.log", rotation="10 MB")

registrant_datafield_names = [
    "email",
    "first_name",
    "last_name",
]
Registrant = namedtuple("Registrant", registrant_datafield_names)


def eventbrite_api_request(url) -> dict:
    headers = {
    "Authorization": "Bearer " + credentials.EVENTBRITE_API_KEY
    }
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    json_data = response.json()
    return json_data


def get_future_events():
    time_now = datetime.now()
    data = eventbrite_api_request(EVENTBRITE_BASE_URL +
                           EVENTBRITE_ORG_ENDPOINT +
                           credentials.EVENTBRITE_ORGANIZATION_ID +
                           EVENTBRITE_EVENTS_ENDPOINT
    )
    # Make list of events with end time in the future, using local time zone
    f_events = [item for item in data['events'] if datetime.strptime(item['end']['local'], EVENTBRITE_TIME_FORMAT) > time_now]
    logger.info(f"Found {len(f_events)} future events")
    logger.info(data)
    return f_events


def get_event_selection(f_events):
    for index, item in enumerate(f_events):
        print(index + 1, ". " + item['name']['text'])
    sel = input("\nSelect item: ")
    return int(sel) - 1


def get_registrants(id):
    # get all registrants for the specified event
    data = eventbrite_api_request(EVENTBRITE_BASE_URL +
                           EVENTBRITE_EVENTS_ENDPOINT +
                           id +
                           EVENTBRITE_ORDERS_ENDPOINT)
    regs = [Registrant(order['email'], order['first_name'], order['last_name']) for order in data['orders']]

    # get the event info and extract the end date
    date_data = eventbrite_api_request(EVENTBRITE_BASE_URL +
                           EVENTBRITE_EVENTS_ENDPOINT +
                           id)
    end_date = datetime.strptime(date_data['end']['local'], EVENTBRITE_TIME_FORMAT)
    logger.info(regs)
    logger.info(end_date)
    return regs, end_date


def waiverforever_api_request(url, regs):
    headers = {'X-API-Key': credentials.WAIVERFOREVER_API_KEY}
    data = []
    for reg in tqdm(regs, desc="Searching registrant emails in Waiverforever"):
        response = requests.post(url,
                                 json={"search_term": reg.email},
                                 headers=headers)
        response.raise_for_status()
        json_data = response.json()
        data.append({'eventbrite_data':reg,'waiverforever_data':json_data})
    logger.info(data)
    return data


def process_waiver_data(registration_data, end_date):
# Each registration may have multiple waivers, or zero waivers.
# For each registration, extract email + names, plus dates of each waiver. Assess if one or more waiver is current
# These go into a new list called processed_data

    processed_data = []
    for reg in registration_data:
        dates = []
        is_current = False
        waiverforever_email = ''
        for waiver in reg['waiverforever_data']['data']['waivers']:
            waiver_unix_date = waiver['received_at']
            waiver_date = datetime.fromtimestamp(waiver_unix_date)
            dates.append(waiver_date)
            if waiver_date + timedelta(days=WAIVER_VALIDITY) > end_date:
                is_current = True
            for field in waiver['data']:
                if field['type'] == 'email_field':
                    waiverforever_email = field['value']
        assert (waiverforever_email == '') or (waiverforever_email.lower() == reg['eventbrite_data'].email.lower()), (
                'found a non-matching email field. WF: ' + waiverforever_email + ' EB: ' + reg['eventbrite_data'].email)
        # make a new list of dicts of just the Eventbrite (eb) and Waiverforever (wf) fields we are interested in
        processed_data.append({
            'wf_email': waiverforever_email,
            'eb_email': reg['eventbrite_data'].email,
            'eb_first_name': reg['eventbrite_data'].first_name,
            'eb_last_name': reg['eventbrite_data'].last_name,
            'dates': dates,
            'is_current': is_current,
        })
    logger.info(processed_data)
    return processed_data

def report_to_console(data):
    # Count number of current waivers
    waiver_invalid_count = sum(1 for item in data if not item.get('is_current'))
    print("\nFound " + str(waiver_invalid_count) + " invalid or missing waivers out of " +
          str(len(data)) + " waivers.")
    print("Invalid or missing waivers:")
    print(f"{'Email':<30} {'Firstname':<15} {'Lastname':<15}")
    for waiver in data:
        if not waiver.get('is_current'):
            print(f"{waiver['eb_email']:<30} {waiver['eb_first_name']:<15} {waiver['eb_last_name']:<15}")


def prepare_data_for_reporting(data):
    for item in data:
        if item.get('dates'):
            item['dates'] = [date.strftime('%-m/%-d/%Y') for date in item['dates']]
    return data


def output_to_google_sheets(data):
# Clear then write data to Google Sheet.
    range_name = 'Sheet1!A:Z'

    # Clear old data
    clear_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/{range_name}:clear"
    requests.post(clear_url, params={'key': GOOGLE_SHEETS_API_KEY})

    # Write with USER_ENTERED
    write_url = f"https://sheets.googleapis.com/v4/spreadsheets/{SPREADSHEET_ID}/values/Sheet1!A1"
    params = {
        'valueInputOption': 'USER_ENTERED',
        'key': GOOGLE_SHEETS_API_KEY
    }
    body = {'values': data}

    response = requests.put(write_url, params=params, json=body)
    response.raise_for_status()
    logger.info(response.json())
    return response.json()


future_events = get_future_events()
if not future_events:
    print("No future events found.")
    exit(0)
#Prompt user to select one event from a list
selected_event = get_event_selection(future_events)
print("Selected event: " + future_events[int(selected_event)]['name']['text'] +
      "\nEnds: " + str(future_events[selected_event]['end']['local']) +
      "\nEventbrite event ID: " + str(future_events[selected_event]['id']))
print("\nGetting registrant names and emails from Eventbrite...")
#Get registrants for the selected event
registrants, event_end_date = get_registrants(str(future_events[selected_event]['id']))
#Check Waiverforever for each registrant
waiver_data=waiverforever_api_request(WAIVERFOREVER_SEARCH_ENDPOINT, registrants)
#Compare each registrant's most recent waiver date with event end date
users_x_waivers = process_waiver_data(waiver_data, event_end_date)
formatted_data = prepare_data_for_reporting(users_x_waivers)
report_to_console(formatted_data)
#output_to_google_sheets(formatted_data)


