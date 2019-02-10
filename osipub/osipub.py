# Matthew Merrill <mattmerr.com>
# HackDavis 2019
import requests as req
from websocket import create_connection

ROOT_URL = 'https://ucd-pi-iis.ou.ad3.ucdavis.edu/piwebapi'

def send_request(url):
    return req.get(url).json()


def get_named_item(results, target):
    try:
        return next(item for item in results['Items'] if item['Name'] == target)
    except StopIteration:
        return None


def loadBuildings(root_url):
    root_content = send_request(root_url)
    asset_servers = send_request(root_content['Links']['AssetServers'])
    util_af = send_request(get_named_item(asset_servers, 'UTIL-AF')['Links']['Databases'])
    utilities = send_request(get_named_item(util_af, 'Utilities')['Links']['Elements'])
    buildings = send_request(get_named_item(utilities, 'Buildings')['Links']['Elements'])
    return buildings['Items']


def with_display_name(building):
    values = send_request(building['Links']['Value'])
    display_name_obj = get_named_item(values, 'Display Name')
    if display_name_obj is not None and 'Good' in display_name_obj['Value'] and display_name_obj['Value']['Good']:
        return (building, display_name_obj['Value']['Value'])
    else:
        return (building, building['Name'])


def open_channel(tups):
    names = { tup[0]['WebId']: tup[1] for tup in tups }
    socket = 


if __name__ == '__main__':
    buildings = loadBuildings(ROOT_URL)
    building_name_tups = [with_display_name(building) for building in buildings]
    open_channel(building_name_tups)


