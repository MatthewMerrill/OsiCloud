# Matthew Merrill <mattmerr.com>
# HackDavis 2019
import requests as req
import websocket

from google.cloud import spanner

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


def with_electricity_webid(building_name_pair):
    try:
        building, name = building_name_pair
        elements = send_request(building['Links']['Elements'])
        electricity_values = send_request(get_named_item(elements, 'Electricity')['Links']['Value'])
        electricity_demand = get_named_item(electricity_values, 'Demand')
        return (electricity_demand['WebId'], name)
    except:
        return None


def open_channel(tups):
    names = { tup[0]: tup[1] for tup in tups }
    #tups = tups[0:8]
    url = ROOT_URL + '/streamsets/channel?' + (''.join([ '&WebId=' + tup[0] for tup in tups]))[1:]
    url = url.replace('https', 'wss')
    print(url)
    socket = websocket.create_connection(url)
    while True:
        yield socket.recv()


if __name__ == '__main__':
    buildings = loadBuildings(ROOT_URL)
    building_name_tups = [with_display_name(building) for building in buildings]
    elecweb_name_tups = (with_electricity_webid(pair) for pair in building_name_tups)
    elecweb_name_tups = [pair for pair in elecweb_name_tups if pair is not None]

    spanner_client = spanner.Client()
    spanner_instance_id = 'hackdavisdatadump'
    spanner_instance = spanner_client.instance(spanner_instance_id)
    spanner_database_id = 'datadump'
    spanner_database = instance.database(database_id)

    for data in open_channel(elecweb_name_tups):
        with spanner_database.batch() as batch:
            batch.insert(
                    table='electricity',
                    columns = ('BuildingName', 'Timestamp', 'Value'),
                    values = [
                        data
                        ])
        print(data)



