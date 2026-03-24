#!/usr/bin/python3
import functools
import json
import subprocess
import time

import requests


PRICES_URL = 'https://www.fuel-finder.service.gov.uk/api/v1/pfs/fuel-prices'
STATIONS_URL = 'https://www.fuel-finder.service.gov.uk/api/v1/pfs'
TOKEN_URL = 'https://www.fuel-finder.service.gov.uk/api/v1/oauth/generate_access_token'

CREDENTIALS_PATH = 'credentials.json'
OUTPUT_PATH = 'docs/all.json'


def rate_limit(period):
    def wrap(f):
        available_at = time.time()

        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            nonlocal available_at
            delay = available_at - time.time()
            if delay > 0:
                print(f"Sleeping for {delay}")
                time.sleep(delay)
            available_at = time.time() + period
            return f(*args, **kwargs)

        return wrapper
    return wrap


_token = None
_token_expires_at = 0
TOKEN_EXPIRY_BUFFER = 300


def get_token():
    global _token, _token_expires_at

    token = None

    # Can we re-use the in-memory token?
    if time.time() < _token_expires_at - TOKEN_EXPIRY_BUFFER:
        print('Re-using in-memory token')
        token = _token

    # Can we re-use the on-disk token?
    else:
        try:
            with open('token.json') as f:
                body = json.load(f)
            print('Checking on-disk token')
            _token = body['data']['access_token']
            _token_expires_at = body['data']['expires_at']
            if time.time() < _token_expires_at - TOKEN_EXPIRY_BUFFER:
                print('Re-using on-disk token')
                token = _token
        except FileNotFoundError:
            pass

    if token is None:
        # Nothing usable ... get a fresh token
        with open(CREDENTIALS_PATH) as f:
            credentials = json.load(f)

        print('Requesting token')
        response = requests.post(TOKEN_URL, data={
                'grant_type': 'client_credentials',
                'client_id': credentials['client_id'],
                'client_secret': credentials['client_secret'],
                'scope': 'fuelfinder.read',
            })
        body = response.json()
        print('   ...done')
        body['data']['expires_at'] = time.time() + body['data']['expires_in']
        with open('token.json', 'w') as f:
            json.dump(body, f, indent=2)
        _token = body['data']['access_token']
        _token_expires_at = body['data']['expires_at']
        token = _token

    return token


@rate_limit(2)
def station_batch(token, batch_number):
    print(f"Fetching station batch {batch_number}")
    response = requests.get(
        STATIONS_URL,
        headers={'Authorization': f"Bearer {token}"},
        params={'batch-number': batch_number}
    )
    if response.ok:
        batch = response.json()
    else:
        batch = []
    return batch


def all_stations(token):
    batch_number = 1
    while stations := station_batch(token, batch_number):
        yield from stations
        batch_number += 1


@rate_limit(2)
def price_batch(token, batch_number):
    print(f"Fetching price batch {batch_number}")
    response = requests.get(
        PRICES_URL,
        headers={'Authorization': f"Bearer {token}"},
        params={'batch-number': batch_number}
    )
    if response.ok:
        batch = response.json()
    else:
        batch = []
    return batch


def all_prices(token):
    batch_number = 1
    while prices := price_batch(token, batch_number):
        yield from prices
        batch_number += 1


def create_station_lookup(stations):
    def simple_station(station):
        return {
            'brand': station['brand_name'],
            'location': {
                'latitude': station['location']['latitude'],
                'longitude': station['location']['longitude'],
            },
            'node_id': station['node_id'],
        }

    stations_by_id = {station['node_id']: simple_station(station)
                      for station in stations}
    return stations_by_id


def update_station_lookup(stations_by_id, station_prices):
    def convert(fuel_info):
        price = fuel_info['price']
        # Handle the cases where the price has been supplied in pounds or tenths
        # of a penny.
        if price < 10:
            price *= 100
        elif price > 1000:
            price /= 10
        return {
            'price': price,
            'price_last_updated': fuel_info['price_last_updated']
        }

    for station in station_prices:
        prices = {fuel['fuel_type']: convert(fuel)
                  for fuel in station['fuel_prices']}
        stations_by_id[station['node_id']]['prices'] = prices


def download_all():
    token = get_token()
    stations = all_stations(token)
    stations_by_id = create_station_lookup(stations)
    prices = all_prices(token)
    update_station_lookup(stations_by_id, prices)
    node_ids = sorted(stations_by_id.keys())
    pricing = [stations_by_id[node_id] for node_id in node_ids]
    pricing = [station for station in pricing if 'prices' in station]
    with open(OUTPUT_PATH, 'w') as f:
        json.dump(pricing, f, indent=2)


def run(args):
    print(" ".join(args))
    process = subprocess.run(args, capture_output=True, check=True, text=True)
    return process.stdout


def publish():
    stdout = run(["git", "status", "--porcelain"])
    if f" M {OUTPUT_PATH}" in stdout.split("\n"):
        run(["git", "add", OUTPUT_PATH])
        run(["git", "commit", "-m", "Update data"])
        run(["git", "push", "origin", "main"])
    else:
        print("No change")


def update():
    download_all()
    publish()


if __name__ == "__main__":
    update()
