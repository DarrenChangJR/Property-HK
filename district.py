import json

import googlemaps
import pandas as pd
import pydeck as pdk
import requests
from bs4 import BeautifulSoup

import env.keys


base_url = 'https://truehome.hk'
session = requests.Session()
gmaps = googlemaps.Client(key=env.keys.GOOGLE_API_KEY)


def get_prices() -> pd.DataFrame:
    try:
        prices_df = pd.read_json('./data/prices.json', orient='index')
        return prices_df
    except FileNotFoundError:
        pass
    
    home_page = session.get(base_url + '/prices/neighborhoods')
    home_page_soup = BeautifulSoup(home_page.text, 'html.parser')
    neighbourhood_links = [a['href'] for a in home_page_soup.find_all('a', class_='hk11')]

    prices_df = pd.DataFrame()
    for neighbourhood_link in neighbourhood_links:
        page = session.get(base_url + neighbourhood_link)
        if page.status_code != 200: continue
        soup = BeautifulSoup(page.text, 'html.parser')
        data = json.loads(soup.find('script', id='__NEXT_DATA__').string)
        if 'statusCode' in data['props']['pageProps']: continue
        prices_df = pd.concat([prices_df, pd.DataFrame(data['props']['pageProps']['data']['buildings'])], ignore_index=True)
    return prices_df

def get_coordinates(buildingaddress: pd.Series) -> pd.DataFrame:
    
    def get_coordinates(building: str) -> tuple[float, float]:
        geocode_result = gmaps.geocode(address=building, region='HK')
        if not geocode_result: return 0,0
        return geocode_result[0]['geometry']['location']['lat'], geocode_result[0]['geometry']['location']['lng']
    
    try:
        coordinates_df = pd.read_json('./data/coordinates.json', orient='index')
    except FileNotFoundError:
        coordinates_df = pd.DataFrame(columns=['lat', 'lng'])

    buildings_to_get = buildingaddress[~buildingaddress.isin(coordinates_df.index)]
    if buildings_to_get.empty: return coordinates_df
    coordinates_df = pd.concat([coordinates_df, pd.DataFrame(buildings_to_get.apply(get_coordinates).tolist(), index=buildings_to_get, columns=['lat', 'lng'])])
    coordinates_df = coordinates_df[coordinates_df['lat'].notna()]
    return coordinates_df

def get_districts() -> pd.DataFrame:
    district_df = pd.read_json('./data/districts.json', orient='index')
    district_df = district_df.explode('regions')
    district_df = district_df.rename_axis('district').reset_index().set_index('regions')
    return district_df

def combine_and_format_data(prices_df: pd.DataFrame, coordinates_df: pd.DataFrame, district_df: pd.DataFrame) -> pd.DataFrame:
    result_df = prices_df.join(coordinates_df, on='buildingaddress', how='inner')
    result_df = result_df.join(district_df, on='region')

    result_df = result_df[['buildingname', 'buildingaddress', 'region', 'district', 'medianpredprice', 'lat', 'lng', 'transactionscount', 'built', 'numberofunits', 'numberoffloors', 'colour']]
    result_df['medianpredprice'] = result_df['medianpredprice'].astype(int)
    result_df['medianpredprice_formatted'] = result_df['medianpredprice'].apply(lambda x: f'{x:,d}')
    result_df[['r', 'g', 'b']] = result_df['colour'].str.split(',', expand=True).astype(int)
    result_df['lat'] = result_df['lat'].astype(float)
    result_df['lng'] = result_df['lng'].astype(float)
    result_df['transactionscount'] = result_df['transactionscount'].astype(int)
    result_df['built'] = result_df['built'].astype(int)
    result_df['numberofunits'] = result_df['numberofunits'].astype(int)
    result_df['numberoffloors'] = result_df['numberoffloors'].astype(int)

    return result_df

def visualize(prices_df):
    layer = pdk.Layer(
        'ColumnLayer',
        data=prices_df,
        get_position=['lng', 'lat'],
        get_elevation='medianpredprice',
        elevation_scale=0.0001,
        radius=15,
        get_fill_color=['r', 'g', 'b', 140],
        pickable=True,
        auto_highlight=True
    )

    view_state = pdk.ViewState(
        longitude=114.173355,
        latitude=22.302711,
        zoom=11,
        min_zoom=10,
        max_zoom=15,
        pitch=40.5,
        bearing=-27.36)

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip={
            'text': '{buildingname}\nMedian Price: {medianpredprice_formatted}\nUnits: {numberofunits}\nFloors: {numberoffloors}\nRegion: {region}, {district}\nBuilt: {built}\nTransactions: {transactionscount}'
        }
    )
    deck.to_html('district.html')

def main():
    prices_df = get_prices()
    prices_df.to_json('./data/prices.json', orient='index', indent=2)

    coordinates_df = get_coordinates(prices_df['buildingaddress'])
    coordinates_df.to_json('./data/coordinates.json', orient='index', indent=2)

    district_df = get_districts()
    
    result_df = combine_and_format_data(prices_df, coordinates_df, district_df)
    visualize(result_df)

if __name__ == "__main__":
    main()