import requests
from dataclasses import dataclass
import base64
import json
import pandas as pd
import datetime
import os
import pickle
import numpy as np
import copy
import yaml
import IPython




@dataclass
class ArtistData:
    artist: None
    popularity: None
    followers: None


# Spotify API credentials
client_id = '82fab1bc845649d786fbca9a6420e494'
client_secret = '14dc6a7823fc4e25afd3fa1df282e3ef'

# Function to get an access token
def get_access_token(client_id, client_secret):
    # Spotify URL for authentication
    auth_url = 'https://accounts.spotify.com/api/token'

    # Encode as Base64
    credentials = f'{client_id}:{client_secret}'
    credentials_b64 = base64.b64encode(credentials.encode())

    # Make a POST request
    auth_response = requests.post(auth_url, 
                                  headers={'Authorization': f'Basic {credentials_b64.decode()}'}, 
                                  data={'grant_type': 'client_credentials'})

    # Convert the response to JSON
    auth_response_data = auth_response.json()

    # Get the access token
    access_token = auth_response_data['access_token']

    return access_token

def get_artist_data(artist_id, access_token, id_is_name=False):

    if id_is_name:
        artist_id = search_artist(artist_id, access_token)

    artist_url = f'https://api.spotify.com/v1/artists/{artist_id}'

    response = requests.get(artist_url, headers={'Authorization': f'Bearer {access_token}'})
    artist_data = response.json()

    return artist_data

def get_artist_genres(artist_id, access_token, id_is_name=False):

    # if id_is_name:
    #     artist_id = search_artist(artist_id, access_token)

    # artist_url = f'https://api.spotify.com/v1/artists/{artist_id}'

    # response = requests.get(artist_url, headers={'Authorization': f'Bearer {access_token}'})
    # artist_data = response.json()

    artist_data = get_artist_data(artist_id, access_token, id_is_name)

    return artist_data.get('genres', [])

def search_artist(artist_name, access_token):
    search_url = f'https://api.spotify.com/v1/search?q={artist_name}&type=artist'

    response = requests.get(search_url, headers={'Authorization': f'Bearer {access_token}'})
    search_results = response.json()

    artists = search_results.get('artists', {}).get('items', [])
    if artists:
        return artists[0].get('id', None)
    return None

def get_artist_data_from_subgenres(subgenre, access_token):

    # Make a bunch of queries for artist data according to subgenre. Because there is
    # a limit on number of artists that can be returned per response, keep making 
    # queries while moving the offset index until no data is returned. This is how
    # you get all the data.
    limit = 50
    offset = 0
    q_str = 'genre:"' + subgenre + '"'
    print(q_str)

    artist_data = []

    responses = []

    while(True):

        # Update the subgenre string here.
        # params = {'q': 'subgenre:"urdu hip hop"', 'type': 'artist', 'limit': limit, 'offset': offset}
        params = {'q': q_str, 'type': 'artist', 'limit': limit, 'offset': offset}
        response = requests.get(search_url, params=params, headers={'Authorization': f'Bearer {access_token}'})
        responses.append(response)
        this_artist_data = response.json()
        

        # I think the total field represents the total number of entries...
        # should use this total field to help ensure we get the same consistent data.
        # print(list(this_artist_data['artists'].keys()))
        # print(this_artist_data['artists']['total'])


        len_items = len(this_artist_data['artists']['items']) if 'artists' in this_artist_data else None
        request_error_detected = 'error' in this_artist_data

        if len_items==0 or request_error_detected:
            break
        else:
            offset = offset + len_items

            for k in this_artist_data['artists']['items']:
                artist_data.append(k)

    # # For some reason, the artist names are sometimes repeated and don't always come
    # # in the same order in the response we make above. So get unique values and sort for consistency.
    # artist_names_unique = sorted(set(artist_names))

    # print(artist_names_unique)
    # print(len(artist_names_unique))

    # return artist_names_unique
    return artist_data, responses

def remove_duplicates_from_artist_data(artist_data_in):

    artist_data_out = []
    artist_data_out_ids = []
    for a in artist_data_in:
        if a['id'] not in artist_data_out_ids:
            artist_data_out.append(a)
            artist_data_out_ids.append(a['id'])
        
    return artist_data_out

def filter_artists_by_popularity(artist_data_in, popularity_threshold=50):

    artist_data_out = []
    for a in artist_data_in:
        if a['popularity'] > popularity_threshold:
            artist_data_out.append(a)
        
    return artist_data_out

def build_artist_df(artist_data):

    import copy
    
    artist_data = copy.copy(artist_data)

    if isinstance(artist_data, dict):
        artist_data = [artist_data]

    for a in artist_data:
        a['followers'] = a['followers']['total']
        a['genres'] = str(a['genres'])
        a['link'] = a['external_urls']['spotify']
        a.pop('href')
        a.pop('images')
        a.pop('uri')
        a.pop('type')
        a.pop('external_urls')
    
    df = pd.DataFrame(artist_data)

    column_order = ['name', 'id', 'link', 'genres', 'popularity', 'followers']

    df = df[column_order]

    return df

def update_csv(df, csv_file):

    if os.path.isfile(csv_file):
        old_df = pd.read_csv(csv_file)
        new_df = old_df.merge(df, how='outer', on='name')
        new_df = new_df.sort_values(by=['name'])
    else:
        new_df = df

    new_df.to_csv(csv_file, index=False)

def build_data_dfs(df, data_timestamp):

    data_timestamp_str = data_timestamp.strftime("%m-%d-%Y, %H:%M:%S")

    artist_df = df[['name', 'id', 'link', 'genres']]

    popularity_df = pd.DataFrame(df[['name', 'id']])
    popularity_df[data_timestamp_str] = df['popularity']

    followers_df = pd.DataFrame(df[['name', 'id']])
    followers_df[data_timestamp_str] = df['followers']

    d = ArtistData(artist_df, popularity_df, followers_df)

    return d

def diff_and_update_artist_data(d1, d2): 

    updated_d1 = copy.deepcopy(d1)

    diff_fields = ['name', 'genres', 'link']

    # Get the entries in the data corresponding to the same id and put it in a temp dataframe.
    suffixes=['_x', '_y']
    temp_d = d1.artist.merge(d2.artist, how='inner', on='id', suffixes=suffixes)

    for ch in diff_fields:

        # For each field of interest, look for a diff between the two datasets.
        ch_x = ch + suffixes[0]
        ch_y = ch + suffixes[1]
        equal_ch = ch + '_changed'
        temp_d[equal_ch] = np.equal(temp_d[ch_x].values, temp_d[ch_y].values)
        changed_idx = np.where(np.invert(temp_d[equal_ch].values))[0]

        # Update the returned copy of d1 to be updated with new values
        # from  d2. Log any changes for reporting and debugging.
        for i in changed_idx:
            id = temp_d.id.values[i]
            name = temp_d.name.values[i]

            old_value = temp_d[ch_x].values[i]
            new_value = temp_d[ch_y].values[i]

            if ch in updated_d1.artist.keys():
                updated_d1.artist[ch][np.where(d1.artist.id.values==id)[0][0]] = new_value

            if ch in updated_d1.popularity.keys():
                updated_d1.popularity[ch][np.where(d1.popularity.id.values==id)[0][0]] = new_value
    
            if ch in updated_d1.followers.keys():
                updated_d1.followers[ch][np.where(d1.followers.id.values==id)[0][0]] = new_value

            txt = "ID {id} with  name {name} changed artist atttribute '{ch}' from '{old_value}' to '{new_value}'"
            txt = txt.format(id=id, name=name, ch=ch, old_value=str(old_value), new_value=str(new_value))
            print(txt)


    return updated_d1

   
    


def merge_artist_data(d1, d2):

    output_d = ArtistData(None, None, None)

    if d1.artist is not None and d2.artist is not None:

        d1  = diff_and_update_artist_data(d1, d2)

        # output_d.artist = d1.artist.merge(d2.artist, how='outer', on='name')
        output_d.artist = d1.artist.merge(d2.artist, how='outer', on='id', suffixes=[None, '_y'])
        output_d.artist = output_d.artist.sort_values(by=['name'])
        output_d.artist = output_d.artist.drop(columns=['name_y', 'genres_y', 'link_y'])

        # output_d.popularity = d1.popularity.merge(d2.popularity, how='outer', on='name')
        output_d.popularity = d1.popularity.merge(d2.popularity, how='outer', on='id', suffixes=[None, '_y'])
        output_d.popularity = output_d.popularity.sort_values(by=['name'])
        output_d.popularity = output_d.popularity.drop(columns=['name_y'])

        # output_d.followers = d1.followers.merge(d2.followers, how='outer', on='name')
        output_d.followers = d1.followers.merge(d2.followers, how='outer', on='id', suffixes=[None, '_y'])
        output_d.followers  = output_d.followers.sort_values(by=['name'])
        output_d.followers = output_d.followers.drop(columns=['name_y'])
    
    elif d1.artist is None:
        output_d = copy.copy(d1)
    elif d2.artist is None:
        output_d = copy.copy(d2)
    else:
        pass

    return output_d

def get_genres(seed_artists, include_genres, exclude_genres):

    # Start with include genres.
    genres = copy.copy(include_genres)

    # Search through artists and get their genres.
    seed_artist_genres = []
    for a in seed_artists:
        artist_id = search_artist(a, access_token)
        this_artist_genres = get_artist_genres(artist_id, access_token)
        for s in this_artist_genres:
            seed_artist_genres.append(s)

    genres += seed_artist_genres

    # Remove genres in exclude genres.
    genres = [s for s in genres if s not in exclude_genres]

    # Remove duplicate genres.
    genres = list(set(genres))

    return genres

def run_artist_discovery(access_token, cfg):

    # Get cfg.
    seed_artists = cfg['seed_artists']
    include_genres = cfg['include_genres']
    exclude_genres = cfg['exclude_genres']
    popularity_threshold = cfg['popularity_threshold']

    # Build genre list.
    genres = get_genres(seed_artists, include_genres, exclude_genres)

    # Get all data for artists from these genres.
    new_artist_data = []
    for s in genres:
        a_list, responses = get_artist_data_from_subgenres(s, access_token)
        for a in a_list:
            new_artist_data.append(a)
    df = build_artist_df(new_artist_data)

    # Only keep new artists that today exceed the popularity threshold.
    df = df[df.popularity>popularity_threshold]
    df = df.drop_duplicates(subset='name')
    df = df.sort_values('name')

    return df


if __name__ == "__main__":
    
    enable_artist_discovery = False
    enable_artist_update = True

    # config_file = 'config.yaml'
    config_file = 'config_afrobeat.yaml'

    with open(config_file, 'r') as stream:
        cfg = yaml.safe_load(stream)

    search_url = f'https://api.spotify.com/v1/search'

    # Main process
    access_token = get_access_token(client_id, client_secret)



    data_timestamp = datetime.datetime.now()

    # 1. Read artists list from file. Get today's artists data. Use id to avoid issues caused by name changes etc...
    if enable_artist_update:
        prev_artist_df = pd.read_csv('artist.csv')
        prev_popularity_df = pd.read_csv('popularity.csv')
        prev_followers_df = pd.read_csv('followers.csv')
        prev_data = ArtistData(prev_artist_df, prev_popularity_df, prev_followers_df)
        artist_id_list = prev_artist_df.id.values

        artist_data = []
        for a in artist_id_list:
            print(a)
            artist_data.append(get_artist_data(a, access_token))

        today_df = build_artist_df(artist_data)

        today_data = build_data_dfs(today_df, data_timestamp)
        final_data = merge_artist_data(prev_data, today_data)

        final_data.artist.to_csv('artist.csv', index=False)
        final_data.popularity.to_csv('popularity.csv', index=False)
        final_data.followers.to_csv('followers.csv', index=False)

    # 2. Discover newly popular artists from genre search.
    if enable_artist_discovery:
        new_today_df = run_artist_discovery(access_token, cfg['artist_discovery'])
        new_today_data = build_data_dfs(new_today_df, data_timestamp)
        new_today_data.artist.to_csv('artist_discover.csv', index=False)
        new_today_data.popularity.to_csv('popularity_discover.csv', index=False)
        new_today_data.followers.to_csv('followers_discover.csv', index=False)
    
