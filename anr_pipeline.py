import requests
import base64
import json
import pandas as pd
import datetime
import os
import pickle
import numpy as np
import IPython


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

start_artist_names = [
    'AP Dhillon', 'Raf Saperra', 'Rovalio', 'Divine', 
    'Prateek Kuhad', 'Lisa Mishra','Rahul', 'Abdullah Siddiqui', 'Ikky', 
    'Shubh', 'Karan Aujla', 'Hassan Raheem', 'AUR', 'King', 'Sharn', 
    'Ranj', 'Clifr', 'Umair', 'Abhi The Nomad', 'MC Stan','Young Stunners',
    'Prabh Singh', 'Prabh Deep', 'Lifafa', 'Badshah', 'Taimoor Baig', 'Umair'
]
# start_artist_names = [
#     'AP Dhillon'
# ]
access_token = get_access_token(client_id, client_secret)
artist_data = get_artist_data(start_artist_names[0], access_token, True)

# df = build_artist_df(artist_data)

# IPython.embed()


popularity_threshold = 50


exclude_subgenres = ['art pop', 'chicago rap', 'norwegian pop', 'folk-pop']

# Subgenre strings for reference.
subgenres_refernce = {'hindi indie', 'pakistani indie', 'folk-pop', 'hindi hip hop', 'bhangra', 'pakistani hip hop', 'art pop', 'northeast indian indie', 'desi pop', 'pakistani pop', 'norwegian pop', 'punjabi hip hop', 'desi trap', 'urdu hip hop', 'punjabi pop', 'indian indie', 'pakistani electronic', 'desi hip hop', 'indian singer-songwriter'}

search_url = f'https://api.spotify.com/v1/search'


artist_names = []

# # Main process
access_token = get_access_token(client_id, client_secret)

# Get subgenres from starting artists.
start_artist_subgenres = []
for artist in start_artist_names:
    artist_id = search_artist(artist, access_token)
    this_start_artist_subgenres = get_artist_genres(artist_id, access_token)
    for s in this_start_artist_subgenres:
        start_artist_subgenres.append(s)

# Get unique subgenres.
start_artist_subgenres = list(set(start_artist_subgenres))

# Remove unwanted subgenres.
subgenres = [s for s in start_artist_subgenres if s not in exclude_subgenres]

# import IPython
# IPython.embed()

print(subgenres)

# To-do: go through all subgenres, aggregate all response data, remove repeated entries from all response data, dump names/scores/popularity metrics to csv file.
artist_data = []
for s in subgenres:
    a_list, responses = get_artist_data_from_subgenres(s, access_token)
    for a in a_list:
        # if a['name'] == 'Sudesh Kumari':
        #     import IPython
        #     IPython.embed()
        artist_data.append(a)


df = build_artist_df(artist_data)

unique_name, unique_name_idx = np.unique(df['name'], return_index=True)
unique_df = df.iloc[unique_name_idx]

popular_df = unique_df[unique_df.popularity > popularity_threshold]



# # Remove duplicate entries of artist data.
# unique_artist_data = remove_duplicates_from_artist_data(artist_data)

# # Filter by popularity score.
# popular_artist_data = filter_artists_by_popularity(unique_artist_data, popularity_threshold)

# # Code for saving all responses to json file. First remove unneeded fields (that aren't useful)
# # and save all the data to a json file.

# rm_fields = ['external_urls', 'href', 'images', 'type', 'uri']
# metadata_fields = ['name', 'id', 'genres']

# popular_artist_metadata = []

# for a in popular_artist_data:
#     for f in rm_fields:
#         del a[f]
#     metadata = {}
#     for m in metadata_fields:
#         metadata[m] = a[m]
#     popular_artist_metadata.append(metadata)


# genres_correct = True
# subgenres_check = subgenres
# for a in unique_artist_data:
#     this_genres_correct = np.any([g in subgenres_check for g in a['genres']])
#     print(a['name'], a['genres']) if not this_genres_correct else None
#     genres_correct = this_genres_correct & np.any([g in subgenres for g in a['genres']])


# # dfs = [pd.DataFrame(a) for a in unique_artist_data]
# # df = pd.concat(dfs)


# # Create 
# popularity_df_data = {a['name']:[a['popularity']] for a in popular_artist_data}
# followers_df_data = {a['name']:[a['followers']['total']] for a in popular_artist_data}

# data_timestamp = datetime.datetime.now()
# data_timestamp_str = data_timestamp.strftime("%m-%d-%Y, %H:%M:%S")

# popularity_df = pd.DataFrame.from_dict(popularity_df_data)
# popularity_df['time'] = [data_timestamp_str]
# followers_df = pd.DataFrame.from_dict(followers_df_data)
# followers_df['time'] = [data_timestamp_str]


# # Saving the objects:
# with open('objs.pkl', 'wb') as f: 
#     pickle.dump([a_list, responses], f)


# popularity_csv_file = 'popularity.csv'
# if os.path.isfile(popularity_csv_file):
#     all_popularity_df = pd.read_csv(popularity_csv_file)
#     all_popularity_df = pd.concat([all_popularity_df, popularity_df])
# else:
#     all_popularity_df = popularity_df
# col = all_popularity_df.pop("time")
# all_popularity_df.insert(0, col.name, col)
# all_popularity_df.to_csv(popularity_csv_file, index=False)

# followers_csv_file = 'followers.csv'
# if os.path.isfile(followers_csv_file):
#     all_followers_df = pd.read_csv(followers_csv_file)
#     all_followers_df = pd.concat([all_followers_df, followers_df])

# else:
#     all_followers_df = followers_df
# col = all_followers_df.pop("time")
# all_followers_df.insert(0, col.name, col)
# all_followers_df.to_csv(followers_csv_file, index=False)


# artist_metadata_file = 'artist_metadata.json'
# if os.path.isfile(artist_metadata_file):
#     with open(artist_metadata_file , "r") as f:
#         all_popular_artist_metadata = json.load(f)
#     all_popular_artist_metadata = all_popular_artist_metadata + popular_artist_data
#     all_popular_artist_metadata = remove_duplicates_from_artist_data(all_popular_artist_metadata)
# else:
#     all_popular_artist_metadata = popular_artist_metadata
# with open(artist_metadata_file , "w") as f:
#     json.dump(popular_artist_metadata, f, indent=4)


import IPython
IPython.embed()