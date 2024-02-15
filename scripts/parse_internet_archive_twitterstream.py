# -*- coding: utf-8 -*-
"""
Reads JSONL Twitter archive files from Internet Archive for a
single day and exports geocoded records to a new JSON file

https://archive.org/details/twitterstream

Frank Donnelly GIS and Data Librarian
Brown University Library
April 26, 2023 / Rev Sept 28, 2023
"""
import os,json, csv
from datetime import datetime

#INPUT_DIR is the directory that contains a day's worth of files
#Make sure you unzipped them first! This directory should be below the
#directory where this script is stored:

INPUT_DIR='data/internet-archive-data/2021'

OUTPUT_DIR='data/tweets'

TWEET_OUTPUT_FID_SUFFIX = '_tweets.csv'

FIELDS = [
    'tweet_id',
    'timestamp',
    'text'
    'source_url',
    'source_name',
    'lang',
    'lon',
    'lat',
    'country',
    'ccode',
    'place_short',
    'user_id',
]

def process_tweet(tweet_data):
    # process the tweet dict to return a dict of normalized values 
    tweet_id = tweet_data['id']
    timestamp=tweet_data.get('created_at')
    text=tweet_data.get('text')
    # Source is in HTML with anchors. Separate the link and source name
    source=tweet_data.get('source') # This is in HTML
    if source !='':
        source_url=source.split('"')[1] # This gets the url
        source_name=source.strip('</a>').split('>')[-1] # This gets the name
    else:
        source_url=None
        source_name=None
    lang=tweet_data.get('lang')
    # Value for long / lat is stored in a list, must specify position
    if tweet_data['geo'] !=None:
        longitude=tweet_data.get('geo').get('coordinates')[1]
        latitude=tweet_data.get('geo').get('coordinates')[0]
    else:
        longitude=None
        latitude=None
    if tweet_data['place'] !=None:
        country=tweet_data['place'].get('country')
        ccode=tweet_data.get('place').get('country_code')
        place_short=tweet_data.get('place').get('name')
        place_full=tweet_data.get('place').get('full_name')
    else: # Necessary to avoid errors if there are no place elements
        country=None
        ccode=None
        place_short=None
        place_full=None
    user_id=tweet_data.get('user').get('id')  

    # Format tweet data
    normalized_data = {
        'tweet_id':tweet_id,
        'timestamp':timestamp,
        'text':text,
        'source_url':source_url,
        'source_name':source_name,
        'lang':lang,
        'lon':longitude,
        'lat':latitude,
        'country':country,
        'ccode':ccode,
        'place_short':place_short,
        'user_id': user_id,
    }
    return normalized_data


def write_dict_list_to_csv(data):

    # Format timestamp for file id
    current_datetime = datetime.now()
    current_timestamp = current_datetime.strftime('%Y%m%d%H%M%S')

    csv_file_path = f'{OUTPUT_DIR}/{current_timestamp}{TWEET_OUTPUT_FID_SUFFIX}'

    # Extract the keys (column names) from the first dictionary
    fields = data[0].keys()

    # Write the data to the CSV file
    with open(csv_file_path, 'w', newline='') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fields)

        # Write the header row
        writer.writeheader()

        # Write the data rows
        for row in data:
            writer.writerow(row)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

tweet_json_paths = []

for path, dirs, files in os.walk(INPUT_DIR):
    for f in files:
        if f.endswith('.json'):
            json_path=os.path.join(path,f)
            tweet_json_paths.append(json_path)

n_tweet_json_paths = len(tweet_json_paths)

tweets = []

for i, json_path in enumerate(tweet_json_paths):
    with open(json_path,'r',encoding='utf-8') as json_f:
        for json_str in json_f:
            raw_tweet_dict = json.loads(json_str) # convert string to dict
            if 'id' in raw_tweet_dict: # skip tweets that do not have ids (these are deleted)
                htags = raw_tweet_dict['entities']['hashtags'] 
                if htags and raw_tweet_dict['lang'] == 'en':
                    print(htags)
                    print(raw_tweet_dict.keys())
                    exit()
                    

                tweets.append(process_tweet(raw_tweet_dict))
    if i%100 == 0:
        write_dict_list_to_csv(tweets)
    print(f'Processed {i} / {n_tweet_json_paths} Tweet Json Files.')
    print(f'Total tweets processed: {len(tweets)}')
write_dict_list_to_csv(tweets)
