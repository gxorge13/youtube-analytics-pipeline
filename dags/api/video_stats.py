import requests
import json

# import os
# from dotenv import load_dotenv
# load_dotenv(dotenv_path='./.env')


from datetime import date
from airflow.decorators import task
from airflow.models import Variable


MAX_VIDS = 50
REQUEST_TIMEOUT = 30


def youtube_config():
    """Load credentials at task runtime instead of during DAG discovery."""
    return Variable.get("API_KEY"), Variable.get("CHANNEL_HANDLE")



@task
def get_playlist_ID():
    api_key, channel = youtube_config()
    try:
        url = 'https://youtube.googleapis.com/youtube/v3/channels'
        response = requests.get(
            url,
            params={"part": "contentDetails", "forHandle": channel, "key": api_key},
            timeout=REQUEST_TIMEOUT,
        )

        response.raise_for_status() # if not code 2XX, raises an exception

        data = response.json() # turns a json into nested dictionaries
        # print(json.dumps(data, indent=4)) # python object, into a json formatted string. So it can be readable 

        playlist_ID = data['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        # print (playlist_ID)
        return playlist_ID
    except requests.exceptions.RequestException as e: #being specific on the bug that made it quit
        raise e

@task
def get_video_IDs(playlist):
    api_key, _ = youtube_config()
    base_url = 'https://youtube.googleapis.com/youtube/v3/playlistItems'
    video_ids = []
    try:
        params = {
            "part": "contentDetails",
            "maxResults": MAX_VIDS,
            "playlistId": playlist,
            "key": api_key,
        }
        response = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        data = response.json()

        for item in data['items']:
            video_ids.append(item['contentDetails']['videoId'])

        page_token = data.get('nextPageToken')

        while page_token is not None:
            params["pageToken"] = page_token
            response = requests.get(base_url, params=params, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()
            data = response.json()

            for item in data['items']:
                video_ids.append(item['contentDetails']['videoId'])

            page_token = data.get('nextPageToken')
            
        return video_ids
    except requests.exceptions.RequestException as e:
        raise e

@task
def extract_video_stats(video_ids):
    api_key, _ = youtube_config()
    
    def batch_list(video_ids, batch_size = 50):
        """Yield successive n-sized chunks from l."""
        for i in range(0, len(video_ids), batch_size):
            yield video_ids[i:i + batch_size] # yield returns a generator
        


    extracted_data = []
#   

    try:
        batches = batch_list(video_ids, 50)
        for batch in batches:
            # print(batch) # a list of 50 video IDs
            batch = ','.join(batch) # turn list into a string separated by commas
            # break
            
            stats = 'https://youtube.googleapis.com/youtube/v3/videos'
            response = requests.get(
                stats,
                params={
                    "part": "contentDetails,snippet,statistics",
                    "id": batch,
                    "maxResults": MAX_VIDS,
                    "key": api_key,
                },
                timeout=REQUEST_TIMEOUT,
            )
            
            response.raise_for_status()
            
            data = response.json()
            
            '''
            Want 6 things
             	• published at 
                • title
                • duration
                • views
                • likes
                • comment count
            '''
            for item in data['items']:
                
                video_id = item['id']
                
                published_at = item['snippet']['publishedAt']
                title = item['snippet']['title']
                
                duration = item['contentDetails']['duration']
                
                views = item['statistics'].get('viewCount', None) # some videos have views disabled
                likes = item['statistics'].get('likeCount', None) # some videos have likes disabled
                comment_count = item['statistics'].get('commentCount', None) # some videos have comments disabled
                video_data = {
                    'video_id': video_id,
                    'published_at': published_at,
                    'title': title,
                    'duration': duration,
                    'views': views,
                    'likes': likes,
                    'comment_count': comment_count
                }
                extracted_data.append(video_data)
            
        return extracted_data
            
    except requests.exceptions.RequestException as e:
        raise e
    
@task
def save_to_json(stats):
    file_path = f'./data/YT_data_{date.today()}.json'
    with open(file_path, 'w', encoding='utf-8') as f: # create folder if doesnt exist
        json.dump(stats, f, indent=4, ensure_ascii=False) # writes into 'stats' file f

if __name__ == '__main__': # if ran directily
    playlist = get_playlist_ID()
    video_ids = get_video_IDs(playlist)
    stats = extract_video_stats(video_ids)
    save_to_json(stats)
    '''
    stats is of this shape:
    [
        ...
        {
        "video_id": "DZIASl9q90s",
        "published_at": "2025-05-24T16:00:01Z",
        "title": "Beat Neymar, Win $500,000",
        "duration": "PT27M53S",
        "views": "154654254",
        "likes": "3516090",
        "comment_count": "80511"
    },
    {
        "video_id": "7qY-qalCI2Y",
        "published_at": "2025-05-21T15:59:50Z",
        "title": "Grab The Rolex, Keep It!",
        "duration": "PT23S",
        "views": "192839315",
        "likes": "2940111",
        "comment_count": "8921"
        }
    ]
    '''
    stat_json = json.dumps(stats, indent=4) # python object into a json formatted string
    
    
