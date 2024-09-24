import scrapetube
from tqdm import tqdm
import pandas as pd
import gzip
import csv
from datetime import datetime
from utils.string_utils import convert_time_to_seconds, convert_views_to_int
import time
import requests
import logging

def add_channel_video_list_per_channel_id_to_list(channel_id, threshold):
    channel_video_list = []
    video_count = 0
    video_Set = set()
    for video in scrapetube.get_channel(channel_id, limit=threshold):
        video_count=video_count+1
    if video_count == threshold:
        channel_video_list.extend(add_videos_to_list_per_channel_id_per_type(channel_id, threshold//3, 'popular', video_Set))
        channel_video_list.extend(add_videos_to_list_per_channel_id_per_type(channel_id, threshold//3, 'newest', video_Set))
        channel_video_list.extend(add_videos_to_list_per_channel_id_per_type(channel_id, threshold//3, 'oldest', video_Set))
    else:
        channel_video_list.extend(add_videos_to_list_per_channel_id_per_type(channel_id, threshold, 'newest', video_Set))
    return channel_video_list

def add_videos_to_list_per_channel_id_per_type(channel_id, limit, sort_by, video_set):
    video_list = []
    videos = scrapetube.get_channel(channel_id, limit=limit, sort_by=sort_by)
    for video in videos:
        current_time = datetime.now()
        if 'thumbnailOverlays' in video:
            if 'thumbnailOverlayTimeStatusRenderer' in video["thumbnailOverlays"][0]:
                if video['videoId'] not in video_set and video["thumbnailOverlays"][0]["thumbnailOverlayTimeStatusRenderer"]["style"]!='LIVE':
                    
                    if "descriptionSnippet" in video:
                        video_description = video["descriptionSnippet"]["runs"][0]["text"].replace("\n", "")
                    else:
                        video_description = ""
                        
                    if "upcomingEventData" in video:
                        view_count = None
                        publish_date = None
                    elif 'viewCountText' not in video:
                        view_count = None
                        publish_date = video['publishedTimeText']['simpleText']
                    elif video['viewCountText']['simpleText']=="No views":
                        view_count = 0
                        publish_date = video['publishedTimeText']['simpleText']
                    else:
                        view_count = convert_views_to_int(video['viewCountText']['simpleText'])
                        publish_date = video['publishedTimeText']['simpleText']
                        
                    if 'title' in video:
                        if 'runs' in video['title']:
                            video_title = video['title']['runs'][0]['text']
                        elif 'simpleText' in video['title']:
                            video_title = video['title']['simpleText']
                    else:
                        video_title = None

                    if 'lengthText' in video:
                        video_length = convert_time_to_seconds(video['lengthText']['simpleText'])
                    else:
                        video_length = None
                        
                    video_list.append([channel_id, video['videoId'], video_title, video_length, video_description, video['thumbnail']['thumbnails'][0]['url'], view_count, publish_date, current_time])
                    video_set.add(video['videoId'])
    return video_list



def create_channel_video_relationship_list_limited_videos(channel_ids_df:pd.DataFrame, threshold:int = 60, counter:int = 0):

    channel_video_list =[]
    channel_index = 0
    
    for row in tqdm(channel_ids_df.itertuples()):
        channel_index = channel_index+1
        if row.is_active is True:
            while True:
                try:
                    channel_video_list.extend(add_channel_video_list_per_channel_id_to_list(row.channel_id, threshold))
                    break

                except requests.exceptions.RequestException as e:
                    if e.response.status_code == 429:
                        print(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        logging.error(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        logging.error(f"System will wait 1 hour to try again.")
                        time.sleep(3600)
                    elif e.response.status_code == 404:
                        print(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        logging.error(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        logging.error(f"System will wait 1 hour to try again.")
                        time.sleep(3600)
                    else:
                        print(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        logging.error(f"Error occurred: {e}, channel_id: {row.channel_id}")
                        channel_video_list.append([row.channel_id, None, None, None, None, None, None, None, datetime.now()])
                        break
                except e: 
                    print(f"Error occurred: {e}, channel_id: {row.channel_id}")
                    logging.error(f"Error occurred: {e}, channel_id: {row.channel_id}")
                    channel_video_list.append([row.channel_id, None, None, None, None, None, None, None, datetime.now()])
                    break
    return channel_video_list



def channels_videos_fetch_and_write_to_csv_gzipped(channel_ids_df:pd.DataFrame, output_address:str, output_file_name:str = None, write_size:int = 100, col_names:list = ['channel_id', 'video_id', 'video_title', 'video_length', 'video_description', 'video_thumbnail_url', 'video_views_count', 'video_publish_date', 'date_of_capture']):
    size = len(channel_ids_df['channel_id'])
    if output_file_name is None: output_file_name = f"{size}_channels_videos_list.csv.gz"

    csv_file_path=output_address+output_file_name

    log_file_name = output_file_name.replace(".csv.gz", ".log")
    logging.basicConfig(
        filename= log_file_name,            # Log file name
        filemode='w',                  # Write mode ('w' for overwrite, 'a' for append)
        format='%(asctime)s - %(levelname)s - %(message)s',  # Log format
        level=logging.DEBUG            # Log level (DEBUG captures everything)
    )

    logging.debug("This is a debug message")
    logging.info("This is an info message")
    logging.warning("This is a warning message")
    logging.error("This is an error message")
    logging.critical("This is a critical message")

    with gzip.open(csv_file_path, mode='wt', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)

    with gzip.open(csv_file_path, mode='at', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        i=0
        while (i+1) * write_size < size:
            writer.writerows(create_channel_video_relationship_list_limited_videos(channel_ids_df[(i*write_size):((i+1)*write_size)]))
            i=i+1
            print((i) * write_size)
        
        writer.writerows(create_channel_video_relationship_list_limited_videos(channel_ids_df[(i*write_size):size]))


        
