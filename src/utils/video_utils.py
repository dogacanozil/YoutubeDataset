import scrapetube
from tqdm import tqdm
import pandas as pd
import gzip
import csv
from datetime import datetime
from utils.string_utils import convert_time_to_seconds, convert_views_to_int

def add_videos_to_list_per_channel_id(channel_id, limit, sort_by, video_set, video_list):

    videos = scrapetube.get_channel(channel_id, limit=limit, sort_by=sort_by)
    for video in videos:
        current_time = datetime.now()
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
                    
                video_list.append([channel_id, video['videoId'], video_title, convert_time_to_seconds(video['lengthText']['simpleText']), video_description, video['thumbnail']['thumbnails'][0]['url'], view_count, publish_date, current_time])
                video_set.add(video['videoId'])



def create_channel_video_relationship_list_limited_videos(channel_ids_df:pd.DataFrame, threshold:int = 60):

    channel_video_list =[]
    channel_index = 0
    for row in channel_ids_df.itertuples():
        print(channel_index)
        channel_index = channel_index+1
        if row.is_active is True:
            video_count = 0
            channel_set = set()
            for video in scrapetube.get_channel(row.channel_id, limit=threshold):
                video_count=video_count+1
            if video_count == threshold:
                add_videos_to_list_per_channel_id(row.channel_id, threshold//3, 'popular', channel_set, channel_video_list)
                add_videos_to_list_per_channel_id(row.channel_id, threshold//3, 'newest', channel_set, channel_video_list)
                add_videos_to_list_per_channel_id(row.channel_id, threshold//3, 'oldest', channel_set, channel_video_list)
            else:
                add_videos_to_list_per_channel_id(row.channel_id, threshold, 'newest', channel_set, channel_video_list)

    return channel_video_list



def channels_videos_fetch_and_write_to_csv_gzipped(channel_ids_df:pd.DataFrame, output_address:str, output_file_name:str = None, write_size:int = 100, col_names:list = ['channel_id', 'video_id', 'video_title', 'video_length', 'video_description', 'video_thumbnail_url', 'video_views_count', 'video_publish_date', 'date_of_capture']):
    size = len(channel_ids_df['channel_id'])
    if output_file_name is None: output_file_name = f"{size}_channels_videos_list.csv.gz"

    csv_file_path=output_address+output_file_name

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


        
