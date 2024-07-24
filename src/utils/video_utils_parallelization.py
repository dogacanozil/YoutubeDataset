import scrapetube
from tqdm import tqdm
import pandas as pd
import gzip
import csv
from pytube import YouTube
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

def create_channel_video_relationship_list(channel_ids:list):
    channel_video_list =[]
    for channel_id in tqdm(channel_ids):
        try:
            videos = scrapetube.get_channel(channel_id)
            for video in videos:
                channel_video_list.append([channel_id, video['videoId']])
        except:
            channel_video_list.append([channel_id, "This channel does not exist."])
    return channel_video_list

def fetch_videos_for_channel(row):
    channel_video_list = []
    if row.is_active is True:
        video_count = 0
        channel_set = set()
        for video in scrapetube.get_channel(row.channel_id, limit=120):
            video_count += 1
        if video_count == 120:
            for sort_by in ['newest', 'oldest', 'popular']:
                videos = scrapetube.get_channel(row.channel_id, limit=40, sort_by=sort_by)
                for video in videos:
                    current_time = datetime.now()
                    if video['videoId'] not in channel_set:
                        video_object = YouTube(f"https://www.youtube.com/watch?v={video['videoId']}")
                        video_description = video.get("descriptionSnippet", {}).get("runs", [{}])[0].get("text", "").replace("\n", "")
                        channel_video_list.append([row.channel_id, video['videoId'], video_object.title, video_object.length, video_description, video_object.thumbnail_url, video_object.views, video_object.publish_date, current_time])
                        channel_set.add(video['videoId'])
        else:
            videos = scrapetube.get_channel(row.channel_id)
            for video in videos:
                current_time = datetime.now()
                video_object = YouTube(f"https://www.youtube.com/watch?v={video['videoId']}")
                video_description = video.get("descriptionSnippet", {}).get("runs", [{}])[0].get("text", "").replace("\n", "")
                channel_video_list.append([row.channel_id, video['videoId'], video_object.title, video_object.length, video_description, video_object.thumbnail_url, video_object.views, video_object.publish_date, current_time])
    return channel_video_list

def channels_videos_fetch_and_write_to_csv_gzipped(channel_ids_df:pd.DataFrame, output_address:str, output_file_name:str = None, write_size:int = 100, col_names:list = ['channel_id', 'video_id', 'video_title', 'video_length', 'video_description', 'video_thumbnail_url', 'video_views_count', 'video_publish_date', 'date_of_capture'], max_workers = 10):
    size = len(channel_ids_df['channel_id'])
    if output_file_name is None: output_file_name = f"{size}_channels_videos_list.csv.gz"

    csv_file_path = output_address + output_file_name

    with gzip.open(csv_file_path, mode='wt', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = []
        for i in range(0, size, write_size):
            batch = channel_ids_df[i:i+write_size]
            for row in batch.itertuples():
                futures.append(executor.submit(fetch_videos_for_channel, row))

        with gzip.open(csv_file_path, mode='at', newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            for future in as_completed(futures):
                result = future.result()
                writer.writerows(result)
