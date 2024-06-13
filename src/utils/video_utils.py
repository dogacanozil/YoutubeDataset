import scrapetube
from tqdm import tqdm
import pandas as pd
import gzip
import csv

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


def create_channel_video_relationship_list_limited_videos(channel_ids_df:pd.DataFrame, threshold:int = 120):

    channel_video_list =[]
    for row in channel_ids_df.itertuples():
        if row.channel_name!='This channel does not exist.':
            video_count = 0
            channel_set = set()
            for video in scrapetube.get_channel(row.channel_id, limit=threshold):
                video_count=video_count+1
            if video_count == threshold:
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='newest')
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='oldest')
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='popular')
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
            else:
                videos = scrapetube.get_channel(row.channel_id)
                for video in videos:
                    channel_video_list.append([row.channel_id, video['videoId']])

    return channel_video_list



def channels_videos_fetch_and_write_to_csv_gzipped(channel_ids_df:pd.DataFrame, output_address:str, output_file_name:str = None, write_size:int = 100, col_names:list = ['channel_id', 'video_id']):
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


        
