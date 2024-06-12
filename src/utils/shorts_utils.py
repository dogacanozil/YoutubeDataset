import scrapetube
from tqdm import tqdm
import pandas as pd


def create_channel_shorts_relationship_list(channel_ids:list):
    channel_video_list =[]
    for channel_id in tqdm(channel_ids):
        try:
            videos = scrapetube.get_channel(channel_id, content_type= "shorts")
            for video in videos:
                channel_video_list.append([channel_id, video['videoId']])
        except:
            channel_video_list.append([channel_id, "This channel does not exist."])
    return channel_video_list


def create_channel_shorts_relationship_list_limited_shorts(channel_ids_df:pd.DataFrame, threshold:int = 120):

    channel_video_list =[]
    for row in channel_ids_df.itertuples():
        if row.channel_name!='This channel does not exist.':
            video_count = 0
            channel_set = set()
            for video in scrapetube.get_channel(row.channel_id, limit=threshold, content_type= "shorts"):
                video_count=video_count+1
            if video_count == threshold:
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='newest', content_type= "shorts")
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='oldest', content_type= "shorts")
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
                videos = scrapetube.get_channel(row.channel_id, limit=threshold//3, sort_by='popular', content_type= "shorts")
                for video in videos:
                    if video['videoId'] not in channel_set:
                        channel_video_list.append([row.channel_id, video['videoId']])
                        channel_set.add(video['videoId'])
            else:
                videos = scrapetube.get_channel(row.channel_id, content_type= "shorts")
                for video in videos:
                    channel_video_list.append([row.channel_id, video['videoId']])

    return channel_video_list


        
