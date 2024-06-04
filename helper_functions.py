from utils import find_between
from pytube import Channel
import scrapetube
import json
import gzip
from datetime import datetime
import csv
from tqdm import tqdm


def create_channel_record_as_json_by_channel_id(channel_id:str):

    current_time = datetime.now()
    channel_object = Channel(f"https://www.youtube.com/channel/{channel_id}")
    
    try:
        channel_name = channel_object.channel_name
    except:
        return {
            "channel_id" : channel_id,
            "channel_url" : "This channel does not exist.",
            "channel_name" : "This channel does not exist.",
            "channel_description" : "This channel does not exist.",
            "channel_links" : "This channel does not exist.",
            "channel_subscriber_count" : "This channel does not exist.",
            "date_of_capture" : "This channel does not exist.",
        }
    
    channel_about_html = str(channel_object.about_html)

    channels_links_as_string = find_between(channel_about_html, ',"links":', ',"displayCanonicalChannelUrl":')

    try:
        channels_links_as_list = json.loads(channels_links_as_string)
    except:
        channels_links_as_list = {}

    channels_links_per_channel = []
    for channel in channels_links_as_list:
        channels_links_per_channel.append(f"{channel['channelExternalLinkViewModel']['title']['content']}: {channel['channelExternalLinkViewModel']['link']['content']}")

    channel_subscriber = find_between(channel_about_html, '"subscriberCountText":"', ' subscribers')


    return {
        "channel_id" : channel_id,
        "channel_url" : f"https://www.youtube.com/channel/{channel_id}",
        "channel_name" : channel_name,
        "channel_description" : find_between(channel_about_html, 'name="description" content="', '"><meta name="keywords"'),
        "channel_links" : channels_links_per_channel,
        "channel_subscriber_count" : channel_subscriber,
        "date_of_capture" : current_time
    }


def create_channel_record_as_list_by_channel_id(channel_id:str):
    current_time = datetime.now()
    channel_object = Channel(f"https://www.youtube.com/channel/{channel_id}")
    
    try:
        channel_name = channel_object.channel_name
    except:
        return [channel_id, "This channel does not exist.", "This channel does not exist.", "This channel does not exist.", "This channel does not exist.", "This channel does not exist.", "This channel does not exist."]
    
    channel_about_html = str(channel_object.about_html)
    channels_links_as_string = find_between(channel_about_html, ',"links":', ',"displayCanonicalChannelUrl":')
    try:
        channels_links_as_list = json.loads(channels_links_as_string)
    except:
        channels_links_as_list = {}

    channels_links_per_channel = []
    for channel in channels_links_as_list:
        channels_links_per_channel.append(f"{channel['channelExternalLinkViewModel']['title']['content']}: {channel['channelExternalLinkViewModel']['link']['content']}")

    channel_subscriber = find_between(channel_about_html, '"subscriberCountText":"', ' subscribers')
    channel_description = find_between(channel_about_html, 'name="description" content="', '"><meta name="keywords"')


    return [channel_id, f"https://www.youtube.com/channel/{channel_id}", channel_name, channel_description, channels_links_per_channel, channel_subscriber, current_time]


def create_channel_records_list_list(channel_ids:list):
    return [create_channel_record_as_list_by_channel_id(channel_id) for channel_id in tqdm(channel_ids)]


def create_channel_records_list_json(channel_ids:list):
    return [create_channel_record_as_json_by_channel_id(channel_id) for channel_id in tqdm(channel_ids)]


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


def write_json_to_csv(list_json, output_file_name, col_names:list = ['channel_id', 'channel_url', 'channel_name', 'channel_description', 'channel_links', 'channel_subscriber_count', 'date_of_capture']):

    with open(output_file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames = col_names, delimiter= '\u0001')
        for row in list_json:
            writer.writerow(row)


def write_list_to_csv(list_json, output_file_name, col_names:list = ['channel_id', 'video_id']):

    with open(output_file_name, mode='w', newline='') as file:
        writer = csv.writer(file, delimiter= '\u0001')
        writer.writerow(col_names)
        writer.writerows(list_json)


def channels_fetch_and_write_to_csv(channel_ids:list, output_address:str, output_file_name:str = None, write_size:int = 1000, col_names:list = ['channel_id', 'channel_url', 'channel_name', 'channel_description', 'channel_links', 'channel_subscriber_count', 'date_of_capture']):
    size = len(channel_ids)
    if output_file_name is None: output_file_name = f"{size}_channels_list.csv"

    csv_file_path=output_address+output_file_name

    with open(csv_file_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)

    with open(csv_file_path, mode='a', newline='') as file:
        writer = csv.writer(file)

        i=0
        while (i+1) * write_size < size:
            writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):((i+1)*write_size)]))
            i=i+1
            print((i) * write_size)
        
        writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):size]))


def channels_fetch_and_write_to_csv_gzipped(channel_ids:list, output_address:str, output_file_name:str = None, write_size:int = 1000, col_names:list = ['channel_id', 'channel_url', 'channel_name', 'channel_description', 'channel_links', 'channel_subscriber_count', 'date_of_capture']):
    size = len(channel_ids)
    if output_file_name is None: output_file_name = f"{size}_channels_list.csv.gz"

    csv_file_path=output_address+output_file_name

    with gzip.open(csv_file_path, mode='wt', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        writer.writerow(col_names)

    with gzip.open(csv_file_path, mode='at', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        i=0
        while (i+1) * write_size < size:
            writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):((i+1)*write_size)]))
            i=i+1
            print((i) * write_size)
        
        writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):size]))


def channels_fetch_and_write_to_csv_gzipped_continue_from_index(channel_ids:list, output_address:str, index, output_file_name:str = None, write_size:int = 1000, col_names:list = ['channel_id', 'channel_url', 'channel_name', 'channel_description', 'channel_links', 'channel_subscriber_count', 'date_of_capture']):
    
    size = len(channel_ids)
    if output_file_name is None: output_file_name = f"{size}_channels_list.csv.gz"

    csv_file_path=output_address+output_file_name

    with gzip.open(csv_file_path, mode='at', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)

        i=0
        while index+((i+1) * write_size) < size:
            writer.writerows(create_channel_records_list_list(channel_ids[(index+i*write_size):(index+(i+1)*write_size)]))
            i=i+1
            print(index+(i) * write_size)
        
        writer.writerows(create_channel_records_list_list(channel_ids[index+(i*write_size):size]))



        
