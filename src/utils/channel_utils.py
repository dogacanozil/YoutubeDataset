from utils.string_utils import find_between
from pytube import Channel
import json
import gzip
from datetime import datetime
import csv
from tqdm import tqdm
import requests
import urllib.request
import urllib.error
import logging
import time


def create_channel_record_as_list_by_channel_id(channel_id:str, counter:int = 0):
    current_time = datetime.now()
    channel_object = Channel(f"https://www.youtube.com/channel/{channel_id}")

    channel_name = channel_object.channel_name
    channel_about_html = str(channel_object.about_html)
    channels_links_as_string = find_between(channel_about_html, ',"links":', ',"displayCanonicalChannelUrl":')
    if channels_links_as_string == '':
        channels_links_as_list = {}
    else:
        channels_links_as_list = json.loads(channels_links_as_string)

    channels_links_per_channel = []
    for channel in channels_links_as_list:
        channels_links_per_channel.append(f"{channel['channelExternalLinkViewModel']['title']['content']}: {channel['channelExternalLinkViewModel']['link']['content']}")

    channel_subscriber_count = find_between(channel_about_html, '"subscriberCountText":"', ' subscribers')
    if find_between(channel_about_html, ',"videoCountText":"', ' videos",').replace(',' , '') != "": 
        channel_video_count = int(find_between(channel_about_html, ',"videoCountText":"', ' videos",').replace(',' , ''))
    else:
        channel_video_count = 0
    
    if find_between(channel_about_html, ',"viewCountText":"', ' views",').replace(',' , '') != "": 
        channel_view_count = int(find_between(channel_about_html, ',"viewCountText":"', ' views",').replace(',' , ''))
    else:
        channel_view_count = 0
    channel_description = find_between(channel_about_html, 'name="description" content="', '"><meta name="keywords"')
    channel_creation_date = datetime.strptime(find_between(channel_about_html, 'joinedDateText":{"content":"Joined ', '","styleRuns"').replace(',' , ''), "%b %d %Y")

    channel_country = find_between(channel_about_html, '"country":"', '","')
    is_active = True

    return [channel_id, channel_name, channel_description, channels_links_per_channel, channel_video_count, channel_view_count, channel_subscriber_count, channel_country , channel_creation_date, current_time, is_active]

 
def create_channel_records_list_list(channel_ids:list):
    channels_list = []
    for channel_id in tqdm(channel_ids):
        while True:
            try:
                channel_record = create_channel_record_as_list_by_channel_id(channel_id)
                channels_list.append(channel_record)
                break

            except requests.exceptions.RequestException as e:
                if e.response.status_code == 429:
                    print(f"Error occurred: {e}, channel_id: {channel_id}")
                    logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                    logging.error(f"System will wait 1 hour to try again.")
                    time.sleep(3600)
                elif e.response.status_code == 404:
                    print(f"Error occurred: {e}, channel_id: {channel_id}")
                    logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                    logging.error(f"System will wait 1 hour to try again.")
                    time.sleep(3600)
                else:
                    print(f"Error occurred: {e}, channel_id: {channel_id}")
                    logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                    channels_list.append([channel_id, None, None, None, None, None, None, None , None, datetime.now(), None])
                    break

            except urllib.error.HTTPError as e:
                print(f"Error occurred: {e}, channel_id: {channel_id}")
                logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                if e.code == 404:
                    channels_list.append([channel_id, "", "", "", "", "", "", "" , "", datetime.now(), False])
                    break
                else:
                    channels_list.append([channel_id, None, None, None, None, None, None, None , None, datetime.now(), None])
                    break

            except KeyError as e:
                print(f"Error occurred: {e}, channel_id: {channel_id}")
                logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                if "'metadata'" == str(e):
                    channels_list.append([channel_id, "", "", "", "", "", "", "" , "", datetime.now(), False])
                    break
                else:
                    channels_list.append([channel_id, None, None, None, None, None, None, None , None, datetime.now(), None])
                    break
                
            except e: 
                print(f"Error occurred: {e}, channel_id: {channel_id}")
                logging.error(f"Error occurred: {e}, channel_id: {channel_id}")
                channels_list.append([channel_id, None, None, None, None, None, None, None , None, datetime.now(), None])
                break

    return channels_list


def channels_fetch_and_write_to_csv_gzipped(channel_ids:list, output_address:str, output_file_name:str = None, write_size:int = 1000, col_names= None, from_index = None, until_index = None):

    if col_names is None: col_names = ['channel_id', 'channel_name', 'channel_description', 'channel_links', 'channel_video_count', 'channel_view_count', 'channel_subscriber_count', 'channel_country',  'channel_creation_date', 'date_of_capture', 'is_active']
    if from_index is None: from_index = 0
    if until_index is None: until_index = len(channel_ids)

    channel_ids = channel_ids[from_index:until_index]

    size = len(channel_ids)
    if output_file_name is None:
        output_file_name = f"from_{from_index}_until_{until_index}_channels_list.csv.gz"

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
            writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):((i+1)*write_size)]))
            i=i+1
            print((i) * write_size)
        
        writer.writerows(create_channel_records_list_list(channel_ids[(i*write_size):size]))
