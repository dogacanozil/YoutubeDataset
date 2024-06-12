import csv
import gzip

def write_json_to_csv(list_json, output_file_name, col_names:list = ['channel_id', 'channel_url', 'channel_name', 'channel_description', 'channel_links', 'channel_subscriber_count', 'date_of_capture']):

    with open(output_file_name, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames = col_names, delimiter= '\u0001')
        for row in list_json:
            writer.writerow(row)


def write_list_to_csv(list_json, output_file_name, col_names:list = ['channel_id', 'video_id']):

    with open(f"{output_file_name}.csv", mode='w', newline='') as file:
        writer = csv.writer(file, delimiter= '\u0001')
        writer.writerow(col_names)
        writer.writerows(list_json)


def write_list_to_csv_zipped(list_json, output_file_name, col_names:list = ['channel_id', 'video_id']):

    with gzip.open(f"{output_file_name}.csv.gz", mode='wt', newline='', encoding='utf-8') as file:
        writer = csv.writer(file, delimiter= '\u0001')
        writer.writerow(col_names)
        writer.writerows(list_json)