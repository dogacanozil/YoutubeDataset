import sys
import utils.channel_utils as cu
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def fetch_channels(start_index, fetch_size_per_iteration, iteration):

    channel_ids = pd.read_csv('dataset/input_dataset/channel_ids.csv.gz', compression='gzip')
    sample_channel_subset_ids = channel_ids['channel_id'].to_list()

    start_index = start_index
    for i in range(iteration):
        from_index = start_index + (i * fetch_size_per_iteration)
        to_index = from_index+fetch_size_per_iteration
        print(f"Fetch from: {from_index} ; to: {to_index}")
        cu.channels_fetch_and_write_to_csv_gzipped(sample_channel_subset_ids, output_address= "dataset/output_channel_dataset/", from_index=from_index, until_index=to_index)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python fetch_channels <start_index> <fetch_size_per_iteration> <fetch_size_per_iteration>")
        print("Example for from index 0 to 10,000 in 1 iteration")
    else:
        start_index = int(sys.argv[1])
        fetch_size_per_iteration = int(sys.argv[2])
        iteration = int(sys.argv[3])
        fetch_channels(start_index, fetch_size_per_iteration, iteration)
