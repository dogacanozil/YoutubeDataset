import sys
import utils.video_utils as vu
import pandas as pd
import ssl
ssl._create_default_https_context = ssl._create_unverified_context

def fetch_videos_channels(from_index, to_index):
    input_df = pd.read_csv(f'dataset/output_channel_dataset/from_{from_index}_until_{to_index}_channels_list.csv.gz', compression='gzip')

    vu.channels_videos_fetch_and_write_to_csv_gzipped(
        channel_ids_df = input_df,
        output_address= "dataset/output_videos_channels_dataset/",
        output_file_name= f"videos_from_{from_index}_until_{to_index}_channels.csv.gz"
    )

if __name__ == "__main__":
    if int(sys.argv[1]) is None:
        print("from_index can not be None")
        raise Exception
    else:
        from_index = int(sys.argv[1])

    if int(sys.argv[2]) is None:
        print("to_index can not be None")
        raise Exception
    else:
        to_index = int(sys.argv[2])

    if int(sys.argv[3]) is None:
        print("iterations can not be None")
        raise Exception
    else:
        iterations = int(sys.argv[3])

        for i in range(iterations):
            print(f"Now it is iteration:{i+1}, it is fetching between {from_index+(i*10000)} and {to_index+(i*10000)}")
            fetch_videos_channels(from_index+(i*10000), to_index+(i*10000))
