import pandas as pd
from tempfile import TemporaryDirectory


def convert_json_to_csv(src_path: str, dst_path: str):
    df = pd.read_json(path_or_buf=src_path)
    df.to_csv(dst_path, sep=',')



convert_json_to_csv('/Users/dohayon/Projects/challenge/raw_data/input_data/events_sample.json',
                    '/Users/dohayon/Projects/challenge/raw_data/events/events.csv')

# df = pd.read_json(path_or_buf='/Users/dohayon/Projects/challenge/raw_data/input_data/events_sample.json')
