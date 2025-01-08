"""
This is for generating dummy data in CSV format for testing purposes
"""


import numpy as np
import pandas as pd
import os


if __name__ == "__main__":

    max_rows_per_file = 1000

    min_value = -10
    max_value = 200

    column_names = ['value']

    save_dir = './runtime_test_files/'
    save_dir_parquet = './runtime_test_files_parquet/'

    os.makedirs(save_dir, exist_ok=True)
    os.makedirs(save_dir_parquet, exist_ok=True)

    number_of_files = 101

    len_files = np.arange(number_of_files)*100
    
    for length in len_files:
        values = np.random.randint(min_value, max_value, length)
        df = pd.DataFrame(data = values, columns = column_names)
        df.to_csv(f'{save_dir}test_file_size_{length}.csv', index=False)
        df.to_parquet(f'{save_dir_parquet}test_file_size_{length}.parquet', index=False)
