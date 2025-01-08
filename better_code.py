import pyarrow.csv as csv
import pyarrow.parquet as pq
import numpy as np
import logging
import os
import pandas as pd
import concurrent.futures
import glob
from typing import List

LOGGING_DIR = './logs/optimized_code/'

class CNST: 
    """
    This class saves the hard-coded values that were in code.py before refactoring. 
    Variable names could be changed according to logic
    """
    FILTER_THRESH = 42
    COL_NAME = 'value'
    NUM_0 = 0
    NUM_2 = 2
    NUM_10 = 10
    NUM_20 = 20
    NUM_50 = 50
    NUM_100 = 100
    NUM_200 = 200
    NUM_1K = 1000
    LOG_SEP = '*'*100

def init_logger( logging_dir: str, file_path: str) -> logging.Logger: 
        """ 
        - Initializes the logger to be used within the class. 
        - Logger format: 2025-01-05 15:35:12,379 - INFO - $message$
        """
        if not os.path.exists(logging_dir):
            os.makedirs(logging_dir)
        log_file_path = os.path.join(logging_dir,os.path.splitext(os.path.basename(file_path))[0]+'.log')
        logger = logging.getLogger(__name__)
        logging.basicConfig(filename= log_file_path , format='%(asctime)s - %(levelname)s - %(message)s',  level=logging.DEBUG)
        return logger


class ArrowDatasetManipulation:
    """
    A class for manipulating an Arrow dataset.
    Attributes:
        msg (str): Human readable string describing the exception.
        code (int): Exception error code.
        file_path (str): path for data file
        table (pd.DataFrame): loaded file data 
        completed_run (bool): if True, manipulation has completed
        indices_matching_filter (List[int]): saves indices of values that match filter
        generated_results_data (np.ndarray): saves results of data manipulation
    """

    def __init__(self, file_path: str):
        """ 
        - Initializes the class given the path for a data file
        - Supports files of format {csv, parquet}
        - The loaded data is saved in self.table

        Args:
            path (str): The path of the file to be loaded

        """
        self.file_path = file_path
        self.logger =  init_logger(LOGGING_DIR,self.file_path)
        self.table = self.read_data_to_df(self.file_path)
        self.completed_run = False
        self.indices_matching_filter = None
        self.generated_results_data = None


    

    def read_data_to_df(self, path: str) -> pd.DataFrame:
        """ 
        - Loads data from a given file path into a dataframe. 
        - The function can load csv files and parquet files.

        Args:
            path (str): The path of the file to be loaded

        Returns:
            (pd.DataFrame): A dataframe containing the loaded data from the given path
        """
        self.logger.info(f"Reading data from {path}")
        assert os.path.isfile

        try: 
            if path.endswith('.csv'):
                self.logger.info("File is a CSV!")
                data = csv.read_csv(path)
            elif path.endswith('.parquet'):
                self.logger.info("File is a Parquet!")
                data = pq.read_table(path)
            else: 
                self.logger.error("Unknown file type!")
                raise ValueError("Unknown file type!")
            data = data.to_pandas()
            return data
        except Exception as e:
                self.logger.error("Error reading file:", e)
                raise ValueError("Error reading file:", e)



    def filter_data(self, filter_threshold:int =42):
        """ 
        - Returns list of indices of data rows that are less than a given threshold.

        Args:
            filter_threshold (int): A number that represents the filter threshold value.

        Updates:
            self.indices_matching_filter: Class variable that is a list carrying indices
                                          of data less than specified threshold.
        """
        if not self.table.empty:
            self.logger.info(CNST.LOG_SEP)
            self.logger.info(f"Filtering data with a value less than {filter_threshold}")
            assert CNST.COL_NAME in self.table.columns
            self.indices_matching_filter = self.table[self.table[CNST.COL_NAME]<filter_threshold].index.to_list()
            return 
        
        self.logger.warning("No data to filter")
         

    def add_level_column(self):
        """ 
        - Appends to the dataset an extra column called "Level"
        - The value of Level column depends on the number stored in the column "value"
        - The Level column values can be: 
            __ "High": if value>100
            __ "Medium": if 50<value<=100
            __ "Low": for all other values

        Updates:
            self.table: The dataset table is updated to contain the added column, "Level".
        """
        self.logger.info(CNST.LOG_SEP)
        self.logger.info("Adding level column to the data.")
        if not self.table.empty :
            df = self.table
            assert 'Level' not in self.table.columns
            df['Level'] = ['Low'] * len(df)
            df.loc[((df[CNST.COL_NAME]>CNST.NUM_50)&(df[CNST.COL_NAME]<=CNST.NUM_100)), 'Level'] = 'Medium'
            df.loc[df[CNST.COL_NAME]>CNST.NUM_100, 'Level'] = 'High'
            self.table = df
            self.logger.info("Column added!")
            return 
        
        self.logger.warning("No data to modify.")

    def generate_first_part_of_results(self) -> np.ndarray :
        """ 
        - Creates the first chunck of the self.generated_results_data array.
        - Copies table data into df
        - Creates an extra column in df called 'result'
        - Results column is filled according to the following rules: 

        Case 1: data has more than 50 entriews
            __ value*2: if even indexed row and value>10
            __ value+100: if even indexed row and value<=10
            __ [value+1, value*3, value+1, value+1, value*3]: if odd indexed row and value<100
            __ [value-2, value*3, value-2, value-2, value*3]: if odd indexed row and value>100
            __ [value, value*3, value, value, value*3]: if odd indexed row and value=100
        Case 2: data has less than 50 entries
            __value*10 : if value<5
            __[value, value+1, ..., value+4]: if value>200
            __[value, value-1, ..., value-4]: if 5<=value<=200

        - All row entries stored in results column are then flattened into a numpy array that is the output of the function.
        
        Returns:
            (np.ndarray): a numpy array containing: All row entries stored in results column are then concatenated and flattened.
        """
        df = self.table
        if len(self.table[CNST.COL_NAME]) > CNST.NUM_50:
            # even indices
            df.loc[((df.index%2==0)&(df[CNST.COL_NAME]>10)), 'result'] = df[CNST.COL_NAME]*CNST.NUM_2
            df.loc[((df.index%2==0)&(df[CNST.COL_NAME]<=10)), 'result'] = df[CNST.COL_NAME]+CNST.NUM_100
            # odd indices
            df['temp'] = [np.array([1,0,1,1,0])]*len(df)
            df['temp2'] = [np.zeros(5)]*len(df)
            df.loc[((df.index % 2 != 0)&(df[CNST.COL_NAME]<CNST.NUM_100)), 'result'] = df[CNST.COL_NAME]+df['temp']
            df.loc[((df.index % 2 != 0)&(df[CNST.COL_NAME]>CNST.NUM_100)), 'result'] = df[CNST.COL_NAME]- CNST.NUM_2 *df['temp']
            df.loc[((df.index%2!=0)&(df[CNST.COL_NAME]==CNST.NUM_100)), 'result'] = df[CNST.COL_NAME]+df['temp2']
            
            df['result'] = df['result'].apply(lambda x:[x[0], x[1] * 3, x[2], x[3], x[4]*3]if 'arr' in str(type(x)) else x )
            return np.hstack(df['result'].to_numpy()).flatten()
        
        df['temp'] = [np.arange(5)]*len(df)
        df.loc[df[CNST.COL_NAME]<CNST.NUM_10, 'result'] = df[CNST.COL_NAME]*CNST.NUM_10
        df.loc[df[CNST.COL_NAME]>CNST.NUM_200, 'result'] = df[CNST.COL_NAME]+df['temp']
        df.loc[((5<=df[CNST.COL_NAME])&(df[CNST.COL_NAME]<=CNST.NUM_200)), 'result'] = df[CNST.COL_NAME]-df['temp']
        return np.hstack(df['result'].to_numpy()).flatten()
        
    def generate_second_part_of_results(self) -> np.ndarray:
        """ 
        - Creates the second chunck of the self.generated_results_data array.
        - Copies table data into df
        - Creates an extra column in df called 'result'
        - Results column is filled according to the following rules: 
            __ value+10: if even value and value<50
            __ value-10: if odd value and value<50
            __ value*2: if value>100
            __ [value+20, value-20]: otherwise
        - Results entries are repeated three times per row       
        - All row entries stored in results column are then flattened into a numpy array that is the output of the function.
        
        Returns:
            (np.ndarray): a numpy array containing: All row entries stored in results column are then concatenated and flattened.
        """
        df = self.table
        df.loc[((df[CNST.COL_NAME]<CNST.NUM_50) & (df[CNST.COL_NAME]%2==0)), 'result'] = df[CNST.COL_NAME]+ CNST.NUM_10
        df.loc[((df[CNST.COL_NAME]<CNST.NUM_50) & (df[CNST.COL_NAME]%2!=0)), 'result'] = df[CNST.COL_NAME]- CNST.NUM_10
        df.loc[(df[CNST.COL_NAME]>CNST.NUM_100), 'result'] = df[CNST.COL_NAME]*CNST.NUM_2

        df['temp'] = [np.array([CNST.NUM_20,-1*CNST.NUM_20])]*len(df)
        df.loc[((df[CNST.COL_NAME]>=CNST.NUM_50) & (df[CNST.COL_NAME]<=CNST.NUM_100)), 'result'] = df[CNST.COL_NAME]+df['temp']
        result = np.repeat(df['result'].to_numpy(),3)
    
        return np.hstack(result).flatten()


    def log_results_range_counts(self):
        """ 
        - Logs the count of results within each specified range
            __ counts large numbers detected where: result>1000
            __ counts small numbers detected where: result<10
            __ counts normal numbers detected where: the rest of results
        """
        
        large_values_count = np.sum(self.generated_results_data>CNST.NUM_1K)
        small_values_count = np.sum(self.generated_results_data<CNST.NUM_10)
        normal_values_count = self.generated_results_data.shape[0]-large_values_count-small_values_count
        self.logger.info(CNST.LOG_SEP)
        self.logger.info(f"The number of large values detected in the generated results array is {large_values_count}")
        self.logger.info(f"The number of small values detected in the generated results array is {small_values_count}")
        self.logger.info(f"The number of normal values detected in the generated results array is {normal_values_count}")
        self.logger.info(CNST.LOG_SEP)

        
    def generate_results_array(self):
        """ 
        - Generates the array which contains the results of data manipulation according to the logic in code.py
        - Obtains the values to fill the array from the two functions:
                 self.generate_first_part_of_results, self.generate_second_part_of_results
        Updates:
            self.generated_results_data (np.ndarray): array stores the manipulation results
        """
        if not self.table.empty:
            if len(self.table)>CNST.NUM_1K:
                self.logger.info(CNST.LOG_SEP)
                self.logger.warning(f"Data of {len(self.table)} entries is too large for any sensible manipulation.") 
                self.logger.info(CNST.LOG_SEP)

            results_1 = self.generate_first_part_of_results()
            results_2 = self.generate_second_part_of_results()
            self.generated_results_data = np.concatenate((results_1, results_2))

            self.log_results_range_counts() 
            return
        
        self.logger.warning("No data to manipulate!")



    def log_values_range_counts(self):
        """ 
        - Logs the count of values from the dataset within each specified range
            __ counts numbers where: value>10
            __ counts numbers where: 5<value<10
            __ counts numbers where: 0<value<5
            __ counts non-positive values.
        """
        if not self.table.empty:
            assert CNST.COL_NAME in self.table.columns
            #note: name it logging fucntoin,,, design for testability,, make table passable to function instead
            self.logger.info(f'There are {len(self.table[self.table["value"]>10])} values that are greater than 10!')
            self.logger.info(CNST.LOG_SEP)

            self.logger.info(f'There are {len(self.table[(5<self.table["value"])&(self.table["value"]<10)])} values that are greater than 5 but less than 10!')
            self.logger.info(CNST.LOG_SEP)

            self.logger.info(f'There are {len(self.table[(0<self.table["value"])&(self.table["value"]<5)])} values that are greater than 0 but less than 5!')
            self.logger.info(CNST.LOG_SEP)

            self.logger.info(f'There are {len(self.table[self.table["value"]<0])} non-positive values!')
            self.logger.info(CNST.LOG_SEP)
            return
        
        self.logger.warning("No data to check!")

    def run_manipulation_methods(self):
        """ 
        - Runs the methods in the class to perform the required list of manipulation steps.
        - Steps to be performed: 
            1. method, self.filter_data :  filters the data according to a threshold, and saves filter indices
            2. method, self.add_level_column :  appends level column to dataset
            3. method, self.generate_results_array :  generates the array that carries the manipulation results
        """
        if not self.table.empty:
            self.filter_data(filter_threshold=CNST.FILTER_THRESH)
            self.add_level_column()
            self.generate_results_array()
            self.log_values_range_counts()
            self.completed_run = True 
            return 
        self.logger.warning("No data to process!")
        self.completed_run = True 

    @staticmethod
    def process_dataset_in_parallel(files_dir: str, num_workers: int) -> List[List] :
        """
        This function parallelizes the code with multi-threading to manipulate multiple files in parallel 

        Args:
            files_dir (str): path to the directory that contains the files to be processed in parallel 
            num_workers (int): number of workers

        Returns: 
            (List[[bool, str]]): each entry contains a bool to flag manipulation failures along with the file path
        """
        # files_dir = './runtime_test_files/'
        files_paths = glob.glob(f'{files_dir}*')

        print(f'Running on {len(files_paths)} files.')
    

        def process_one_file(file_path):
            dataset_new = ArrowDatasetManipulation(file_path)
            dataset_new.run_manipulation_methods()
            return dataset_new.completed_run

        results_list = []

        with concurrent.futures.ThreadPoolExecutor(max_workers= num_workers) as executor:
            print('The maximum number of available workers is ', executor._max_workers)

            futures_dict = {executor.submit(process_one_file, file_path=file_path): file_path for file_path in files_paths}
            for future in concurrent.futures.as_completed(futures_dict):
                results_list.append([future.result(), futures_dict[future]])
        return results_list
       


if __name__ == "__main__":
    test_file_path = './runtime_test_files/test_file_size_100.csv'
    ArrowData = ArrowDatasetManipulation(test_file_path)
    ArrowData.run_manipulation_methods()
