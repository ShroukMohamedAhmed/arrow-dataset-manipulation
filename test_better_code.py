import unittest
import better_code
import code_1
import time
import glob
import numpy as np

class TestBetterCode(unittest.TestCase):

    def test_execution_time(self):
        """
        Testing the decrease in the execution time of new code vs older code.
        """
        file_path = 'runtime_test_files/test_file_size_5000.csv'
        start_time = time.time()
        dataset_old = code_1.BadArrowDatasetManipulation(file_path)
        dataset_old.do_everything()
        end_time = time.time()
        old_code_runtime = end_time - start_time

        start_time = time.time()
        dataset_new = better_code.ArrowDatasetManipulation(file_path)
        dataset_new.run_manipulation_methods()
        end_time = time.time()
        new_code_runtime = end_time - start_time

        assert new_code_runtime <= old_code_runtime, f"The new code is slower than the old code., new code runtime = {new_code_runtime}, ol code runtime = {old_code_runtime}"

    def test_results_matching_one_file(self):
        """
        Testing that the results generated by the new code match those generated by the old code.
        """
        file_path = 'runtime_test_files/test_file_size_5000.csv'
        dataset_old = code_1.BadArrowDatasetManipulation(file_path)
        dataset_old.do_everything()

        dataset_new = better_code.ArrowDatasetManipulation(file_path)
        dataset_new.run_manipulation_methods()

        assert np.array_equal(np.array(dataset_old.final_data),dataset_new.generated_results_data), f"The values of the results array generated from the two codes do not match"


    def test_results_matching_all_files(self):
        """
        Testing that the results generated by the new code match those generated by the old code.
        """
        files_dir = './runtime_test_files/'
        files_paths = glob.glob(f'{files_dir}*')
        # files_paths = files_paths[0:10]
        
        failed_cases_paths = []
        for file_path in files_paths : 
            dataset_old = code_1.BadArrowDatasetManipulation(file_path)
            dataset_old.do_everything()
            dataset_new = better_code.ArrowDatasetManipulation(file_path)
            dataset_new.run_manipulation_methods()
            if not np.array_equal(np.array(dataset_old.final_data),dataset_new.generated_results_data):
                failed_cases_paths.append(file_path)
        assert len(failed_cases_paths) == 0, f'Failed to match for the files: {failed_cases_paths}'


    def test_manipulate_empty_file(self): 
        """
        Testing passing an empty file to the new code
        """
        file_path = 'runtime_test_files/test_file_size_0.csv'
    
        dataset_new = better_code.ArrowDatasetManipulation(file_path)
        dataset_new.run_manipulation_methods()

        assert dataset_new.table.empty

    def test_csv_read_file(self): 
        """
        Testing passing a non-empty existing csv file to the new code
        """
        file_path = 'runtime_test_files/test_file_size_100.csv'
        
        dataset_new = better_code.ArrowDatasetManipulation(file_path)
        assert not dataset_new.completed_run
        dataset_new.run_manipulation_methods()

        assert dataset_new.completed_run
        assert not dataset_new.table.empty


    def test_parquet_read_file(self): 
        """
        Testing passing a non-empty existing parquet file to the new code
        """
        file_path = 'runtime_test_files/test_file_size_1000.parquet'
        dataset_new = better_code.ArrowDatasetManipulation(file_path)
        assert not dataset_new.completed_run
        dataset_new.run_manipulation_methods()
        assert dataset_new.completed_run
        assert not dataset_new.table.empty


    def test_parallel_execution(self): 
        """
        Tests the success of the new code to perform multithreading on an entire dataset.
        """
        files_dir = './runtime_test_files/'
        results = better_code.ArrowDatasetManipulation.process_dataset_in_parallel(files_dir=files_dir,num_workers=5)
        
        failed_cases = [x[1] for x in results if not x[0]]

        assert len(failed_cases)==0, f'Failed to run on files: {failed_cases}'


if __name__ == "__main__": 
    unittest.main()