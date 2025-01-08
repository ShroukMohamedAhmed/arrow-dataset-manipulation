# First of all, the code below will break. Please fix the bugs.

# Please refactor the code to be more readable, maintainable, and have better debuggability. Meaning
#     1. Descriptive variable names, function names, class names.
#     2. The depth of nested `if else statement` or `for loops branches` should be <= 3.
#     3. Rearrange the code -- such as creating new functions, removing functions, rearranging the code orders.
#     4. Remove unnecessary code when possible.
#     5. Keep functions to be short, e.g. < 20 lines.
#     6. Anything else that improves debuggability.
#     7. Add Google-style documentation for each function.
#
# Please optimize the code to reduce its runtime.
# Hint: can you use less for loops? You can use pandas or numpy if you want. Can you test how much runtime it saves with
# your optimized code with a large input file?

# Write Unit tests to test the code so that the results before and after the refactoring stays the same. Ideally test
# every function. You need to create test data to cover as many scenarios as possible.
#
# Bonus: Can you parallelize the code with multi-threading so that it can process 100 input files in parallel?
#
# Please do not use chatGPT or any AI tools directly for these tasks.



import pyarrow as pa
import os
import logging


class BadArrowDatasetManipulation:

    def __init__(self, file_path):
        # self.data = None
        self.file = file_path

        self.logging_dir = './logs/old_code/'
        if not os.path.exists(self.logging_dir):
            os.makedirs(self.logging_dir)

        self.log_file_path = self.logging_dir+self.file.split('/')[-1].split('.')[0]+'.log'

        self.logger = logging.getLogger(__name__)
        logging.basicConfig(filename= self.log_file_path ,  level=logging.DEBUG)



        self.tbl = self.read_data_from_file(self.file)
        self.result = None
        self.filter_result = None
        self.some_other_data = None
        self.final_data = None

    def read_data_from_file(self, path):
        self.logger.info("Reading data...")
        if path.endswith('.csv'):
            self.logger.info("File is a CSV!")
            try:
                import pyarrow.csv as csv
                return csv.read_csv(path).to_pandas()
            except Exception as e:
                self.logger.error("Error reading file:", e)
                return None
        elif path.endswith('.parquet'):
            self.logger.info("File is a Parquet!")
            return None
        else:
            self.logger.error("Unknown file type!")
            return None

    def filter_data(self):
        if not self.tbl.empty:
            filter_val = 42
            self.logger.info("Filtering data based on some condition...")
            filtered = []
            # Accessing the 'value' column correctly
            value_column = self.tbl['value']  # Column data as a ChunkedArray

            # Iterate through the column (ChunkedArray) by index
            for row in range(len(value_column)):
                if value_column[row] < filter_val:
                    filtered.append(row)

            self.filter_result = filtered
            return filtered
        else:
            self.logger.warning("No data to filter")
            return None

    def add_column(self):
        self.logger.info("Adding a new column...")
        if not self.tbl.empty:
            new_column_data = []
            value_column = self.tbl['value']  # Accessing 'value' column directly
            for row in range(len(value_column)):
                self.logger.info(f"Debug code: {value_column[row]}")
                if value_column[row] > 100:
                    new_column_data.append('High')
                elif value_column[row] <= 100 and value_column[row] > 50:
                    new_column_data.append('Medium')
                else:
                    new_column_data.append('Low')
            # self.tbl = self.tbl.append_column('new_column', pa.array(new_column_data))
            self.tbl['new_column'] = new_column_data
            self.logger.info("Column added!")
        else:
            self.logger.warning("No data to modify.")

    def complex_manipulation(self):
        self.logger.info("Performing complex manipulations...")
        if not self.tbl.empty:
            result = []
            value_column = self.tbl['value']
            length = len(value_column)

            if length > 50:
                for i in range(length):
                    if i % 2 == 0:
                        if value_column[i] > 10:
                            result.append(value_column[i] * 2)
                        elif value_column[i] <= 10:
                            result.append(value_column[i] + 100)
                        else:
                            result.append(value_column[i] - 50)
                    else:
                        for j in range(3):
                            if value_column[i] < 100:
                                result.append(value_column[i] + 1)
                            elif value_column[i] > 100:
                                result.append(value_column[i] - 2)
                            else:
                                result.append(value_column[i])
                            if j % 2 == 0:
                                result.append(value_column[i] * 3)
                        if i == length - 1:
                            self.logger.info("End of first loop")
                        else:
                            self.logger.info("Continuing first loop")

            else:
                self.logger.info("Using the else block for no reason.")
                for i in range(length):
                    if value_column[i] < 5:
                        result.append(value_column[i] * 10)
                    else:
                        for j in range(5):
                            if value_column[i] > 200:
                                result.append(value_column[i] + j)
                            else:
                                result.append(value_column[i] - j)

            for i in range(length):
                for j in range(3):
                    if value_column[i] < 50:
                        if value_column[i] % 2 == 0:
                            result.append(value_column[i] + 10)
                        else:
                            result.append(value_column[i] - 10)
                    elif value_column[i] > 100:
                        result.append(value_column[i] * 2)
                    else:
                        for k in range(2):
                            if k == 0:
                                result.append(value_column[i] + 20)
                            elif k == 1:
                                result.append(value_column[i] - 20)
                            else:
                                result.append(value_column[i])

            if length > 1000:
                self.logger.warning("Data is too large for any sensible manipulation.")

            for i in range(len(result)):
                if result[i] > 1000:
                    self.logger.info(f"Large value detected: {result[i]}")
                elif result[i] < 10:
                    self.logger.info(f"Small value detected: {result[i]}")
                else:
                    self.logger.info(f"Normal value detected: {result[i]}")

            self.final_data = result
            self.logger.info("Manipulation complete with unnecessary steps!")
        else:
            self.logger.warning("No data to manipulate!")

    def redundant_check_function(self):
        if not self.tbl.empty:
            value_column = self.tbl['value']
            for row in range(len(value_column)):
                if value_column[row] > 10:
                    self.logger.info("Greater than 10!")
                else:
                    if value_column[row] > 5:
                        self.logger.info("Greater than 5 but less than 10!")
                    else:
                        if value_column[row] > 0:
                            self.logger.info("Greater than 0 but less than 5!")
                        else:
                            self.logger.info("Non-positive value!")
        else:
            self.logger.info("No data to check!")

    def do_everything(self):
        if not self.tbl.empty:
            self.filter_data()
            self.add_column()
            self.complex_manipulation()
            self.redundant_check_function()
            unused_var = 42
            unused_var2 = "a varaible"
            if unused_var == 42:
                self.logger.info("Something!")
            self.result = "Done!"
        else:
            self.logger.warning("No data to process!")

    def xfunction(self):
        for i in range(1000000):
            if i % 100 == 0:
                self.logger.info(f"Working on {i}...")
            if i % 2 == 0:
                for j in range(10):
                    if j % 2 == 0:
                        pass
                    else:
                        pass
        self.logger.info("Finished dummy function!")


if __name__=="__main__":
    bad_script = BadArrowDatasetManipulation('good.csv')
    bad_script.do_everything()
    bad_script.xfunction()
