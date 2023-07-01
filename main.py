# Import necessary modules
import os
import sys
from datetime import datetime
import polars as pl
from memory_profiler import profile

from utils import read_logs_by_chunk_and_write_aggregation_by_chunk, reduce_aggregated_wrote_counts, \
    collect_final_result_from_reduce_aggregated_wrote_counts, write_final_result, \
    move_recent_files_to_directory, get_all_files_in_specific_folder

input_file_path = sys.argv[1]
input_file_name = os.path.basename(input_file_path)
output_file_name_pattern = f"country_top50_{datetime.now().strftime('%Y-%m-%d')}.txt"
separator = "|"
CHUNKED_ROWS_NUMBER = 500000
skip_leading_rows = 0
DAILY_ROWS = 30000000
process_continue = True

# Define the schema for the CSV file
dtype = {
    "sng_id": pl.Int64,
    "user_id": pl.Int64,
    "country": pl.Utf8
}

if __name__ == "__main__":
    @profile
    def my_function():

        move_recent_files_to_directory()
        log_files = get_all_files_in_specific_folder()
        for index, file_name in enumerate(log_files):
            print(f"processing file_name : {file_name} : ")
            read_logs_by_chunk_and_write_aggregation_by_chunk(input_file_path=file_name, separator=separator,
                                                              schema=dtype,
                                                              number_of_chunks=CHUNKED_ROWS_NUMBER,
                                                              total_file_count=DAILY_ROWS)
        #TODO : Remove two last params
        multipart_df = reduce_aggregated_wrote_counts(separator=separator)

        result_df = collect_final_result_from_reduce_aggregated_wrote_counts(multipart_df=multipart_df)

        write_final_result(output_path=output_file_name_pattern, result_df=result_df)
        move_recent_files_to_directory(source_dir='./in', target_dir='./processed_logs')


    my_function()
