# utils
import os
import shutil
import subprocess
import polars as pl
import re
from polars import DataFrame


def move_recent_files_to_directory(source_dir='./processed_logs', target_dir='./in'):
    import datetime
    from datetime import datetime as dt

    # Get the current date and time
    now = datetime.datetime.now()

    # Calculate the date and time 7 days ago
    delta = datetime.timedelta(days=7)
    seven_days_ago = now - delta

    # Loop through the files in the directory
    for filename in os.listdir(source_dir):
        if filename.startswith('.DS_Store'):
            continue  # skip .DS_Store files
        filepath = os.path.join(source_dir, filename)

        print(f"created_time {filepath}")
        if os.path.isfile(filepath):
            # Get the creation time of the file
            created_time = extract_date_from_filename(filename)
            print(f"created_time {created_time}")
            # Check if the file was created within the last 7 days
            if dt.strptime(str(created_time), '%Y%m%d') >= seven_days_ago:
                # Load the file here
                print(f"Loading {filepath}...")
                try:
                    shutil.move(os.path.join(source_dir, filename), target_dir)
                except:
                    print(f"Destination path {filepath} already exists")


def count_rows_in_logs(input_dir, file_name):
    find_command = ["find", input_dir, "-name", file_name, "-type", "f"]
    wc_command = ["xargs", "wc", "-l"]
    awk_command = ["awk", "END{print $1}"]

    find_process = subprocess.Popen(find_command, stdout=subprocess.PIPE)
    wc_process = subprocess.Popen(wc_command, stdin=find_process.stdout, stdout=subprocess.PIPE)
    awk_process = subprocess.Popen(awk_command, stdin=wc_process.stdout, stdout=subprocess.PIPE)

    output, error = awk_process.communicate()

    row_numbers = int(output.strip())
    return row_numbers


def extract_date_from_filename(filename):
    match = re.search(r'\d{8}', filename)
    if match:
        return match.group()
    else:
        return None


def get_all_files_in_specific_folder(input_folder="./in/"):
    import glob
    logfiles = []
    for file in glob.glob(f"{input_folder}*.log"):
        logfiles.append(file)
    return logfiles


# Define a function to join strings
def join_strings(arr, sep=","):
    return sep.join(str(x) for x in arr)


# TODO create a tmp folder if not exist
def get_file_base_name(absolute_path):
    return os.path.basename(absolute_path)


def read_logs_by_chunk_and_write_aggregation_by_chunk(input_file_path, separator, schema, number_of_chunks,
                                                      total_file_count):
    """
    Function to read logs from a file and apply transformations on the data.
    """
    skip_leading_rows = 0
    process_continue = True
    accumalor = 1

    while process_continue:
        loaded_stream_logs = pl.DataFrame()

        loaded_stream_logs = pl.read_csv(input_file_path, skip_rows=skip_leading_rows,
                                         separator=separator,
                                         ignore_errors=True, dtypes=schema, infer_schema_length=0,
                                         n_rows=number_of_chunks) \
            .filter(
            (pl.col('sng_id').is_not_null() & pl.col('user_id').is_not_null() & pl.col('country').is_not_null())) \
            .filter(pl.col('country').str.lengths() == 2) \
            .select(pl.col(["sng_id", "user_id", "country"])) \


        window_function_df = loaded_stream_logs.with_columns( \
            pl.count().over(["country", "sng_id"]).alias("top_listening")
        ).unique(subset=["country", "sng_id"])
        temporary_output_file_name_pattern = f"./tmp/count_aggregation_temp{accumalor}_{get_file_base_name(input_file_path)}"
        window_function_df.write_csv(temporary_output_file_name_pattern, separator=separator)

        accumalor += 1
        skip_leading_rows += number_of_chunks
        print(f"reading current : {skip_leading_rows} rows")

        mine = loaded_stream_logs.is_empty()
        if skip_leading_rows >= total_file_count:
            process_continue = False
#TODO : Remove two last params
def reduce_aggregated_wrote_counts(separator):
    #TODO : f"./tmp/count_aggregation_temp*" is the right thing
    read_multipart_df = pl.read_csv(
        f"./tmp/count_aggregation_temp*",
        separator=separator,
        ignore_errors=True, infer_schema_length=10000
    ) \
        .with_columns( \
        pl.col('top_listening').sum().over(["country", "sng_id"]).alias("final_top_listening")
    ).unique(subset=["country", "sng_id"]) \
        .sort(["country", "final_top_listening"], descending=True).select(
        pl.col(["sng_id", "country", "final_top_listening"])).with_columns(
        pl.col('final_top_listening').rank(method='ordinal').sort_by("final_top_listening", descending=False).over(
            'country').alias(
            'interm_rank')).filter(pl.col("interm_rank") <= 50)

    # write
    return read_multipart_df

def collect_final_result_from_reduce_aggregated_wrote_counts(multipart_df : DataFrame):
    multipart_ranked_df = multipart_df .with_columns(pl.concat_str(
        pl.col('sng_id'),
        pl.lit(":"),
        pl.col('final_top_listening')
    ).alias("SNG_ID_PER_TOP_LIST"))

    # Group the data by country and concatenate the song ID and top listening count for each country
    result = multipart_ranked_df.groupby('country').agg(
        pl.implode("SNG_ID_PER_TOP_LIST").flatten().apply(lambda s: join_strings(s, ",")))
    return result

def write_final_result(output_path, result_df):
    result_df.write_csv(output_path, separator="|")