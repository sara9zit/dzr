# Define the pattern to match the filenames
filename_pattern="*.log"
input_dir="./in"
processed_dir="./processed_logs"
temporary_dir="./tmp"

# Create the processed logs directory if it doesn't exist
mkdir -p "$processed_dir"

# Create the tmp directory if it doesn't exist
mkdir -p  "$temporary_dir"

mkdir -p "$input_dir"


echo $"$input_dir/$filename_pattern"
pip3 install -r requirements.txt

python3 main.py "$input_dir/$filename_pattern"
