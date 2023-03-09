
import json

# if __name__ == '__main__':
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('mode', type=str, required=True, choices=['download', 'configure'], help='Mode or action to run the program')
parser.add_argument('--config_dir', '-cd', type=str, required=True, help='Directory where the configuration files are written or read it from')
parser.add_argument('--table', '-t', type=str, help='Table to work with')
parser.add_argument('--table_schema', '-ts', type=str, help='DB Schema of the table to work with')
parser.add_argument('--limit_mode', '-lm', type=str, choices=['records', 'B', 'KB', 'MB', 'GB', 'TB'], help='How to calculate the batchsize of the parquets files')
parser.add_argument('--limit_num', '-ln', type=int, help='Size of the batch of the parquet files')
parser.add_argument('--group', '-g', nargs='+', help='List of fields to make a grouping and spliting files')
parser.add_argument('--null_treatment', '-nt', help='Text in json format or path to json file where there is a specification of each field, tha values that represent null and a static value for replacement')
parser.add_argument('--download_dir', '-dd', type=str, help='Path where the data files will be downloaded')
parser.add_argument('--download_mode', '-dm', choices=['local', 'GCP_cloud_storage'], help='File system or bucket where the files will be written')

args = parser.parse_args()

# self call to class?

# Check which of the modes will be executed
if args.mode == 'configure':
  # Validate the required arguments
  if not args.config_dir
    parser.error("The --config_dir flag is required as the path to write the configuration files")
  if not args.table or not args.table_schema
    parser.error("Both --table and --table_schema flags are required to spacify for the configuration file creation")
  if not args.limit_mode
    parser.error("The --limit_mode flag must be specified")

elif args.mode == 'download':
  # Validate the required arguments
  if not args.config_dir and (not args.table or not args.table_schema)
    parser.error("Either the --config_dir flag or the --table and --table_schema must be specified")
  if not args.config_dir and (not args.limit_mode or not args.limit_num)
    parser.error("Either the --config_dir flag or the --limit_mode and --limit_num must be specified")
  if not args.config_dir and (not args.download_dir or not args.download_mode)
    parser.error("The --download_dir and --download_mode must be specified")

if args.null_treatment
  try:
    # Try to load JSON from string
    null_json = json.loads(args.json)
  except json.JSONDecodeError:
    # If that fails, assume it's a file path and load from file
    with open(args.json) as f:
      null_json = json.load(f)

  # file structure
  #[
  #  {
  #    "field": "AEDAT"
  #    "null_const": "19000101"
  #    "nulls": ["","00000000"]
  #    "not_nulls": ["\regex0030d"]
  #  }
  #  {
  #    "field": "BUKRS"
  #    "null_const": "0101"
  #    "nulls": ["","00000000"]
  #    "not_nulls": ["\regex0030d"]
  #  }
  #]

  for fields in null_json:
    field = fields['field']
    null_const = fields['null_const']
    nulls = fields['nulls']
    for null_regex in nulls:
      if null_regex regex 'lalala'
