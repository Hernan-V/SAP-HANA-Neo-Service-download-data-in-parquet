
# Copyright 2023 Hernan Valenzuela
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
from hana_ml import dataframe as hd


def convert_to_bytes(size: int, unit: object) -> object:
  units = dict(B=1, KB=1024, MB=1024 ** 2, GB=1024 ** 3, TB=1024 ** 4)
  return size * units[unit.upper()]


def execute_configuration(config, **kwargs)
# Create the directory if it doesn't exist
  os.makedirs(os.path.dirname(args.config_dir), exist_ok=True)

  cc = hd.ConnectionContext(
      address=os.getenv('SAP_HANA_HOST'),
      port=os.getenv('SAP_HANA_PORT'),
      user=os.getenv('SAP_HANA_USER'),
      password=os.getenv('SAP_HANA_PASSWORD')
  )

  try:
    for table_data in config:
      # Retrieve table name from config file if exist or from script argument
      if 'table' in table_data and table_data['table']:
        table = table_data['table']
      elif 'table' in kwargs and kwargs['table']:
        table = kwargs['table']

  finally:
    cc.close()


# if __name__ == '__main__':
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('mode', type=str, required=True, choices=['download', 'configure'], help='Mode or action to run the program')
parser.add_argument('--config_dir', '-cd', type=str, required=True, help='Directory where the configuration files are written or read it from')
parser.add_argument('--table', '-t', type=str, help='Table to work with')
parser.add_argument('--table_schema', '-ts', type=str, help='DB Schema of the table to work with')
parser.add_argument('--limit_mode', '-lm', type=str, choices=['records', 'B', 'KB', 'MB', 'GB', 'TB'], help='How to calculate the batch size of the parquets files')
parser.add_argument('--limit_num', '-ln', type=int, help='Size of the batch of the parquet files')
parser.add_argument('--group', '-g', nargs='+', help='List of fields to make a grouping and splitting files')
parser.add_argument('--null_treatment', '-nt', help='Text in json format or path to json file where there is a specification of each field, tha values that represent null and a static value for replacement')
parser.add_argument('--download_dir', '-dd', type=str, help='Path where the data files will be downloaded')
parser.add_argument('--download_mode', '-dm', choices=['local', 'GCP_cloud_storage'], help='File system or bucket where the files will be written')

args = parser.parse_args()

config_file = []

# self call to class?

# Check which of the modes will be executed
if args.mode == 'configure':
  # Validate the required arguments
  if not args.config_dir:
    parser.error("The --config_dir flag is required as the path to write the configuration files")
  if not args.table or not args.table_schema:
    parser.error("Both --table and --table_schema flags are required to spacify for the configuration file creation")
  if not args.limit_mode:
    parser.error("The --limit_mode flag must be specified")

  # Create the directory if it doesn't exist
  os.makedirs(os.path.dirname(args.config_dir), exist_ok=True)

  cc = hd.ConnectionContext(
      address=os.getenv('SAP_HANA_HOST'),
      port=os.getenv('SAP_HANA_PORT'),
      user=os.getenv('SAP_HANA_USER'),
      password=os.getenv('SAP_HANA_PASSWORD')
  )

  try:
    # Calculate the record size
    table_size = cc.sql(f'SELECT RECORD_COUNT, TABLE_SIZE FROM SYS.M_TABLES WHERE SCHEMA_NAME = {args.table_schema} AND TABLE_NAME = {args.table}')
    if args.limit_mode == 'records'
      record_size = args.limit_num
    else:
      byte_size = convert_to_bytes(args.limit_num, args.limit_mode)
      record_size = (byte_size * table_size[0]) // table_size[1]

    #sql_statmnt = f'SELECT * FROM SYS.M_TABLES WHERE SCHEMA_NAME={args.table_schema} AND TABLE_NAME={args.table}'

    # build the dictionary to download to configuration file
    config_file.append({})
    config_file[0]["schema"] = args.table_schema
    config_file[0]["table"] = args.table
    config_file[0]["rec_size"] = record_size

    # add fields
    if args.null_treatment:
      try:
        # Try to load JSON from string
        null_config = json.loads(args.json)
      except json.JSONDecodeError:
        # If that fails, assume it's a file path and load from file
        with open(args.json) as f:
          null_config = json.load(f)

    table_fields = cc.sql(f'SELECT "COLUMN_NAME", "INDEX_TYPE", "DATA_TYPE_NAME", "LENGTH", "SCALE" FROM "SYS"."TABLE_COLUMNS" WHERE "TABLE_SCHEMA" = {args.table_schema} AND "TABLE_NAME" = {args.table} ORDER BY "POSITION"')
    if table_fields:
      config_file[0]["fields"] = {}
      for fields in table_fields:
        config_file[0]["fields"][fields[0]] = {}
        config_file[0]["fields"][fields[0]]["key"] = "X" if fields[1] != 'NONE' else ""
        config_file[0]["fields"][fields[0]]["type"] = fields[2]
        config_file[0]["fields"][fields[0]]["length"] = fields[3]
        config_file[0]["fields"][fields[0]]["scale"] = fields[4]

        # Parse the null exceptions for each field
        for null_except in null_config:
          if 'field' in null_except and fields[0] == null_except['field']:
            if 'null_const' in null_except:
              config_file[0]["fields"][fields[0]]["null_const"] = null_except['null_const']
            if 'nulls' in null_except:
              config_file[0]["fields"][fields[0]]["nulls"] = null_except['nulls']
            if 'not_nulls' in null_except:
              config_file[0]["fields"][fields[0]]["not_nulls"] = null_except['not_nulls']

    # add the rest of the info to dump the result data
    if args.download_dir:
      config_file[0]["directory"] = args.download_dir
    if args.download_mode:
      config_file[0]["target"] = download_mode

    # add grouping
    if args.group:
      group_fields_string = ','.join(['"' + item + '"' for item in args.group])
      #table_grouping_validation = cc.sql(f'SELECT SUM(EXCEED_LIMIT)*100/SUM(GRAND_TOTAL) FROM (SELECT {group_fields_string}, 1 AS GRAND_TOTAL, (CASE WHEN RECORD_COUNT > RECORD_LIMIT THEN 1 ELSE 0 END) AS EXCEED_LIMIT FROM (SELECT {group_fields_string}, COUNT(*) AS RECORD_COUNT, {record_size} AS RECORD_LIMIT FROM {args.table_schema}.{args.table} GROUP BY {group_fields_string}))')
      # Pop up message to validate if countinue or not
      grouping_values = cc.sql(f'SELECT DISTINCT {group_fields_string} FROM {args.table_schema}.{args.table}')
      for unique_key in grouping_values:
        final_config = config_file
        for name, value in zip(args.group, unique_key):
          if not 'grouping' in final_config[0]:
            final_config[0]['grouping'] = []
            config_file_name = f'config_{args.table_schema}_{args.table}'
          final_config[0]["grouping"].append({"field": name, "value": value})
          config_file_name += f'_{name}-{value}'
        # save the result into a json file
        with open(os.path.join(args.config_dir, f'{config_file_name}.json'), 'w') as f:
            # write the data to the file using json.dump()
          json.dump(final_config, f)
    else:
      with open(os.path.join(args.config_dir, f'config_{args.table_schema}_{args.table}.json'), 'w') as f:
          # write the data to the file using json.dump()
        json.dump(config_file, f)

  finally:
    cc.close()

elif args.mode == 'download':
  # Validate the required arguments
  if not args.config_dir and (not args.table or not args.table_schema):
    parser.error("Either the --config_dir flag or the --table and --table_schema must be specified")
  if not args.config_dir and (not args.limit_mode or not args.limit_num):
    parser.error("Either the --config_dir flag or the --limit_mode and --limit_num must be specified")
  if not args.config_dir and (not args.download_dir or not args.download_mode):
    parser.error("The --download_dir and --download_mode must be specified")

  # Loop through each file in the directory
  if os.path.isdir(args.config_dir):
    for filename in os.listdir(args.config_dir):
      if filename.endswith(".json"):
        file_path = os.path.join(args.config_dir, filename)
        with open(file_path) as f:
          config = json.load(f)
          execute_configuration(config, **vars(args))
  else:
    with open(args.config_dir) as f:
      config = json.load(f)
      execute_configuration(config, **vars(args))

  table_fields = cc.sql(f'SELECT {field_list} FROM "{args.table_schema}"."{args.table}" {where_clause}')
