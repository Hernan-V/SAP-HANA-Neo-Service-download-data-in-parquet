
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

import argparse
import json
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hana_ml import dataframe as hd


def call_api_to_create_directory_on_an_existing_bucket():
  var=1

def convert_to_bytes(size: int, unit: str) -> int:
  units = dict(B=1, KB=1024, MB=1024 ** 2, GB=1024 ** 3, TB=1024 ** 4)
  return size * units[unit.upper()]


def calculate_max_record_batch(ConnectionContext: hd.ConnectionContext, **kwargs) -> int:
  if not "limit_mode" in kwargs or not kwargs["limit_mode"] or not "limit_num" in kwargs or not kwargs["limit_num"]:
    return 0
  elif "limit_mode" in kwargs and kwargs["limit_mode"] == 'records':
    return kwargs["limit_num"]
  else:
    byte_size = convert_to_bytes(kwargs["limit_num"], kwargs["limit_mode"])
    table_size = ConnectionContext.sql(
      f'SELECT RECORD_COUNT, TABLE_SIZE FROM SYS.M_TABLES WHERE SCHEMA_NAME = {kwargs["table_schema"]} AND TABLE_NAME = {kwargs["table"]}')
    return (byte_size * table_size[0]) // table_size[1]

def check_arg_in_config_or_input(attribute :str, config_line, parser: argparse.ArgumentParser, message :str, **kwargs ):
  if attribute in config_line and config_line[attribute]:
    kwargs[attribute] = config_line[attribute]
  elif attribute not in kwargs or not kwargs[attribute]:
    parser.error(message)
    exit()

def execute_configuration(config, parser: argparse.ArgumentParser, **kwargs):

  cc = hd.ConnectionContext(
      address=os.getenv('SAP_HANA_HOST'),
      port=os.getenv('SAP_HANA_PORT'),
      user=os.getenv('SAP_HANA_USER'),
      password=os.getenv('SAP_HANA_PASSWORD')
  )

  try:
    for config_line in config:
      # Retrieve table name from config file if exist or from script argument
      check_arg_in_config_or_input('table', config_line, parser,
                                   "Either the configuration file at --config_dir need attribute table or the --table and --table_schema must be specified",
                                   **kwargs)
      # Retrieve table schema name from config file if exist or from script argument
      check_arg_in_config_or_input('table_schema', config_line, parser,
                                   "Either the configuration file at --config_dir need attribute table or the --table and --table_schema must be specified",
                                   **kwargs)
      # Check if the data will be download locally or on the cloud
      check_arg_in_config_or_input('download_mode', config_line, parser,
                                   "The --download_dir and --download_mode must be specified or the same attribute on configuration file at --config_dir",
                                   **kwargs)
      # Create the directory if it doesn't exist to dump the data
      check_arg_in_config_or_input('download_dir', config_line, parser,
                                   "The --download_dir and --download_mode must be specified or the same attribute on configuration file at --config_dir",
                                   **kwargs)
      if kwargs["download_mode"] == 'local':
        os.makedirs(os.path.dirname(kwargs['download_dir']), exist_ok=True)
      else:
        call_api_to_create_directory_on_an_existing_bucket()

      config_file_name = f'config_{kwargs["table_schema"]}_{kwargs["table"]}'

      # Retrieve batch mode and size from config file if exist or from script argument
      if "rec_size" in kwargs or kwargs["rec_size"]:
        record_size = kwargs["rec_size"]
      else:
        record_size = calculate_max_record_batch(cc, **kwargs)

      # Create the grouping as a where condition
      where_clause = ""
      for group in config_line['grouping']:
        config_file_name += f'_{group["field"]}_{group["name"]}'
        if len(where_clause) < 1:
          where_clause = f"WHERE \"{group['field']}\" = \'{group['value']}\'"
        else:
          where_clause += f" AND \"{group['field']}\" = \'{group['value']}\'"

      # Create the list of fields to retrieve
      field_list = ""
      keys = ""
      for field_name, field_properties in config_line['fields'].items() :
        if len(field_list) < 1:
          field_list = f"\"{field_name}\""
        else:
          field_list += f" , \"{field_name}\""
        # Populate the key fields
          if isinstance(field_properties['key'], str):
            if field_properties['key'].upper() == 'X':
              if len(keys) < 1:
                keys = f"\"{field_name}\""
              else:
                keys += f" , \"{field_name}\""
      if len(keys) < 1:
        keys = field_list

      if record_size > 1:
        offset = 0
        table_df = pd.DataFrame()
        while not table_df.empty:
          table_df = cc.sql(
            f'SELECT {field_list} FROM "{kwargs["table_schema"]}"."{kwargs["table"]}" {where_clause} ORDER BY {keys} LIMIT {record_size} OFFSET {offset}').collect()
          # Data cleanse
          for field_name, field_properties in config_line['fields'].items():
            null_const = field_properties.get('null_const', None)
            nulls = field_properties.get('nulls', [])
            not_nulls = field_properties.get('not_nulls', [])

            # replace all invalid values with a constant
            if len(nulls) > 0 and null_const != None:
              for null_value in nulls:
                table_df[field_name] = table_df[field_name].replace(null_value, null_const, regex=True)
            # replace all not valid values with a constant
            if len(not_nulls) > 0 and null_const != None:
              for not_null_value in not_nulls:
                # negate the expression
                not_null_value = '^((?!' + not_null_value + ').)*$'
                table_df[field_name] = table_df[field_name].replace(not_null_value, null_const, regex=True)

          # Convert dataframe to parquet
          table = pa.Table.from_pandas(table_df)
          # Write file data
          if kwargs["download_mode"] == 'local':
            pq.write_table(table, os.path.join(kwargs['download_dir'],
                                               f"{config_file_name}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"),
                           compression='snappy')
          offset += record_size
      else:
        table_df = cc.sql(
          f'SELECT {field_list} FROM "{kwargs["table_schema"]}"."{kwargs["table"]}" {where_clause} ORDER BY {keys}').collect()
        #Data cleanse
        for field_name, field_properties in config_line['fields'].items():
          null_const = field_properties.get('null_const', None)
          nulls = field_properties.get('nulls', [])
          not_nulls = field_properties.get('not_nulls', [])

          #replace all invalid values with a constant
          if len(nulls) > 0 and null_const != None:
            for null_value in nulls:
              table_df[field_name] = table_df[field_name].replace(null_value, null_const, regex=True)
          # replace all not valid values with a constant
          if len(not_nulls) > 0 and null_const != None:
            for not_null_value in not_nulls:
              #negate the expression
              not_null_value = '^((?!' + not_null_value + ').)*$'
              table_df[field_name] = table_df[field_name].replace(not_null_value, null_const, regex=True)

        #Convert dataframe to parquet
        table = pa.Table.from_pandas(table_df)
        #Write file data
        if kwargs["download_mode"] == 'local':
          pq.write_table(table, os.path.join(kwargs['download_dir'],
                                             f"{config_file_name}_{datetime.now().strftime('%Y%m%d%H%M%S%f')}.json"),
                         compression='snappy')


  finally:
    cc.close()


# if __name__ == '__main__':
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
  if (args.limit_num and not args.limit_mode) or (not args.limit_num and args.limit_mode):
    parser.error("Both --limit_mode and --limit_num must be specified")

  # Create the directory if it doesn't exist
  os.makedirs(os.path.dirname(args.config_dir), exist_ok=True)

  cc = hd.ConnectionContext(
      address=os.getenv('SAP_HANA_HOST'),
      port=os.getenv('SAP_HANA_PORT'),
      user=os.getenv('SAP_HANA_USER'),
      password=os.getenv('SAP_HANA_PASSWORD')
  )

  try:
    config_file_name = f'config_{args.table_schema}_{args.table}'
    cursor = cc.connection.cursor()

    # Calculate the record size
    record_size = calculate_max_record_batch(cc, **vars(args))

    # build the dictionary to download to configuration file
    config_file.append({})
    config_file[0]["schema"] = args.table_schema
    config_file[0]["table"] = args.table
    if record_size and record_size > 0:
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

    cursor.execute(f'SELECT "COLUMN_NAME", "INDEX_TYPE", "DATA_TYPE_NAME", "LENGTH", "SCALE" FROM "SYS"."TABLE_COLUMNS" WHERE "TABLE_SCHEMA" = {args.table_schema} AND "TABLE_NAME" = {args.table} ORDER BY "POSITION"')
    table_fields = cursor.fetchall()
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
      config_file[0]["download_dir"] = args.download_dir
    if args.download_mode:
      config_file[0]["download_mode"] = args.download_mode

    # add grouping
    if args.group:
      group_fields_string = ','.join(['"' + item + '"' for item in args.group])
      #table_grouping_validation = cc.sql(f'SELECT SUM(EXCEED_LIMIT)*100/SUM(GRAND_TOTAL) FROM (SELECT {group_fields_string}, 1 AS GRAND_TOTAL, (CASE WHEN RECORD_COUNT > RECORD_LIMIT THEN 1 ELSE 0 END) AS EXCEED_LIMIT FROM (SELECT {group_fields_string}, COUNT(*) AS RECORD_COUNT, {record_size} AS RECORD_LIMIT FROM {args.table_schema}.{args.table} GROUP BY {group_fields_string}))')
      # Pop up message to validate if countinue or not
      cursor.execute(f'SELECT DISTINCT {group_fields_string} FROM {args.table_schema}.{args.table}')
      grouping_values = cursor.fetchall()
      for unique_key in grouping_values:
        final_config = config_file
        for name, value in zip(args.group, unique_key):
          if not 'grouping' in final_config[0]:
            final_config[0]['grouping'] = []
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
    cursor.close()
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
          execute_configuration(config, parser, **vars(args))
  else:
    with open(args.config_dir) as f:
      config = json.load(f)
      execute_configuration(config, parser, **vars(args))

