import argparse
import json
import os
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from hana_ml import dataframe as hd
from tqdm import tqdm
import copy


def create_directory_on_bucket():
    """
    TODO: Implement the API call to create a directory on the cloud bucket
    """
    pass

def check_tilde_expansion(args: argparse.Namespace) -> None:
    """
    Check if the paths have tilde as the $HOME variable
    and expand it to a full path
    Args:
        size (int): size to be converted
        unit (str): unit of the size

    Returns:
        None
    """
    if args.download_dir:
        args.download_dir = os.path.expanduser(args.download_dir)
    if args.config_dir:
        args.config_dir = os.path.expanduser(args.config_dir)
    if args.null_treatment:
        args.null_treatment = os.path.expanduser(args.null_treatment)

def create_directory_if_not_exist(directory: str) -> None:
    """
    Create a directory if it does not exist and add a trailing slash to the end of the directory path.

    Args:
        directory: the path of the directory to create or check

    Returns:
        None
    """
    if not directory.endswith('/'):
        directory = directory + '/'
    os.makedirs(os.path.dirname(directory), exist_ok=True)

def convert_to_bytes(size: int, unit: str) -> int:
    """
    Converts size from unit to bytes
    Args:
        args (argparse.Namespace): Arguments

    Returns:
        size (int): converted size to bytes
    """
    units = dict(B=1, KB=1024, MB=1024 ** 2, GB=1024 ** 3, TB=1024 ** 4)
    return size * units[unit.upper()]


def get_connection_context() -> hd.ConnectionContext:
    """
    Returns a connection context for HANA DB
    Returns:
        cc (hd.ConnectionContext): HANA DB connection context
    """
    cc = hd.ConnectionContext(
        address=os.getenv("SAP_HANA_HOST"),
        port=os.getenv("SAP_HANA_PORT"),
        user=os.getenv("SAP_HANA_USER"),
        password=os.getenv("SAP_HANA_PASSWORD"),
    )
    return cc


def calculate_max_record_batch(cc: hd.ConnectionContext, args: argparse.Namespace) -> int:
    """
    Calculates the maximum record batch size
    Args:
        cc (hd.ConnectionContext): HANA DB connection context
        args (argparse.Namespace): Arguments

    Returns:
        int: Maximum record batch size
    """

    if not (args.limit_mode and args.limit_num):
        return 0
    elif args.limit_mode == "records":
        return args.limit_num
    else:
        try:
            cursor = cc.connection.cursor()
            byte_size = convert_to_bytes(args.limit_num, args.limit_mode)
            cursor.execute(f'SELECT "RECORD_COUNT", "TABLE_SIZE" FROM "SYS"."M_TABLES" WHERE '
                        f'"SCHEMA_NAME" = \'{args.table_schema}\' AND "TABLE_NAME" = \'{args.table}\'')
            table_size = cursor.fetchall()
            return (byte_size * table_size[0][0]) // table_size[0][1]
        finally:
            cursor.close()


def calculate_table_record_count(cc: hd.ConnectionContext, tokens: dict, args: argparse.Namespace) -> int:
    """
    Retrieve the record count of the table filtered by the group condition
    Args:
        cc (hd.ConnectionContext): HANA DB connection context
        tokens (dict): Tokens dictionary
        args (argparse.Namespace): Arguments

    Returns:
        int: Maximum record batch size
    """
    try:
        cursor = cc.connection.cursor()
        cursor.execute(f'SELECT COUNT(*) FROM \"{args.table_schema}\".\"{args.table}\" '
                      f'{tokens.get("where_clause")} ')
        record_count = cursor.fetchall()
        return record_count[0][0]
    finally:
        cursor.close()


def parse_config(config_line: dict, parser: argparse.ArgumentParser, tokens: dict, args: argparse.Namespace) -> None:
    """
    Parses the configuration file or command line arguments and validates required attributes.
    Args:
        config_line (dict): Configuration line
        parser (argparse.ArgumentParser): Argument parser
        tokens (dict): Tokens dictionary
        args (argparse.Namespace): Arguments

    Returns:
        None
    """
    required_attrs = [
        ("table", 'Either the configuration file at --config_dir needs the "table" attribute, or the --table and '
                  '--table_schema arguments must be specified'),
        ("table_schema", 'Either the configuration file at --config_dir needs the "table_schema" attribute, '
                         'or the --table and --table_schema arguments must be specified'),
        ("download_mode", 'Either the --download_dir and --download_mode arguments must be specified, '
                          'or the configuration file at --config_dir needs both the "download_dir" and '
                          '"download_mode" attributes'),
        ("download_dir", 'Either the --download_dir and --download_mode arguments must be specified, '
                         'or the configuration file at --config_dir needs both the "download_dir" and "download_mode"'
                         ' attributes'),
    ]
    # Validate required attributes
    for attr, error_msg in required_attrs:
        if attr in config_line and config_line[attr]:
            setattr(args, attr, config_line[attr])
        elif not getattr(args, attr):
            parser.error(error_msg)
            exit()

    # Create download directory if it does not exist
    if args.download_mode == "local":
        create_directory_if_not_exist(args.download_dir)
    else:
        create_directory_on_bucket()

    # Parse file name and batch size
    tokens["data_file_name"] = f'data_{args.table_schema}_{args.table}'
    tokens["record_size"] = config_line.get('rec_size') or calculate_max_record_batch(get_connection_context(), args)
    # Create the grouping as a where condition
    if config_line.get("grouping"):
        tokens["where_clause"] = "WHERE " + " AND ".join(
            [f'"{group["field"]}" = \'{group["value"]}\'' for group in config_line["grouping"]])
        tokens["data_file_name"] += "_"+"_".join([f'{group["field"]}-{group["value"]}' for group in config_line["grouping"]])
    else:
        tokens["where_clause"] = ''
    # Create the list of fields to retrieve
    tokens["field_list"] = ",".join([f'\"{value}\"' for value in list(config_line["fields"].keys())])
    tokens["keys"] = ",".join([f'\"{key}\"' for key, val in config_line["fields"].items() if val.get("key") == "X"]) \
                     or tokens["field_list"]


def replace_invalid_values(config_line: dict, table_df: pd.DataFrame) -> pd.DataFrame:
    """
    Replaces invalid values in a pandas dataframe with a constant value specified in the configuration file.

    Args:
        config_line: A dictionary representing the configuration settings.
        table_df: A Pandas DataFrame containing the data to process.

    Returns:
        A modified Pandas DataFrame with the invalid values replaced according to the configuration settings.
    """
    for field_name, field_properties in config_line["fields"].items():
        null_const = field_properties.get("null_const", None)
        nulls = field_properties.get("nulls", [])
        not_nulls = field_properties.get("not_nulls", [])

        # replace all invalid values (OR) with a constant
        null_values = "|".join(nulls)
        if null_values:
            table_df[field_name] = table_df[field_name].replace(null_values, null_const, regex=True)

        # replace all not valid values (AND) with a constant
        if not_nulls:
            not_null_value = "^((?!" + "|".join(not_nulls) + ").)*$"
            table_df[field_name] = table_df[field_name].replace(not_null_value, null_const, regex=True)

    return table_df


def write_parquet_table(table: pa.Table, download_mode: str, data_file_name: str, download_dir: str):
    """Write data to a Parquet file.

    Args:
        table: A PyArrow Table containing the data to write.
        download_mode: A string representing the download mode (e.g., 'local', 's3', etc.).
        data_file_name: A string representing the name of the file containing the data.
        download_dir: A string representing the download directory.

    Returns:
        None
    """
    file_name = f'{data_file_name}_{datetime.now().strftime("%Y%m%d%H%M%S%f")}.parquet.snappy'
    if download_mode == "local":
        file_path = os.path.join(download_dir, file_name)
        pq.write_table(table, file_path, compression="snappy")


def execute_configuration(config: list, parser: argparse.ArgumentParser, args: argparse.Namespace):
    """Execute the configuration settings and download the data

    Args:
        config: A list of dictionaries representing the configuration settings.
        parser: An instance of the ArgumentParser class.
        args: An instance of the Namespace class.

    Returns:
        None
    """
    cc = get_connection_context()
    table_df = pd.DataFrame()

    try:
        for config_line in config:
            tokens = {}
            parse_config(config_line, parser, tokens, args)

            if tokens.get("record_size") > 1:
                offset = 0

                max_size_limit = calculate_table_record_count(cc, tokens, args)
                pbar_batch = tqdm(desc=f'Download table {args.table_schema}.{args.table}', total=max_size_limit)
                while offset <= max_size_limit:
                    table_df = cc.sql(
                        f'SELECT {tokens.get("field_list")} FROM \"{args.table_schema}\".\"{args.table}\" '
                        f'{tokens.get("where_clause")} ORDER BY {tokens.get("keys")} LIMIT'
                        f' {tokens.get("record_size")} OFFSET {offset}').collect()
                    # Convert dataframe to parquet
                    table = pa.Table.from_pandas(replace_invalid_values(config_line, table_df))
                    # Write file data
                    write_parquet_table(table, args.download_mode, tokens.get("data_file_name"), args.download_dir)
                    offset += tokens.get("record_size")
                    pbar_batch.update(tokens.get("record_size"))
                    if table_df.empty:
                        break
                pbar_batch.close()
            else:
                table_df = cc.sql(f'SELECT {tokens.get("field_list")} FROM \"{args.table_schema}\".\"{args.table}\" '
                                  f'{tokens.get("where_clause")} ORDER BY {tokens.get("keys")}').collect()
                # Convert dataframe to parquet
                table = pa.Table.from_pandas(replace_invalid_values(config_line, table_df))
                # Write file data
                write_parquet_table(table, args.download_mode, tokens.get("data_file_name"), args.download_dir)
    finally:
        cc.close()


def create_config_file(args: argparse.Namespace) -> None:
    """Create file to store all configurations to be run for download data for each table

    Args:
        args: An instance of the Namespace class.

    Returns:
        None
    """
    cc = get_connection_context()
    cursor = cc.connection.cursor()
    config_file = []

    try:
        # Construct the config file name
        config_file_name = f"config_{args.table_schema}_{args.table}"

        # Calculate the maximum record batch size
        record_size = calculate_max_record_batch(cc, args)

        # Add the table schema, table, and record size to the config file dictionary
        config_file.append({})
        config_file[0]["table_schema"] = args.table_schema
        config_file[0]["table"] = args.table
        if record_size and record_size > 0:
            config_file[0]["rec_size"] = record_size

        # Query the table's columns and add them to the config file dictionary
        cursor.execute(f'SELECT "COLUMN_NAME", "INDEX_TYPE", "DATA_TYPE_NAME", "LENGTH", "SCALE" FROM '
                       f'"SYS"."TABLE_COLUMNS" WHERE "SCHEMA_NAME" = \'{args.table_schema}\' AND "TABLE_NAME" = '
                       f'\'{args.table}\' ORDER BY "POSITION"')
        table_fields = cursor.fetchall()
        if table_fields:
            config_file[0]["fields"] = {}
            for fields in table_fields:
                config_file[0]["fields"][fields[0]] = {}
                config_file[0]["fields"][fields[0]]["key"] = "X" if fields[1] != "NONE" else ""
                config_file[0]["fields"][fields[0]]["type"] = fields[2]
                config_file[0]["fields"][fields[0]]["length"] = fields[3]
                config_file[0]["fields"][fields[0]]["scale"] = fields[4]

                # Parse the null treatment JSON configuration, if specified
                if args.null_treatment:
                    try:
                        null_config = json.loads(args.null_treatment)
                    except json.JSONDecodeError:
                        with open(args.null_treatment) as f:
                            null_config = json.load(f)
                    # Parse the null treatment for the current field, if specified
                    for null_except in null_config:
                        if "field" in null_except and fields[0] == null_except["field"]:
                            if "null_const" in null_except:
                                config_file[0]["fields"][fields[0]]["null_const"] = null_except["null_const"]
                            if "nulls" in null_except:
                                config_file[0]["fields"][fields[0]]["nulls"] = null_except["nulls"]
                            if "not_nulls" in null_except:
                                config_file[0]["fields"][fields[0]]["not_nulls"] = null_except["not_nulls"]

        # Add download directory and mode to the config file dictionary, if specified
        if args.download_dir:
            config_file[0]["download_dir"] = args.download_dir
        if args.download_mode:
            config_file[0]["download_mode"] = args.download_mode

        # If group fields are specified, split the table into groups and create a separate config file for each group
        if args.group:
            group_fields_string = ",".join([f'"{item}"' for item in args.group])
            cursor.execute(f"SELECT DISTINCT {group_fields_string} FROM {args.table_schema}.{args.table}")
            # table_grouping_validation = cc.sql(f"SELECT SUM(EXCEED_LIMIT)*100/SUM(GRAND_TOTAL) FROM (SELECT {
            # group_fields_string}, 1 AS GRAND_TOTAL, (CASE WHEN RECORD_COUNT > RECORD_LIMIT THEN 1 ELSE 0 END) AS
            # EXCEED_LIMIT FROM (SELECT {group_fields_string}, COUNT(*) AS RECORD_COUNT, {record_size} AS
            # RECORD_LIMIT FROM {args.table_schema}.{args.table} GROUP BY {group_fields_string}))") Pop up message to
            # validate if continue or not
            grouping_values = cursor.fetchall()
            for unique_key in tqdm(grouping_values, desc='Splitting in several configuration files'):
                final_config = copy.deepcopy(config_file)
                final_file_name = config_file_name
                for name, value in zip(args.group, unique_key):
                    if "grouping" not in final_config[0]:
                        final_config[0]["grouping"] = []
                    final_config[0]["grouping"].append({"field": name, "value": value})
                    final_file_name += f"_{name}-{value}"
                # Write config file
                with open(os.path.join(args.config_dir, f"{final_file_name}.json"), "w") as f:
                    json.dump(final_config, f, indent=4)
        else:
            # Write config file
            with open(os.path.join(args.config_dir, f"config_{args.table_schema}_{args.table}.json"), "w") as f:
                json.dump(config_file, f, indent=4)

    finally:
        cursor.close()

def main():
    # Parse the command line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", type=str, choices=["download", "configure"],
                        help="Mode or action to run the program")
    parser.add_argument("--config_dir", "-cd", type=str,
                        help="Directory where the configuration files are written or read it from")
    parser.add_argument("--table", "-t", type=str, help="Table to work with")
    parser.add_argument("--table_schema", "-ts", type=str, help="DB Schema of the table to work with")
    parser.add_argument("--limit_mode", "-lm", type=str, choices=["records", "B", "KB", "MB", "GB", "TB"],
                        help="How to calculate the batch size of the parquets files")
    parser.add_argument("--limit_num", "-ln", type=int, help="Size of the batch of the parquet files")
    parser.add_argument("--group", "-g", nargs="+", help="List of fields to make a grouping and splitting files")
    parser.add_argument("--null_treatment", "-nt",
                        help="Text in json format or path to json file where there is a specification of each field, "
                             "tha values that represent null and a static value for replacement")
    parser.add_argument("--download_dir", "-dd", type=str, help="Path where the data files will be downloaded")
    parser.add_argument("--download_mode", "-dm", choices=["local", "GCP_cloud_storage"],
                        help="File system or bucket where the files will be written")

    args = parser.parse_args()

    check_tilde_expansion(args)
    # Check which of the modes will be executed
    if args.mode == "configure":
        # Validate the required arguments
        if not args.config_dir:
            parser.error("The --config_dir flag is required as the path to write the configuration files")
        if not args.table or not args.table_schema:
            parser.error(
                "Both --table and --table_schema flags are required to specify for the configuration file creation")
        if (args.limit_num and not args.limit_mode) or (not args.limit_num and args.limit_mode):
            parser.error("Both --limit_mode and --limit_num must be specified")

        # Create the directory if it doesn't exist
        create_directory_if_not_exist(args.config_dir)

        create_config_file(args)
    elif args.mode == "download":
        # Validate the required arguments
        if not args.config_dir and (not args.table or not args.table_schema):
            parser.error("Either the --config_dir flag or the --table and --table_schema must be specified")
        #if not args.config_dir and (not args.limit_mode or not args.limit_num):
        #    parser.error("Either the --config_dir flag or the --limit_mode and --limit_num must be specified")
        if not args.config_dir and (not args.download_dir or not args.download_mode):
            parser.error("The --download_dir and --download_mode must be specified")

        # Loop through each file in the directory
        if os.path.isdir(args.config_dir):
            for filename in tqdm(os.listdir(args.config_dir), desc='Parsing tables'):
                if filename.endswith(".json"):
                    file_path = os.path.join(args.config_dir, filename)
                    with open(file_path) as f:
                        config = json.load(f)
                        execute_configuration(config, parser, args)
        else:
            with open(args.config_dir) as f:
                config = json.load(f)
                execute_configuration(config, parser, args)


if __name__ == "__main__":
    main()
