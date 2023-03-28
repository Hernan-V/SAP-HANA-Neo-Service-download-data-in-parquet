# SAP HANA Data Downloader

## Overview

The SAP HANA Data Downloader is a Python script that allows you to download data from SAP HANA databases and store it in parquet files. This can be useful when working with large datasets that are too big to be stored in memory or when you want to work with data in a format that can be easily shared and analyzed.

Overall, the SAPHANANeo_data_downloader script provides a powerful and flexible tool for downloading and processing data from SAP HANA systems, allowing users to customize the download and processing process to fit their specific needs and requirements.

## Pre-Requisites
- Install Python 3 and all the libraries present in requirements.txt
- Download and Install the SAP BTP Neo SDK from [here](https://tools.eu1.hana.ondemand.com/#cloud) and start it with the BTP Sub-Account info
  - Take into account the Java Version that needs the SDK. You could use a JVM given by SAP [here](https://tools.eu1.hana.ondemand.com/#cloud)
  When the tunnel is started using the SAP BTP Neo SDK, the DB instance in the cloud is map to your __localhost__ with a specific port. Generally the first connection will take the port `30015`. Take note of this port to set an environment variable.
- Set the credentials, host and port to connct to the SAP HANA DB instance as a environment variables as the next example (with the exact same names):
```bash
export SAP_HANA_HOST=<localhost>
export SAP_HANA_PORT=<300xx>
export SAP_HANA_USER=<DB User>
export SAP_HANA_PASSWORD=<Password of DB user>
```

> **Note:**
> To be sure that the python script will recognize the environment variables and be independent of any terminal session, add the variables to your `~/.bashrc` file and then execute a `source ~/.bashrc`.
> Update the port and credentials if needed on the file.

## Usage

The SAP HANA Data Downloader can be used in two ways: to configure the data download and to download the data.

### Configuration
These means create several json file that will stored the behaviour, parameters and the origin of the data to be download.
The configuration file approach is to decouple the process from a manual input and automate the extraction.
To configure the data download, you can call the script with the "configure" parameter followed by the following options:

- `--config_dir` or `-cd`: The directory where the configuration files will be stored.
- `--table` or `-t`: The name of the table to be downloaded.
- `--table_schema` or `-ts`: The schema of the table to be downloaded.
- `--limit_mode` or `-lm`: If the data have to be splitted in chunks because a practise, volume, network quota or performance, with these parameter can calculate these _batches_. These is optional and the possible values are `["records", "B", "KB", "MB", "GB", "TB"]`. It's like the size unit of the batch.
- `--limit_num` or `-ln`: If the `--limit_mode` flag was set, here you should set the size of the batch as an integer number.
- `--group` or `-g`: The fields used to group the data files. It's a way to generate all the data of certain subset toghether. It can be use with or without the `--limit_mode` flag.
- `--null_treatment` or `-nt`: The treatment to apply to null values. This can be provided as a string in JSON format or the path to a JSON file that contains the rule. These is optional.
  - The structure of the json file could be separated in:
    - A curly brace pair for each field of the table (or objects)
    - For each object (aka field) the next key valu pairs:
      - At the `field` key complete the name of the field of the table as a string
      - The `null_const` also expects a string for the constant to be used as a replacement for all null values
      - The `nulls` key expect an array of regular expressions as string. The script will recognize each matching value of these regex in the `field` informed and replace it with the `null_const`.
      - Finally there is the key `not_nulls` that can be use togheter or instead the `nulls`. Because the nature of listing the null values needed to recognize what values are present to treat them as null, sometimes more feasible to explain what is the rule to be a expected _"good"_ value.
      These key expect also a array of string, and each value is a regular expression.
      Then the script will compare each data value of the dataset for the column (aka field) and compare to all the values on these array with the AND operator, meaning that all the expressions must be true in order to pass, otherwise it will be replaced by the value of `null_const`
      See an example [here](/samples/example_null_treatment_ABAP_date.json)
- `--download_dir` or `-t`: The directory where the downloaded data will be stored.
- `--download_mode` or `-t`: The download mode. This can be either "local" ~~or "cloud"~~ .

Example:
```bash
python HANA_downloader.py "configure" --config_dir "~/test/configuration" \
--table "T001" --table_schema "ECC" --group MANDT LAND1 \
--null_treatment "[{'field': 'FMHRDATE','null_const': '19000101','nulls': ['^0*$','^.*\s+$'],'not_nulls': ['^(19\d\d|20[0-2]\d)(0[1-9]|1[012])(0[1-9]|[12]\d|3[01])$']}]" \
--download_dir "~/test/data" --download_mode local
```

### Data Download
To download the data stored in a table, you can call the script with the "download" parameter followed by the following options:

- `--table` or `-t`: The name of the table to be downloaded.
- `--table_schema` or `-ts`: The schema of the table to be downloaded.
- `--limit_mode` or `-lm`: If the data have to be splitted in chunks because a practise, volume, network quota or performance, with these parameter can calculate these _batches_.
- `--limit_num` or `-ln`: If the `--limit_mode` flag was set, here you should set the size of the batch as an integer number.
- `--download_dir` or `-t`: The directory where the downloaded data will be stored.
- `--download_mode` or `-t`: The download mode. This can be either "local" ~~or "cloud"~~ .
- `--config_dir` or `-cd`: The directory where the configuration files are stored.
  This argument could be the path of a directory or even a single file.
  If it is populated, then the rest of the flags are optional and will be overwrite with the correspond value present on the configuration file.

Example:
```bash
python HANA_downloader.py "download" --config_dir "~/test/configuration"
```
#### Results

Once the data is downloaded, it will be stored in parquet files that are compressed with snappy. The name of the files will follow the pattern `data_<table_schema>_<table>_[<grouped_fields-values>]_<timestamp>.parquet.snappy`.

You can use the `parq` CLI to inspect the parquet files. For example:
```bash
parq data_ECC_T001_20230317002353886538.parquet.snappy
```
You can also view the schema of the parquet file with:
```bash
parq data_ECC_T001_20230317002353886538.parquet.snappy -s
```
And view part of the data with:
```bash
parq data_ECC_T001_20230317002353886538.parquet.snappy -head
```
## Notes

## Future features or TODO's
- [ ] Add the option to store the data to a bucket in the cloud
- [ ] Create it as a Docker image
- [ ] Add a checksum to see if the data differe from when the configuration file was created and the data present on the system

