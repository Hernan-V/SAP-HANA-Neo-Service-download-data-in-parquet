This script automate the download of data in parquet format from SAP HANA tables

## Installation
USAR SNAPPY para COMPRESION

## Set enviroment variables
Agregar en ~/.bashrc y luego recargar con
source ~/.bashrc

```bash
export BTP_SUBACCOUNT_USER=<BTP Account User>
export BTP_SUBACCOUNT_PASSWORD=<BTP User password>
export BTP_SUBACCOUNT_ID=<Sub Account>
export BTP_SUBACCOUNT_DB=<DB Instance>
export BTP_SUBACCOUNT_HOST=<region>.hana.ondemand.com
./neo.sh open-db-tunnel -h $BTP_SUBACCOUNT_HOST -u $BTP_SUBACCOUNT_USER -p $BTP_SUBACCOUNT_PASSWORD -a $BTP_SUBACCOUNT_ID -i $BTP_SUBACCOUNT_DB --background
```

```bash
export SAP_HANA_HOST=<localhost>
export SAP_HANA_PORT=<300xx>
export SAP_HANA_USER=<DB User>
export SAP_HANA_PASSWORD=<Password of DB user>
```

IF special charaters use \"\" or add special exit character like \\

## Configuration File output

### Example
```json
[
  {
    "table": "MARA",
    "schema": "ECC",
    "rec_size": 1444,
    "grouping":[
      {
        "field": "MTART",
        "value": "ZA10"
      },
      {
        "field": "SPART",
        "value": "10"
      }
    ],
    "fields":{
      "MATNR": {
        "key": "X",
        "type": "NVARCHAR",
        "length":"18",
        "scale":0,
        "null_const":"000000000000000000",
        "nulls":["","^\\s*$"],
        "not_nulls":["^\\d{18}$"]
      },
      "MENGE": {
        "key": "",
        "type": "DECIMAL",
        "length":"15",
        "scale":3
      }
    },
    "download_dir": "/mnt/local/test",
    "download_mode": "local"
  }
]
```

```json
[
  {
    "field": "AEDAT",
    "null_const": "19000101",
    "nulls": ["","00000000"],
    "not_nulls": ["\\regex0030d"],
  },
  {
    "field": "BUKRS",
    "null_const": "0101",
    "nulls": ["","00000000"],
    "not_nulls": ["\\regex0030d"],
  }
]
```
