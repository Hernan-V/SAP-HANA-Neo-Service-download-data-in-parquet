This application can be containerized using following command:

```bash
docker build -t hana-download-image .
```

If the above command finishes successfully, the following command can be executed to display a new image with the latest version of the HANA Downloader application:

```bash
docker images
```

The image can also be built with the BTP sub-account connection login data by running the following command:

```bash
docker build -t hana-download-image . \
--build-arg BTP_SUBACCOUNT_HOST=${BTP_SUBACCOUNT_HOST} \
--build-arg BTP_SUBACCOUNT_USER=${BTP_SUBACCOUNT_USER} \
--build-arg BTP_SUBACCOUNT_PASSWORD=${BTP_SUBACCOUNT_PASSWORD} \
--build-arg BTP_SUBACCOUNT_ID=${BTP_SUBACCOUNT_ID} \
--build-arg BTP_SUBACCOUNT_DB=${BTP_SUBACCOUNT_DB}
```
Alternatively, the sub-account information can be passed as environment variables on the `docker run` command.

Once the image of the HANA Downloader is available, execute the following command to run the HANA Downloader tool as a Docker container:

```bash
docker run --name hana-download \ 
-v ~/example/config/T001:/config \ 
-v ~/example/data/T001:/data \ 
-v ~/example/nulls:/nulls \
-e SAP_HANA_HOST=${SAP_HANA_HOST} \
-e SAP_HANA_USER=${SAP_HANA_USER} \
-e SAP_HANA_PASSWORD=${SAP_HANA_PASSWORD} \
-e SAP_HANA_PORT=${SAP_HANA_PORT} \ 
hana-download-image python SAPHANANeo_data_downloader.py \
"configure" --config_dir "/config" \
--table "T001" --table_schema "ECC" --group MANDT LAND1 \
--null_treatment "/nulls/T001_nulls.json" \
--download_dir "/data" --download_mode local 
```

OR

```bash
docker run --name hana-download \ 
-v ~/example/config/T001:/config \ 
-v ~/example/data/T001:/data \ 
-v ~/example/nulls:/nulls \ 
-e SAP_HANA_HOST=${SAP_HANA_HOST} \
-e SAP_HANA_USER=${SAP_HANA_USER} \
-e SAP_HANA_PASSWORD=${SAP_HANA_PASSWORD} \
-e SAP_HANA_PORT=${SAP_HANA_PORT} \ 
HANADownload_image python SAPHANANeo_data_downloader.py \
"download" --config_dir "/config"
```

Note that in either case, all the necessary environment variables to log in to the SAP HANA server must be passed, 
and all the volumes must be mapped if the `.json` and `parquet` files will be stored outside the container.

You could also use the `docker-compose` file example [here](/docker-compose.yml)