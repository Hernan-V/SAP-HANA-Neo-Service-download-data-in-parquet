This application can be containerized using following command:

```bash
docker build -t HANADownload_image
```

If above command finishes successfully, following command would show a new image with the latest version of data
validation application.

```bash
docker images
```

You can also build the image with the BTP sub-account connection login data as:

```bash
docker build -t HANADownload_image --build-arg BTP_SUBACCOUNT_HOST=${BTP_SUBACCOUNT_HOST} \
      BTP_SUBACCOUNT_USER=${BTP_SUBACCOUNT_USER} \
      BTP_SUBACCOUNT_PASSWORD=${BTP_SUBACCOUNT_PASSWORD} \
      BTP_SUBACCOUNT_ID=${BTP_SUBACCOUNT_ID} \
      BTP_SUBACCOUNT_DB=${BTP_SUBACCOUNT_DB}
```

Once your confirm that image of the HANA Downloader is available. You can run following command to run HANA Downloader
tool as a docker container.

```bash
docker run --name hana-download \ 
-v ~/example/config/T001:/config \ 
-v ~/example/data/T001:/data \ 
-v ~/example/nulls:/nulls \ 
HANADownload_image python SAPHANANeo_data_downloader.py \
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
HANADownload_image python SAPHANANeo_data_downloader.py \
"download" --config_dir "/config"
```
