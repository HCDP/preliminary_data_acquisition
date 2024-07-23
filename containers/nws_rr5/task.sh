#!/bin/bash
echo "[task.sh] [1/4] Starting Execution."

echo "[task.sh] [2/4] Collecting Climate data from NWS on the daily timeframe."
cd /home/hawaii_climate_products_container/preliminary/data_aqs/code/nws_rr5
Rscript nws_rr5_hrly_24hr_webscape.R

echo "[task.sh] [3/4] Injecting authentication variables for uploader."
cd /sync
python3 inject_upload_config.py config.json $CUSTOM_DATE

echo "[task.sh] [4/4] Attempting to upload the gathered data."
python3 upload.py

echo "[task.sh] All done!"