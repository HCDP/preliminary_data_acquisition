FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the hads code
ADD /code/hi_mesonet_synoptic /home/hawaii_climate_products_container/preliminary/data_aqs/code/hi_mesonet_synoptic

# Create the data output folder
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hi_mesonet_synoptic/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hi_mesonet_synoptic/raw

# Copy the upload configuration.
ADD /containers/hi_mesonet_synoptic/configs/upload.json /sync/config.json

# Copy the task definition script.
ADD /containers/hi_mesonet_synoptic/task.sh /actor/task.sh

CMD [ "/bin/bash", "/actor/task.sh" ]