FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the hads code
ADD /code/hi_mesonet /home/hawaii_climate_products_container/preliminary/data_aqs/code/hi_mesonet

# Create the data output folder
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hi_mesonet/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hi_mesonet/raw
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/mesonetSynoptic/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/mesonetSynoptic/raw

# Copy the upload configuration.
ADD /containers/hi_mesonet/upload_config.json /sync/upload_config.json

# Copy the task definition script.
ADD /containers/hi_mesonet/task.sh /actor/task.sh

CMD [ "/bin/bash", "/actor/task.sh" ]