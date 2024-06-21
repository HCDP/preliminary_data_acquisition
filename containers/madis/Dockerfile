FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the hads code
ADD /code/madis /home/hawaii_climate_products_container/preliminary/data_aqs/code/madis

# Create the data output folder
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/madis/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/madis/raw

# Copy the upload configuration.
ADD /containers/madis/upload_config.json /sync/upload_config.json

# Copy the task definition script.
ADD /containers/madis/task.sh /actor/task.sh

CMD [ "/bin/bash", "/actor/task.sh" ]