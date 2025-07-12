FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the hads code
ADD /code/hads /home/hawaii_climate_products_container/preliminary/data_aqs/code/hads

# Create the data output folder
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hads/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/hads/raw

# Copy the upload configuration.
ADD /containers/hads/configs/upload.json /sync/config.json

# Copy the task definition script.
ADD /containers/hads/task.sh /actor/task.sh

CMD [ "/bin/bash", "/actor/task.sh" ]