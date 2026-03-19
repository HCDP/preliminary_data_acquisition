FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the hads code
ADD /code/nws_rr5 /home/hawaii_climate_products_container/preliminary/data_aqs/code/nws_rr5

# Create the data output folder
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/nws_rr5/parse
RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/nws_rr5/raw

# Copy the upload configuration.
ADD /containers/nws_rr5/configs/upload.json /sync/config.json

# Copy the task definition script.
ADD /containers/nws_rr5/task.sh /actor/task.sh

CMD [ "/bin/bash", "/actor/task.sh" ]