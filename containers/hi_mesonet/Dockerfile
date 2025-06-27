FROM ghcr.io/hcdp/task-preliminary-acquisition-base:latest

# Add the mesonet code
ADD /code/hi_mesonet /workspace/code/hi_mesonet

RUN mkdir -p /workspace/envs
ADD /envs/hi_mesonet/prod.env /workspace/envs

# Create the data output folder
RUN mkdir -p /workspace/data_outputs/hi_mesonet/parse

# Copy the upload configuration.
ADD /containers/hi_mesonet/configs/upload.json /sync/config.json

# Copy the task definition script.
ADD /containers/hi_mesonet/task.sh /workspace/task.sh

WORKDIR /workspace
CMD [ "/bin/bash", "task.sh" ]