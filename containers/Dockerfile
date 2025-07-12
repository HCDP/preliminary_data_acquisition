FROM ghcr.io/hcdp/task-base:latest
LABEL org.opencontainers.image.source="https://github.com/HCDP/preliminary_data_acquisition"
LABEL org.opencontainers.image.description="Base Image for data acquisition containers."

# Add pytz dependency (madis)
RUN python3 -m pip install pytz
# Add pandas dependency (madis)
RUN python3 -m pip install pandas
# Add xarray dependency (madis)
RUN python3 -m pip install xarray netcdf4 scipy

# Add R (nws, hads)
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update
RUN apt install -y r-base
RUN R -e 'install.packages("tidyr", repos="http://cran.us.r-project.org")'
RUN R -e 'install.packages("lubridate", repos="http://cran.us.r-project.org")'

RUN mkdir -p /home/hawaii_climate_products_container/preliminary/data_aqs/code/source
ADD /code/source /home/hawaii_climate_products_container/preliminary/data_aqs/code/source

# Pull the latest version of the data uploader.
RUN mkdir /sync
# Does not work with multifile versions, should swap to clone repo
RUN wget https://raw.githubusercontent.com/ikewai/data-uploader/main/upload.py -O /sync/upload.py

# Copy the upload support utilities.
ADD /tools/* /sync
