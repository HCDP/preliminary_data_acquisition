import sys
import os
import warnings
import subprocess
import ftplib
import pandas as pd
import xarray as xr
import numpy as np
from os.path import exists
from datetime import datetime, timedelta
from xarray import SerializationWarning
from itertools import product

# Madis processor for dates more than 122 hours ago

warnings.filterwarnings('ignore',category=SerializationWarning)
#DEFINE CONSTANTS-------------------------------------------------------------
##[OUTPUT_DIR] defines target directory for parsed data csv. 
#OUTPUT_DIR = '/mnt/lustre/koa/koastore/hawaii_climate_risk_group/kodamak8/close_gap/scratch/'
OUTPUT_DIR = '/home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/madis/parse/'
SRC_LIST = ['mesonet','hfmetar']
DF_COLS = ['stationId','stationName','dataProvider','time','varname','value','units','source']
META_VAR_KEYS = ['stationId','stationName','dataProvider','observationTime']
#Network specific variable names, has to be hardcoded since not every network provides the same set of variables
MESONET_DATA_VARS = ['temperature','dewpoint','relHumidity','stationPressure','seaLevelPressure','windDir','windSpeed','windGust','windDirMax','rawPrecip','precipAccum','solarRadiation','fuelTemperature','fuelMoisture','soilTemperature','soilMoisture','soilMoisturePercent','minTemp24Hour','maxTemp24Hour','windDir10','windSpeed10','windGust10','windDirMax10']
HFMETAR_DATA_VARS = ['temperature','dewpoint','windDir','windSpeed','windGust','rawPrecip','precipAccum']
K_CONVERSION_KEYS = ['temperature','dewpoint','fuelTemperature','soilTemperature','minTemp24Hour','maxTemp24Hour']
STR_CONVERSION_KEYS = ['stationId','stationName','dataProvider']
SRC_VAR_HASH = {'mesonet':MESONET_DATA_VARS,'hfmetar':HFMETAR_DATA_VARS}
LON_KEY = 'longitude'
LAT_KEY = 'latitude'
TIME_KEY = 'observationTime'
STR_FMT = "UTF-8"
MIN_LON = -160 #HI: -160 GU: 144.5
MAX_LON = -154 #HI:-154 GU: 145.117
MIN_LAT = 18 #HI: 18 GU: 13.167
MAX_LAT = 22.5 #HI: 22.5 GU: 13.75
K_CONST = 273.15
#END CONSTANTS----------------------------------------------------------------

#DEFINE FUNCTIONS-------------------------------------------------------------
def get_units(unit_dict,ds,source):
    #Internal function, defines units for preset variables
    var_keys = SRC_VAR_HASH[source]
    avail_vars = [vk for vk in var_keys if vk in list(ds.keys())]
    for vk in avail_vars:
        unit_dict[vk] = ds[vk].units
    
    for temp_key in K_CONVERSION_KEYS:
        if temp_key in avail_vars:
            unit_dict[temp_key] = 'celsius'

def extract_values(var_stack,ds,extract_vars,subset_index=None):
    #Internal function: Extracts all variables to var_stack that don't require conversion
    avail_vars = list(ds.keys())
    for vname in extract_vars:
        if vname in avail_vars:
            if subset_index is None:
                converted_array = ds[vname].values
            else:
                converted_array = ds[vname].values[subset_index]
            var_stack[vname] = converted_array
        
def convert_K2C(var_stack,ds,kelvin_vars,source,subset_index=None):
    #Input note: var_stack should pass by reference, assuming it works correctly
    #Internal function: Converts Kelvin-based variables to celsius and adds to var_stack
    avail_vars = list(ds.keys())
    for kv in kelvin_vars:
        if kv in avail_vars:
            if kv in SRC_VAR_HASH[source]:
                if subset_index is None:
                    converted_array = ds[kv].values - K_CONST
                else:
                    converted_array = ds[kv].values[subset_index] - K_CONST
                var_stack[kv] = converted_array
        
def convert_str(var_stack,ds,str_vars,subset_index=None):
    #Internal function: Converts string binaries to standard string data type
    avail_vars = list(ds.keys())
    for sv in str_vars:
        if sv in avail_vars:
            if subset_index is None:
                converted_array = ds[sv].str.decode(STR_FMT,errors='ignore').values
            else:
                converted_array = ds[sv].str.decode(STR_FMT,errors='ignore').values[subset_index]
            var_stack[sv] = converted_array

def convert_time(var_stack,ds,subset_index=None):
    #Internal function: Converts times to datetime type
    time = pd.to_datetime(ds[TIME_KEY].values).strftime('%Y-%m-%d %H:%M:%S')
    if subset_index is not None:
        time = time[subset_index]
    var_stack[TIME_KEY] = time

def process_madis_data(ds,src):
    #Main data extraction function. Calls all internal conversion functions
    #Expected input: xarray.Dataset pulled directly from MADIS netcdf, no alterations
    #[src] defines whether to apply mesonet or hfmetar variable specifications
    lon = ds[LON_KEY]
    lat = ds[LAT_KEY]
    loni = np.where((lon>=MIN_LON) & (lon<=MAX_LON))
    lati = np.where((lat>=MIN_LAT) & (lat<=MAX_LAT))
    hii = np.intersect1d(loni,lati)

    data_var_keys = SRC_VAR_HASH[src]
    unit_dict = {}
    converted_dict = {}
    get_units(unit_dict,ds,src)
    extract_values(converted_dict,ds,data_var_keys,hii)
    convert_K2C(converted_dict,ds,K_CONVERSION_KEYS,src,hii)
    convert_str(converted_dict,ds,STR_CONVERSION_KEYS,hii)
    convert_time(converted_dict,ds,hii)
    df = pd.DataFrame()
    avail_var_keys = [vk for vk in data_var_keys if vk in list(ds.keys())]
    for vk in avail_var_keys:
        meta_group = [[converted_dict[key][index] for key in META_VAR_KEYS] for index in range(len(hii))]
        varname = ((vk+' ')*len(hii)).split()
        units = ((unit_dict[vk]+' ')*len(hii)).split()
        src_col = [src for i in range(len(hii))]
        var_group = [[varname[index],converted_dict[vk][index],units[index],src_col[index]] for index in range(len(hii))]
        full_group = [meta_group[index]+var_group[index] for index in range(len(hii))]
        var_df = pd.DataFrame(full_group,columns=DF_COLS)
        df = pd.concat([df,var_df])
    
    #Clear all no-data values
    df = df[~df['value'].isna()]
    return df
    
def update_csv(csvname,new_df):
    #Appends processed data to preexisting file if exists, creates new otherwise
    if exists(csvname):
        prev_df = pd.read_csv(csvname)
        upd_df = pd.concat([new_df,prev_df],axis=0,ignore_index=True)
        upd_df = upd_df.drop_duplicates()
        upd_df = upd_df.fillna('NA')
        upd_df.to_csv(csvname,index=False)
    else:
        new_df = new_df.fillna('NA')
        new_df.to_csv(csvname,index=False)

#END FUNCTIONS----------------------------------------------------------------

ftplink = "madis-data.ncep.noaa.gov"
ftp_user = "anonymous"
ftp_pass = "anonymous"

dt = datetime.strptime(sys.argv[1], '%Y-%m-%d')

date_str = dt.strftime('%Y-%m-%d')
year_str = date_str.split('-')[0]
mon_str = date_str.split('-')[1]
day_str = date_str.split('-')[2]
#Parse timezones. Have to change directories btw
#for day given by date_str, get the 24 hour marks and then add 10 hours to each
day_st = pd.to_datetime(date_str)
day_en = day_st + timedelta(hours=24)
day_st_utc = day_st + timedelta(hours=10)
day_en_utc = day_en + timedelta(hours=10)

utc_times = pd.date_range(day_st_utc,day_en_utc,freq='1h')
all_utc_files = [dt.strftime('%Y%m%d_%H%M')+'.gz' for dt in utc_times]
unique_dates = pd.Series(utc_times).map(lambda t: t.date()).unique()
unique_days = ["{:02d}".format(dt.day) for dt in unique_dates]
output_csv = OUTPUT_DIR + '_'.join((day_st.strftime('%Y%m%d'),'madis','parsed')) + '.csv'
#Open the ftp connection but don't change directories until days are set
with ftplib.FTP(ftplink, ftp_user, ftp_pass, timeout = 60) as ftp:
    for (src,udate) in product(SRC_LIST,unique_dates):
        unique_year = udate.strftime('%Y')
        unique_mon = udate.strftime('%m')
        unique_day = udate.strftime('%d')
        ftp_parent_dir = '/'.join(('/archive',unique_year,unique_mon,unique_day,'LDAD'))
        ftp.cwd(ftp_parent_dir)
        ftp_srcs = ftp.nlst()
        if src in ftp_srcs:
            ftp_dir = '/'.join((ftp_parent_dir,src,'netCDF/'))
            print(ftp_dir)
            ftp.cwd(ftp_dir)
            ftp_files = ftp.nlst()
            avail_files = [fname for fname in all_utc_files if fname in ftp_files]
            for fname in avail_files:
                local_name = src + fname
                new_local_name = local_name.split('.')[0]
                print('Downloading',fname,'as',local_name)
                with open(local_name,'wb') as f:
                    ftp.retrbinary('RETR %s' % fname,f.write)
                command = "gunzip " + local_name
                res = subprocess.call(command,shell=True)
                ds = xr.open_dataset(new_local_name)
                df = process_madis_data(ds,src)
                update_csv(output_csv,df)
                os.remove(new_local_name)


