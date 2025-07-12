"""
Version 0.1: madis_hourly_aqs test deployment
Objective:
-Run data acquisition from last acquisition through latest complete hour.
Workflow steps:
-Check master log file for last time

*Note: code will acquire at least one hour of data for each time it is launched.
    --At minimum acquires the previous complete hour of data
    --This code will never acquire data for the current incomplete hour
"""
import sys
import os
import pytz
import ftplib
import subprocess
import numpy as np
import pandas as pd
import xarray as xr
from datetime import datetime, timedelta
from os.path import exists

MASTER_LOG = 'master_log_test_new_new.csv'
PARSED_DIR = ''
DATA_STREAM = 'MADIS'
DF_COLS = ['stationId','stationName','dataProvider','time','varname','value','units','source']
META_VAR_KEYS = ['stationId','stationName','dataProvider','observationTime']
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
UTC_OFFSET = -10
LOOKBACK_DAYS = 5


def get_master_status(now):
    if exists(MASTER_LOG):
        if os.stat(MASTER_LOG).st_size != 0:
            #Case: Master log exists and is not empty
            status_df = pd.read_csv(MASTER_LOG)
            #Check the most recent pull time, regardless of success code
            last_time = pd.to_datetime(status_df[status_df['data_stream']==DATA_STREAM]['fetch_time']).values[-1]
            #Pull all other failed pulls.
            if any(status_df['success']==0):
                missing_times = status_df[status_df['success']==0]['target_time'].values
            else:
                missing_times = []
        else:
            #Case: Master log exists but is empty, last run time set to previous hour default, auto-trigger up_to_date=True
            missing_times = []
            last_time = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)     
    else:
        missing_times = []
        last_time = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1) #auto triggers the up_to_date=True condition
    
    hour_before = pd.Timestamp(now).floor('h') - pd.Timedelta(hours=1)
    #Return acquisition list, including all missing times
    aqs_list = list(missing_times)+list([last_time])
    if last_time >= hour_before:
        up_to_date = True
    else:
        up_to_date = False
    return up_to_date,pd.to_datetime(aqs_list)

def update_master_status(now,targ_hour,status):
    columns = ['data_stream','target_time','fetch_time','success']
    if exists(MASTER_LOG):
        if os.stat(MASTER_LOG).st_size != 0:
            #make sure this can append new entry if entry doesn't already exist
            status_df = pd.read_csv(MASTER_LOG)
            new_status = pd.DataFrame([[DATA_STREAM,targ_hour,now.floor('s'),status]],columns=columns)
            status_df = pd.concat([status_df,new_status],axis=0)
            status_df.to_csv(MASTER_LOG,index=False)
        else:
            #If it's empty, no worries about overwriting any important info so just create new file and populate
            status_df = pd.DataFrame([[DATA_STREAM,targ_hour,now.floor('s'),status]],columns=columns)
            status_df.to_csv(MASTER_LOG,index=False)
    else:
        #master log doesn't exist at all so create new and populate
        status_df = pd.DataFrame([[DATA_STREAM,targ_hour,now.floor('s'),status]],columns=columns)
        status_df.to_csv(MASTER_LOG,index=False)
    

#Madis raw netcdf processor functions
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

def process_nc_data(ds,src):
    lon = ds['longitude']
    lat = ds['latitude']

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
    df = pd.DataFrame(columns=DF_COLS)
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
    #concat operation so doesn't need to worry about reading an empty file
    if exists(csvname):
        prev_df = pd.read_csv(csvname)
        upd_df = pd.concat([new_df,prev_df],axis=0,ignore_index=True)
        upd_df = upd_df.drop_duplicates()
        upd_df = upd_df.fillna('NA')
        upd_df.to_csv(csvname,index=False)
    else:
        new_df = new_df.drop_duplicates()
        new_df = new_df.fillna('NA')
        new_df.to_csv(csvname,index=False)

def madis_fetch_hour(hour):
    """
    Functionality: Pulls 1 hour of madis data, extracts Hawaii stations from 
    netcdf and converts to long format pandas dataframe
    Input:
    --hour: Pandas datetime variable for target hour to fetch madis file
    Output:
    --df: All Hawaii observations which took place during the target hour
    """
    ftplink = "madis-data.ncep.noaa.gov"
    ftp_dir = "/LDAD/mesonet/netCDF/"
    ftp_user = 'anonymous'
    ftp_pass = 'anonymous'
    
    #Get filenames by converting hour to utc
    targ_csv = PARSED_DIR + '_'.join((hour.strftime('%Y%m%d'),DATA_STREAM.lower(),'parsed'))+'.csv'
    utc_hour = hour - pd.Timedelta(hours=UTC_OFFSET)
    utc_filename = utc_hour.strftime('%Y%m%d_%H%M')+'.gz'

    #Open ftp connection
    with ftplib.FTP(ftplink,ftp_user,ftp_pass) as ftp:
        for src in list(SRC_VAR_HASH.keys()):
            ftp_dir = "/LDAD/"+src+"/netCDF/"
            ftp.cwd(ftp_dir)
            ftp_files = ftp.nlst()
            if utc_filename in ftp_files:
                local_name = src+utc_filename
                print('Downloading',utc_filename,'as',local_name)
                try:
                    #Wrap into try-except after testing from here---
                    with open(local_name,'wb') as f:
                        ftp.retrbinary('RETR %s' % utc_filename,f.write)
                    #Gunzip file
                    command = "gunzip " + local_name
                    res = subprocess.call(command,shell=True)
                    #Open unzipped file and pass to extraction script
                    new_local_name = local_name.split('.')[0]
                    ds = xr.open_dataset(new_local_name)
                    df = process_nc_data(ds,src)
                    update_csv(targ_csv,df)
                    os.remove(new_local_name)
                    #---to here-------------------------------------
                    return True
                except:
                    print('Error retrieving file')
                    return False
            else:
                print('File not available for',hour.strftime('%Y-%m-%d %H:%M'))
                return False

if __name__=="__main__":
    if len(sys.argv)>1:
        #NOT CURRENTLY ACCEPTING CUSTOM DATETIME ARGUMENTS
        now = sys.argv[1] #YYYY-MM-DDTHH:MM:SS Or "YYYY-MM-DD HH:MM:SS"
        now = pd.to_datetime(now)
        print(now)
    else:
        hst = pytz.timezone('HST')
        now = pd.to_datetime(datetime.now(hst)).tz_localize(None)
        print(now)
    up_to_date_flag, missing_times = get_master_status(now)
    print(up_to_date_flag,missing_times)
    last_run = missing_times[-1]
    print('last_run:',last_run)
    target_time = pd.Timestamp(last_run).floor('h')
    present_hour = pd.Timestamp(now).floor('h') - pd.Timedelta(seconds=1)
    print('Range from',target_time,'to',present_hour)
    lookback_limit = present_hour - pd.Timedelta(hours=LOOKBACK_DAYS*24)
    print("Look back until:",lookback_limit)
    
    if not up_to_date_flag:
        hour_list = list(pd.date_range(start=target_time,end=present_hour,freq='1h'))
        if missing_times.shape[0]>1:
            other_times = [t for t in missing_times if t != last_run]
            hour_list = other_times+hour_list
        
        hour_list = [t for t in hour_list if t > lookback_limit]
        for hr in hour_list:
            print('Fetching data for',hr)
            fetch_flag = madis_fetch_hour(hr)
            update_master_status(now,hr,int(fetch_flag))

    else:
        #Considered up to date, fetch only target time as long as it's not current hour
        if target_time >= present_hour:
            print('Target hour is not complete. Exiting.')
            quit()
        else:
            hour = target_time
            print(hour)
            try:
                fetch_flag = madis_fetch_hour(hour)
                update_master_status(now,target_time,int(fetch_flag))
            except:
                print('Acquisition process failed')
                update_master_status(now,target_time,int(0))

