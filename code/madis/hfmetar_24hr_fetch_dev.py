import os
import warnings
import subprocess
import ftplib
import pytz
import pandas as pd
import xarray as xr
import numpy as np
from os.path import exists
from datetime import datetime,timedelta
from xarray import SerializationWarning

warnings.filterwarnings('ignore',category=SerializationWarning)
#DEFINE CONSTANTS-------------------------------------------------------------
META_VAR_KEYS = ['stationId','stationName','dataProvider','observationTime']
DATA_VAR_DICT = {'temperature':'TA','dewpoint':'TD','windDir':'DS','windSpeed':'US','windGust':'GS','rawPrecip':'PR','precipAccum':'PC'}
DF_COLS = ['stationId','stationName','dataProvider','time','varname','value','units','source']
NO_CONVERSION_KEYS = ['longitude','latitude','elevation','windDir','windSpeed','windGust','rawPrecip','precipAccum']
K_CONVERSION_KEYS = ['temperature','dewpoint']
STR_CONVERSION_KEYS = ['stationId','stationName','dataProvider']
STR_FMT = "UTF-8"
MIN_LON = -160
MAX_LON = -154
MIN_LAT = 18
MAX_LAT = 22.5
K_CONST = 273.15
PARSED_DIR = r'/home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/madis/parse/'
#END CONSTANTS----------------------------------------------------------------

#DEFINE FUNCTIONS-------------------------------------------------------------
def get_units(unit_dict,ds):
    for vk in list(DATA_VAR_DICT.keys()):
        unit_dict[vk] = ds[vk].units
    
    for temp_key in K_CONVERSION_KEYS:
        unit_dict[temp_key] = 'celsius'

def extract_values(var_stack,ds,extract_vars,subset_index=None):
    for vname in extract_vars:
        if subset_index is None:
            converted_array = ds[vname].values
        else:
            converted_array = ds[vname].values[subset_index]
        var_stack[vname] = converted_array

def convert_K2C(var_stack,ds,kelvin_vars,subset_index=None):
    #var_stack should pass by reference, assuming it works correctly
    for kv in kelvin_vars:
        if subset_index is None:
            converted_array = ds[kv].values - K_CONST
        else:
            converted_array = ds[kv].values[subset_index] - K_CONST
        var_stack[kv] = converted_array

def convert_str(var_stack,ds,str_vars,subset_index=None):
    for sv in str_vars:
        if subset_index is None:
            converted_array = ds[sv].str.decode(STR_FMT).values
        else:
            converted_array = ds[sv].str.decode(STR_FMT).values[subset_index]
        var_stack[sv] = converted_array

def convert_time(var_stack,ds,subset_index=None):
    time = pd.to_datetime(ds['observationTime'].values).strftime('%Y-%m-%d %H:%M:%S')
    if subset_index is not None:
        time = time[subset_index]
    var_stack['observationTime'] = time

def process_metar_data(ds,source):
    #Create subset index
    lon = ds['longitude']
    lat = ds['latitude']

    loni = np.where((lon>=MIN_LON) & (lon<=MAX_LON))
    lati = np.where((lat>=MIN_LAT) & (lat<=MAX_LAT))
    hii = np.intersect1d(loni,lati)

    unit_dict = {}
    converted_dict = {}
    get_units(unit_dict,ds)
    extract_values(converted_dict,ds,NO_CONVERSION_KEYS,hii)
    convert_K2C(converted_dict,ds,K_CONVERSION_KEYS,hii)
    convert_str(converted_dict,ds,STR_CONVERSION_KEYS,hii)
    convert_time(converted_dict,ds,hii)
    df = pd.DataFrame(columns=DF_COLS)
    for vk in list(DATA_VAR_DICT.keys()):
        meta_group = [[converted_dict[key][index] for key in META_VAR_KEYS] for index in range(len(hii))]
        varname = ((vk+' ')*len(hii)).split()
        units = ((unit_dict[vk]+' ')*len(hii)).split()
        src_col = [source for i in range(len(hii))]
        var_group = [[varname[index],converted_dict[vk][index],units[index],src_col[index]] for index in range(len(hii))]
        full_group = [meta_group[index]+var_group[index] for index in range(len(hii))]
        var_df = pd.DataFrame(full_group,columns=DF_COLS)
        df = pd.concat([df,var_df])
    
    #Clear all no-data values
    df = df[~df['value'].isna()]
    return df

def update_csv(csvname,new_df):
    if exists(csvname):
        prev_df = pd.read_csv(csvname)
        upd_df = pd.concat([new_df,prev_df],axis=0,ignore_index=True)
        upd_df.to_csv(csvname,index=False)
    else:
        new_df.to_csv(csvname,index=False)
#END FUNCTIONS----------------------------------------------------------------

#FTP info
src_prefix = 'hfmetar'
ftplink = "madis-data.ncep.noaa.gov"
ftp_dir = "/LDAD/hfmetar/netCDF/"
ftp_user = 'anonymous'
ftp_pass = 'anonymous'

#Get filenames in correct time zone
hst = pytz.timezone('HST')
if len(sys.argv) > 1:
        date_str = sys.argv[1]
        prev_day datetime.strptime(date_str, '%Y-%m-%d')
else:
        today = datetime.today().astimezone(hst)
        prev_day = today - timedelta(days=1)


time_st = pd.to_datetime(datetime(prev_day.year,prev_day.month,prev_day.day,0))
time_en = pd.to_datetime(datetime(prev_day.year,prev_day.month,prev_day.day,23))
hst_times = pd.date_range(time_st,time_en,freq='1H',tz='HST')
utc_times = hst_times.tz_convert(tz=None)

prev_day_files = [dt.strftime('%Y%m%d_%H%M')+'.gz' for dt in utc_times]

prev_day_str = prev_day.strftime('%Y-%m-%d')
prev_day_year = prev_day_str.split('-')[0]
prev_day_mon = prev_day_str.split('-')[1]
prev_day_day = prev_day_str.split('-')[2]
csv_name = PARSED_DIR + '_'.join((prev_day.strftime('%Y%m%d'),'madis','parsed')) + '.csv'
#Open FTP connection
with ftplib.FTP(ftplink,ftp_user,ftp_pass) as ftp:
    ftp.cwd(ftp_dir)
    ftp_files = ftp.nlst()
    avail_files = [fname for fname in prev_day_files if fname in ftp_files]
    unavail_files = [fname for fname in prev_day_files if fname not in ftp_files]
    print("Unavailable files:",unavail_files)
    for fname in avail_files:
        local_name = '_'.join((src_prefix,fname))
        print('Downloading',fname,'as',local_name)
        with open(local_name,'wb') as f:
            ftp.retrbinary('RETR %s' % fname,f.write)
        #Gunzip file
        command = "gunzip " + local_name
        res = subprocess.call(command,shell=True)
        #Open the netcdf and get requisite variables
        new_local_name = local_name.split('.')[0] #assumes the filename convention follows prefix_YYYYMMDD_HHMM.gz
        ds = xr.open_dataset(new_local_name)
        #Don't do timezone reconversion yet, just make sure it can get into the csv properly
        df = process_metar_data(ds,src_prefix)
        update_csv(csv_name,df)
        os.remove(new_local_name)
