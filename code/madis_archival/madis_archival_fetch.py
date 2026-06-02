"""
Fetch MADIS archival data
Patch 06.2026
Updated to take only one date as input at a time. Cheesed slightly to allow easy switch back into local batch mode...
Patch 05.2026
Converting ftp process to http via requests
"""
import sys
import os
import warnings
import requests
import gzip
import io
import pandas as pd
import xarray as xr
import numpy as np
from os.path import exists
from datetime import timedelta
from xarray import SerializationWarning
from itertools import product
from util import handle_retry

warnings.filterwarnings('ignore',category=SerializationWarning)
#DEFINE CONSTANTS-------------------------------------------------------------
##[OUTPUT_DIR] defines target directory for parsed data csv. 
OUTPUT_DIR = '/home/hawaii_climate_products_container/preliminary/data_aqs/data_outputs/madis/parse'
#OUTPUT_DIR = './'
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
    df_list = []
    avail_var_keys = [vk for vk in data_var_keys if vk in list(ds.keys())]
    for vk in avail_var_keys:
        meta_group = [[converted_dict[key][index] for key in META_VAR_KEYS] for index in range(len(hii))]
        varname = ((vk+' ')*len(hii)).split()
        units = ((unit_dict[vk]+' ')*len(hii)).split()
        src_col = [src for i in range(len(hii))]
        var_group = [[varname[index],converted_dict[vk][index],units[index],src_col[index]] for index in range(len(hii))]
        full_group = [meta_group[index]+var_group[index] for index in range(len(hii))]
        var_df = pd.DataFrame(full_group,columns=DF_COLS)
        df_list.append(var_df)
    df = pd.concat(df_list,axis=0)
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

def fetch_url(url):
    response = requests.get(url, stream=True)
    if response.status_code == 404:
        return None          # file absent — caller skips, no retry
    response.raise_for_status()  # raises on 5xx, triggering handle_retry
    return response

#END FUNCTIONS----------------------------------------------------------------
if __name__== "__main__":
    st_date = sys.argv[1] #%Y-%m-%d
    en_date = sys.argv[1] #%Y-%m-%d
    http_root = "https://madis-data.ncep.noaa.gov/madisPublic1/data/archive/"

    st_dt = pd.to_datetime(st_date)
    en_dt = pd.to_datetime(en_date)
    date_list = pd.date_range(st_dt,en_dt)

    for (dt,src) in product(date_list,SRC_LIST):
        date_str = dt.strftime('%Y-%m-%d')
        print(f"Fetching {src} files for {date_str}.")
        year_str = date_str.split('-')[0]
        mon_str = date_str.split('-')[1]
        day_str = date_str.split('-')[2]
        #Parse timezones. Have to change directories btw
        #for day given by date_str, get the 24 hour marks and then add 10 hours to each
        day_st = pd.to_datetime(date_str)
        day_en = day_st + timedelta(hours=24)
        day_st_utc = day_st + timedelta(hours=10)
        day_en_utc = day_en + timedelta(hours=10)
        
        utc_times = pd.date_range(day_st_utc,day_en_utc,freq='h')
        output_csv = OUTPUT_DIR + '_'.join((day_st.strftime('%Y%m%d'),'madis','parsed')) + '.csv'
        print(f"Saving request data to {output_csv}")
        
        for utc in utc_times:
            year = utc.strftime("%Y")
            mon = utc.strftime("%m")
            day = utc.strftime("%d")
            file_name = utc.strftime('%Y%m%d_%H%M')+'.gz'
            url = os.path.join(http_root,f"{year}/{mon}/{day}/LDAD/{src}/netCDF",file_name)
            #set requests call
            print(f"Trying {src} {file_name}.")
            #After initial test wrap this in handle_retry
            try:
                response = handle_retry(fetch_url,(url,),max_retries=5)
            except Exception as e:
                print(f"Failed to fetch {file_name} after retries: {e}")
                continue
            if response is None:
                #404 response returned
                print("404, no file exists.")
                continue 
            with gzip.open(io.BytesIO(response.content)) as gz:
                ds = xr.open_dataset(gz)
                df = process_madis_data(ds,src)
                update_csv(output_csv,df)
            print(f"CSV updated.")
            


