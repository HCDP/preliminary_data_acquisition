import os
import sys
import pytz
import requests
import pandas as pd
from datetime import datetime
from util import handle_retry
from io import StringIO

src_name = "hi_mesonet"
hcdp_api_token = os.environ.get("HCDP_API_TOKEN")
project_root = os.environ.get("PROJECT_ROOT")
meso_id_src = os.environ.get("MESO_TABLE")
url = f"https://api.hcdp.ikewai.org/mesonet/db/measurements"
meso_var_list = ["SWin_1_Avg","SWout_1_Avg","LWin_1_Avg","LWinUC_1_Avg","LWout_1_Avg","LWoutUC_1_Avg","SWnet_1_Avg","LWnet_1_Avg","Rin_1_Avg","Rout_1_Avg","Rnet_1_Avg","RnetUC_1_Avg","Tair_1_Avg","Tair_2_Avg","RH_1_Avg","RH_2_Avg","WS_1_Avg","WDuv_1_Avg","RF_1_Tot300s","RF_2_Tot300s","RFint_1_Max","RFmin_1","RFmin_2"]
output_dir = project_root + "data_outputs/"+src_name+"/parse/"

def fetch_raw_mesonet(targ_date):
    """
    Fetches full day with 15 min end buffer of HImesonet data for all available
     stations in database.
    """
    #Specific start and end datetimes for api call
    targ_date_utc = targ_date + pd.Timedelta(hours=10)
    st_dt = targ_date_utc.strftime('%Y-%m-%d %H:%M:%S')
    en_dt = (targ_date_utc + pd.Timedelta(hours=24) + pd.Timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')
    #set params of api call
    params = {
    'start_date':st_dt,
    'end_date':en_dt,
    'var_ids':','.join(meso_var_list),
    'join_metadata':'true'
    }
    req = requests.get(url,params,headers = {'Authorization': f'Bearer {hcdp_api_token}'}, timeout = 5)
    return req

def format_meso_output(targ_date):
    date_code = targ_date.strftime("%Y%m%d")
    outfile = output_dir + '/' + '_'.join((date_code,src_name,'parsed'))+'.csv'
    req = handle_retry(fetch_raw_mesonet,[targ_date])
    if req.status_code != 200:
        print("Issues encountered in downloading",targ_date)
    else:
        print("Success. Downloaded Mesonet data for",targ_date)
        bytes_data = req.content
        decoded = bytes_data.decode('utf-8')
        req_data = pd.read_json(StringIO(decoded))
        #Get conversion table for switching mesonet id to skn for merge
        convert_table = pd.read_csv(meso_id_src)
        convert_table = convert_table.set_index('sta_ID')['SKN']
        skn_merged = req_data.set_index('station_id').join(convert_table,how='left')
        skn_merged = skn_merged[skn_merged['SKN'].notna()]
        skn_merged = skn_merged.reset_index()
        skn_merged = skn_merged.set_index('timestamp')
        skn_merged.index = skn_merged.index.tz_localize(None)
        skn_merged = skn_merged.reset_index()
        skn_merged.to_csv(outfile,index=False)
        print("Wrote data to",outfile)

if __name__=="__main__":
    if len(sys.argv) > 1:
        date_str = sys.argv[1]
        targ_date = pd.to_datetime(date_str)
    else:
        #If no custom date specified, set target to previous full day
        hst = pytz.timezone("HST")
        today = pd.to_datetime(datetime.today().astimezone(hst)).tz_localize(None)
        targ_date = today - pd.Timedelta(days=1)
    format_meso_output(targ_date)