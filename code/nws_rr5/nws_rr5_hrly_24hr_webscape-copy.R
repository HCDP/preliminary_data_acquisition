#this  code grabs 24 hr rainfall total from NWS site from the midnight data upload

rm(list = ls())#remove all objects in R

#set MAIN DIR
mainDir <- "/home/hawaii_climate_products_container/preliminary"

#packages and settings
require(plyr)
options(warn=-1)#supress warnings for session
print(paste("nws hrly data daily scrape run:",Sys.time()))#for cron log

#custom functions
substrRight <- function(x, n){substr(x, nchar(x)-n+1, nchar(x))}

#output dirs
raw_page_wd<-paste0(mainDir,"/data_aqs/data_outputs/nws_rr5/raw")
parse_hrly_wd<-paste0(mainDir,"/data_aqs/data_outputs/nws_rr5/parse")

#blank df to store all DT and V from scrape
dt_df<-data.frame()
S<-Sys.time()
for(i in 1:28){
	url<-paste0("https://forecast.weather.gov/product.php?site=HFO&issuedby=HFO&product=RR5&format=txt&version=",i,"&glossary=0")
	page_check<-readLines(url)
	date_line<-page_check[grep("Hawaii One-Hour Rainfall Summary",page_check)+2] #get dt of page
	if(length(date_line)>0){
	 hourtxt<-trimws(gsub("AM","",gsub("PM","",gsub("HST","",trimws(substrRight(page_check[grep("*One-hour precipitation totals ending at*",page_check)],10))))))#hr text
	 datetxt<-trimws(substrRight(date_line,11))#date txt
	 datetimetxt<-paste(datetxt,hourtxt)#dt txt
	 datetime<-format(as.POSIXct(datetimetxt,format="%b %d %Y %H")-1,"%Y-%m-%d %H:%M")#dt cast as dt
	 date<-as.Date(datetime)
	 row<-data.frame(date=as.character(date),datetime=as.character(datetime),V=i)#save row
	}else{
	 row<-data.frame(date=NA,datetime=NA,V=i)#save row
	 }
	dt_df<-rbind(dt_df,row)#add row to df
	}
E<-Sys.time()
E-S
print(dt_df)

#clean date-time web V table to scape only disired day
undup_dt_df<-dt_df[!duplicated(dt_df$datetime),]#remove duplicate date time
undup_dt_df_nona<-undup_dt_df[!is.na(undup_dt_df$datetime),]#remove NA obs from date time
undup_dt_df_day<-undup_dt_df_nona[as.Date(undup_dt_df_nona$date)==(Sys.Date()-1),]#subset to only dt from yesterday
row.names(undup_dt_df_day)<-NULL #reset rownames (pet-peev)
V_urls<-as.character(undup_dt_df_day$V)

print("hrly datetimes sites found!")

#scape and save raw html text 
setwd(raw_page_wd)#wd to save scraped html

day_dir<-paste0(raw_page_wd,"/nws_hrly_rf_html_",format(Sys.Date()-1,"%Y_%m_%d"))
dir.create(day_dir)

#for loop to scrap all html pages from day of interest
for(i in V_urls){
	url<-paste0("https://forecast.weather.gov/product.php?site=HFO&issuedby=HFO&product=RR5&format=txt&version=",i,"&glossary=0")
	page<-readLines(url)
	page_dt<-as.character(undup_dt_df_day[undup_dt_df_day$V==i,"datetime"])
	obs_dt<-format((as.POSIXct(page_dt)-1),"%Y_%m_%d_%H00")
	write(page,paste0(day_dir,"/NWS_auto_1hr_",obs_dt,".html"))
}

files2zip <- Sys.glob(paths = file.path(day_dir, "*.html")) #list all files to zip and later parse
zip(zipfile = paste0(getwd(),"/nws_hrly_rf_html_",format(Sys.Date()-1,"%Y_%m_%d")), files = files2zip)#zip and/or append raw html files
print("hrly html sites scraped and saved!!")

#parse out hourly data from 24hr scraped files
nws_hrly_data_final<-data.frame(date=character(),start_time=character(),end_time=character(),date_time=character(),nwsli=character(),NWS_name=character(),prec_in_1hr=character(),prec_mm_1hr=character(),stringsAsFactors=FALSE) #blank df to save hrly data

for(i in 1:length(files2zip)){
  htmlfile<-files2zip[i]
	rf_page<-readLines(htmlfile)
	
  #make datetime from file name
	datetimetxt<-gsub(".html","",trimws(substrRight(htmlfile,20)))
	datetime<-as.character(format(as.POSIXct(datetimetxt,format="%Y_%m_%d_%H00"),"%Y-%m-%d-%H"))

 #cut html down to data lines
	data_raw<-rf_page[(grep("*One-hour precipitation totals ending*", rf_page)+1):(grep(".END",page)[1]-1)]
	#print(data_raw)
	if(length(data_raw)>1){
	  #remove non-data lines
	  remove_sites<-grep("*Sites",data_raw) #'Sites' lines to remove
	  data_raw<-data_raw[-remove_sites] #remove 'Sites' lines
	  remove1<-grep("*Island*",data_raw)-1 #start 'Island' lines to remove
	  remove2<-remove1+2 #end 'Island' lines to remove
	  remove_df<-as.data.frame(cbind(remove1,remove2)) #make start end lines to remove df
	  for(i in nrow(remove_df):1){data_raw<-data_raw[-c(remove_df$remove1[i]:remove_df$remove2[i])]}
	  #print(data_raw)
	  #make data lines into data and ID 
	  data_raw<-gsub("  T", "0", data_raw)#make trace rainfall 0.00 inches
	  data_raw<-gsub("  M", " NA", data_raw)#make missing rainfall NA

	  nws_hrly_data<-data.frame(date_time=character(),nwsli=character(), NWS_name=character(), prec_in_1hr=character(),stringsAsFactors=FALSE)#blank df to fill
	  for(j in 1:length(data_raw)){
		  line<-as.vector(do.call('rbind', strsplit(as.character(data_raw[j]),':')))
		  nwsli<-trimws(line[1])
		  NWS_name<-trimws(line[2])
		  data<-as.numeric(trimws(line[3]))
		  sta_obs<-c(datetime,nwsli,NWS_name,data)
		  nws_hrly_data[j,]<-sta_obs
		  }
	  #get drop columns and make columns
	  nws_hrly_data$date<-format((as.POSIXct(nws_hrly_data$date_time,format="%Y-%m-%d-%H")-1),"%Y-%m-%d")
	  nws_hrly_data$start_time<-format((as.POSIXct(nws_hrly_data$date_time,format="%Y-%m-%d-%H")-1),"%H:00")
	  nws_hrly_data$end_time<-format((as.POSIXct(nws_hrly_data$date_time,format="%Y-%m-%d-%H")),"%H:00")
	  nws_hrly_data<-nws_hrly_data[,c("date","start_time","end_time","date_time","nwsli","NWS_name","prec_in_1hr")] 
	  nws_hrly_data$prec_in_1hr<-as.numeric(nws_hrly_data$prec_in_1hr)
	  nws_hrly_data$prec_mm_1hr<-(nws_hrly_data$prec_in_1hr*25.4)
	  nws_hrly_data$date_time<-format((as.POSIXct(nws_hrly_data$date_time,format="%Y-%m-%d-%H")),"%Y-%m-%d %H:%M")
	  nws_hrly_data[nws_hrly_data$end_time=="00:00","end_time"]<-"24:00"
	  nws_hrly_data_final<-rbind(nws_hrly_data_final,nws_hrly_data)
	}
	  }
#print(nws_hrly_data_final)
tail(nws_hrly_data_final)
#str(nws_hrly_data_final)
print("hrly data parsed!!!")

#write or append files
setwd(parse_hrly_wd)
nws_filename<-paste0(format((Sys.Date()-1),"%Y%m%d"),"_nwsrr5_parsed.csv") #dynamic file name that includes date
write.csv(nws_hrly_data_final,nws_filename, row.names=F)
print("24hr of hourly data saved!!!!")

### delete all scaped files
setwd(raw_page_wd)
unlink(files2zip)#deletes all html files in working dir

# ##PAU!!!!!
