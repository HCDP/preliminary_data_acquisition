#this code grabs all HADS data for HI, saves multiple copies

rm(list = ls())#remove all objects in R

Sys.Date()


#make dirs
mainDir <- "/home/hawaii_climate_products_container/preliminary"
codeDir<-paste0(mainDir,"/data_aqs/code/source")

#define dates
source(paste0(codeDir,"/dataDateFunc.R"))
dataDate<-dataDateMkr() #function for importing/defining date as input or as yesterday
currentDate<-dataDate #dataDate as currentDate

#set options
options(warn=-1)#supress warnings for session
print(paste("hads_24hr webscape run:",Sys.time()))#for cron log

Sys.setenv(TZ='Pacific/Honolulu') #set TZ to honolulu

#output dirs
raw_page_wd<-paste0(mainDir,"/data_aqs/data_outputs/hads/raw")
parse_wd<-paste0(mainDir,"/data_aqs/data_outputs/hads/parse")

#get data from HADS url
url<-"https://hads.ncep.noaa.gov/nexhads2/servlet/DecodedData?sinceday=2&hsa=nil&state=HI&nesdis_ids=nil&of=1" #this is the URL for all data in website in the last 2 (sinceday)s
page<-readLines(url)

#save raw data
setwd(raw_page_wd)#sever path for un-parsed hads files

page_name<-paste0("hads_hi_page_ending_",format(currentDate,"%Y_%m_%d"),"_HST.txt")
writeLines(page,page_name)
print(paste("web-scrape saved...",page_name))

#make df of all vars and save/append to csv
all_hads<- read.table(textConnection(page),header=F, sep="|") #this reads that HADS url
names(all_hads)<-c("staID","NWS_sid","var","obs_time","value","random","null")
all_hads$null<-NULL
#head(all_hads,10)

#write data to parsed table
setwd(parse_wd)#sever path for parsed hads files
hads_filename<-paste0(format((currentDate),"%Y%m%d"),"_hads_parsed.csv") #dynamic file name that includes date
write.csv(all_hads,hads_filename, row.names=F)
print("pased all data table saved...")

#code pau
print("PAU!")






 
