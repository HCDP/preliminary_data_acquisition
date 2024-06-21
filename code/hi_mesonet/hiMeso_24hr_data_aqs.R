#this code grabs all hi mesonet station data for the past day and saves it into a raw all station long format table
# author: matthew lucas mplucas@hawaii.edu - 19 sep 2022

rm(list = ls())#remove all objects in R

#packages
#install.packages("tidyr")
library(tidyr)

Sys.setenv(TZ='Pacific/Honolulu') #set TZ to honolulu

#set MAIN DIR
mainDir <- "/home/hawaii_climate_products_container/preliminary"

#output dirs
parse_wd<-paste0(mainDir,"/data_aqs/data_outputs/hi_mesonet/parse")

#set options
print(paste("mesonet 24hr webscape run:",Sys.time()))#for cron log

#custom functions
rbind.all.columns <- function(x, y) {     #function to smart rbind
  if(nrow(x)==0 | nrow(y)==0){
    return(rbind(x, y))
  }else{
    x.diff <- setdiff(colnames(x), colnames(y))
    y.diff <- setdiff(colnames(y), colnames(x))
    x[, c(as.character(y.diff))] <- NA 
    y[, c(as.character(x.diff))] <- NA 
    return(rbind(x, y))}
}

readMetData<-function(metDataFile,sysFile=F){
  header<-read.csv(metDataFile,nrows=4,header=F,stringsAsFactors=FALSE)
  staName<-as.character(header[1,2])
  sta_ID<-as.character(substr(staName,1,4))
  colNames<-as.character(header[2,])
  df<-read.csv(metDataFile, skip = 4, header = F,stringsAsFactors=FALSE)
  if(sysFile){
    colNames<-colNames[!(colNames=="" | colNames=="NA")]
    df<-df[,1:length(colNames)]
  }
  names(df)<-colNames
  df<-data.frame(staName=as.character(rep(staName,nrow(df))),sta_ID=as.character(rep(sta_ID,nrow(df))),df)
  return(df)
}

getMetDataDatesLong<-function(station,startDate,endDate,longOut=FALSE){
  startDate<-as.Date(startDate)
  endDate<-as.Date(endDate)
  if(startDate>endDate){stop("ERROR: start date is more recent then end date!")}
  getDates<-rev(seq.Date(startDate,endDate,by="days"))
  if(length(station)>1){
    metDataFile<-as.character()
    for(j in 1:length(station)){
      metDataFile<-c(metDataFile,paste0("https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw",format(getDates,"/%Y/%m/%d/"),station[j],"_MetData.dat"))
    }
  }else{      
    metDataFile<-paste0("https://ikeauth.its.hawaii.edu/files/v2/download/public/system/ikewai-annotated-data/HCDP/raw",format(getDates,"/%Y/%m/%d/"),station,"_MetData.dat")
  }
  multiMetDF<-data.frame()
  for(i in 1:length(metDataFile)){
    urlCheck<-tryCatch({nrow(readMetData(metDataFile[i]))>0}, error = function(e) as.logical(0))
    if(urlCheck){
      metDF<-readMetData(metDataFile[i])
      metDF_long<-gather(metDF, key="var",value="value", 5:ncol(metDF))
      multiMetDF<-rbind(multiMetDF,metDF_long)
      message(paste(rev(unlist(strsplit(metDataFile[i], split = "/")))[1],paste(rev(unlist(strsplit(metDataFile[i], split = "/")))[2:4],collapse="-"),"found and appended!"))
    }else{
      noDataDF<-data.frame()
      multiMetDF<-rbind(multiMetDF,noDataDF)
      message(paste(rev(unlist(strsplit(metDataFile[i], split = "/")))[1],paste(rev(unlist(strsplit(metDataFile[i], split = "/")))[2:4],collapse="-"), "not found!"))
    }
  }#end file day loop
  if(nrow(multiMetDF)>0){
    return(multiMetDF)
  }else{
    stop("no data found for station on requested dates!")
  }
}


#get and make station urls
himeso<-read.csv("https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/hi_mesonet_sta_status.csv")
str(himeso)

#define date
dataDate<-Sys.Date()-1

#get data and make long 
all_hiMeso<-getMetDataDatesLong(station = himeso$staName,startDate=dataDate ,endDate=dataDate)
head(all_hiMeso)
tail(all_hiMeso)

#write data to file
if(nrow(all_hiMeso)>0){
  setwd(parse_wd)#sever path for parsed hiMeso files
  hiMeso_filename<-paste0(format((Sys.Date()-1),"%Y%m%d"),"_hiMeso_parsed.csv") #dynamic file name that includes date
  write.csv(all_hiMeso,hiMeso_filename, row.names=F)
  print("pased all data table saved...")
  }else{
  message(paste(dataDate, "no station data found no data saved..."))
}

#pau
print(paste("mesonet 24hr webscape end:",Sys.time()))#for cron log
