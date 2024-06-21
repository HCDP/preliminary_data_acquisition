# This code is used to fetch and save all hi mesonet data served up in realtime by the synoptic api

rm(list = ls())
library(lubridate)
library(tidyr)

s<-Sys.time()

#set dirs
mainDir <- "/home/hawaii_climate_products_container/preliminary"
codeDir<-paste0(mainDir,"/data_aqs/code/source")
rawOutDir<-paste0(mainDir,"/data_aqs/data_outputs/mesonetSynoptic/raw")
parseOutDir<-paste0(mainDir,"/data_aqs/data_outputs/mesonetSynoptic/parse")

#custom functions
readSynUrl <- function(url) {
  out <- tryCatch(
    { vars <- read.csv(url, skip = 6, header = F, nrows = 1, as.is = T)
      stadf<-read.csv(url, skip = 9, header = F)
      colnames(stadf) <- vars
      stadf
      #message("data downloaded dataframe made!")
      #print(stadf)
      },
    error=function(cond) {
      message(paste("api url broke:", url))
      message(cond)
      # Choose a return value in case of error
      return(NA)
    }
  )    
  return(out)
}

#define date
source(paste0(codeDir,"/dataDateFunc.R"))
dataDate<-dataDateMkr() #function for importing/defining date as input or as yesterday
dataDateName<-format(dataDate,"%Y%m%d")
dtstart<-as.POSIXct(paste(dataDate,"00:00:00"), tz="HST") #make start date time HST
dtend<-as.POSIXct(paste(dataDate+1,"00:00:00"), tz="HST") #make end date time HST

#convert to UTCC for API
attr(dtstart, "tzone") <- "UTC"
attr(dtend, "tzone") <- "UTC"

#reformat for API
dtstart<-format(dtstart,"%Y%m%d%H%M")
dtend<-format(dtend,"%Y%m%d%H%M")

#get metadata
meta_url <- "https://raw.githubusercontent.com/ikewai/hawaii_wx_station_mgmt_container/main/Hawaii_Master_Station_Meta.csv"
geog_meta<-read.csv(meta_url, colClasses=c("NESDIS.id"="character"))
himesoIDs<-geog_meta[tolower(geog_meta$Observer)=="himesonet" & !is.na(geog_meta$NWS.id),"NWS.id"]


#get syno api token from environment
synoToken<-Sys.getenv("SYNOPTIC_TOKEN")

#set up station loop outputs
daynames<-as.character()
allMesoData<-data.frame()

#loop through ID to get all data
for(i in 1:length(himesoIDs)){
  dataURL<-paste0("https://api.mesowest.net/v2/station/timeseries?&stid=",himesoIDs[i],"&start=",dtstart,"&end=",dtend,"&output=csv&token=",synoToken)
 
  #get parsed data
  stadf<-readSynUrl(url=dataURL)
  #head(stadf)
  
  if(is.data.frame(stadf)){ #add station df if not NA (no api error)
    #make data long
    stadfLong<-gather(stadf, "var", "value", -Station_ID,-Date_Time)
    allMesoData<-rbind(allMesoData,stadfLong)
  
    #get raw data
    rawData<-readLines(dataURL)
  
    #write unparse day/station file
    setwd(rawOutDir)
    rawName<-paste0(himesoIDs[i],"_",dataDateName,".txt")
    daynames<-c(daynames,rawName)
    write(rawData,rawName)    
  }else{
    message(paste("non station data found",himesoIDs[i],dataDate))
  }#end na sta return if else
}#end station loop

#zip all station day files and remove raw station data
if(length(daynames)>0){
  setwd(rawOutDir)
  zip(zipfile = paste0(dtstart,"_api_synoptic_raw"), files = daynames)
  unlink(daynames)#delete station day files
}

#save all station/day files long

if(nrow(allMesoData)>0){
  setwd(parseOutDir)
  write.csv(allMesoData,paste0(dataDateName,"_himeso_synoptic.csv"),row.names = F)
}#end parse row check/write

e<-Sys.time()
print(e-s)
#PAU#
