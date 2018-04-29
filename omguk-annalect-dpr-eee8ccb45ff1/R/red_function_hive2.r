
#' DEPRECATED - PLEASE USE THE GOLD_FUNCTION INSTEAD
#'
#' DEPRECATED - PLEASE USE THE GOLD_FUNCTION INSTEAD
#'
#' @param username The username for redshift
#' @param pw The password for that username
#' @param dbname Database name, defaults to "annalectuk"
#' @param dbHost Database host, defaults to "54.77.118.78"
#' @param KeyID AWS Access Key ID
#' @param SecretAccessKey AWS Secret Access Key
#' @param clust_size AWS size of cluster
#' @param DPID Unique identifier for this DP run. Must not contain spaces or any special characters
#' @param data_source The data source. "DC" for Double Click
#' @param conv_type The conversion type of run you want to do. "C" for standard conversion, "NV" for new visitors, and "NVC" for new visitors conversions. When using "NV" and "NVC" don't forget to specify your prior period with the NVPrior option. Inputing a number between 0 and 1 will perform standard conversion but will sample the number of user_ids to the ratio you specify. i.e. 0.25 would give you a quarter of all the user_ids.     
#' @param chan_merge The channel merge method. "S" for a standard join to the Hierarchy File on page_id only.  "K" is for Keyword matching, and "KP" for keyword and page_id matching. If you select these you need to include ppc_hierarchy_location, and ppc_match_col 
#' @param join_type The join type between the clicks nad impressions to the hierarchy. "left" for a left join. Any pageIDs not in the hierarchy file will be filled in with "Other". "inner" for an inner join. Only channels in the hierarchy file will be included in the analysis. 
#' @param agency What agency are you? eg "m2m", "omd", "mg", "phd". If you are using networkId 33505 (Liberty Global) then the Agency should be the country eg "uk", "be". You will also need to ensure you are accessing the correct database eg dbname="lgiemea"
#' @param networkId The network ID for the Double Click data. If you are using networkId 33505 (Liberty Global) then the Agency should be the country eg "uk", "be". You will also need to ensure you are accessing the correct database eg dbname="lgiemea"
#' @param advertiserIds A character vector of advertiser ids to select
#' @param campaignIds A character vector of campaign ids to select
#' @param activity_sub_type what activity sub type are we interested in?
#' @param startDate the start date of the DP in "YYYY-MM-DD" format
#' @param endDate the end date of the DP in "YYYY-MM-DD" format
#' @param startPre the start date of the warmup period for the DP in "YYYY-MM-DD" format
#' @param endPre the end date of the warmup period for the DP in "YYYY-MM-DD" format 
#' @param NVprior when running for New Vistors the length of the prior period in which "new" visitors must not have been seen in
#' @param sample The sample sizes to feed into the model. Specify a multiple of converters you want from the non_converters i.e 20. Will give you 20 non-converters for every converter. Or specify a maximum limit for converters and non-converters i.e. c(50000, 500000) This would ensure you get a maximum of 50K converters and 500K non-converters. However if there are fewer than that available, it will just take as many as are available.
#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param hierarchy_location Where the hierarchy is saved. The hierachy file must contain 4 columns in this order page_id, buy_id, channel, sub_channel. It should also contain headers in the first row only. sub_channels must be unique. i.e. you can't have the same sub_channel under two different channels. channel and sub_channel names must not begin with numeric characters, and must only contain alphanumerics or underscores '_'. For example "Display Channel" would be "Display_Channel"
#' @param ppc_hierarchy_location The location of your keyword matching file. For keyword matching it should be a csv containing following three columns in this order: ppc_match_col, channel, sub_channel. For keyword and page_id matching it should be a csv containing following four columns in this order: page_id, ppc_match_col, channel, sub_channel. In both cases it should also contain headers in the first row only. sub_channels must be unique. i.e. you can't have the same sub_channel under two different channels. channel and sub_channel names must not begin with numeric characters, and must only contain alphanumerics or underscores '_'. For example "Display Channel" would be "Display_Channel"
#' @param ppc_match_col The name of the column in the dartsearch table you wish you perform keyword matching with. Either ds_campaign_name or kw_ad_group_name
#' @param group If you are part of the central team and don't have access to agency s3 buckets set this to true to run in the group area. By default FALSE 
#' 
#' @author James Thomson, \email{james.thomson@@omnicommediagroup.com}
#' @references \url{https://bitbucket.org/omguk-annalect/dpr/wiki/Home}
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location ready for use with the blue_function, rather than sent to the console.

#' @examples
#' 
#' #gold_red_function(username="xxxxx", pw="xxxxx", dbname="annalectuk", dbHost="54.77.118.78",
#' #                  KeyID="xxxxx", SecretAccessKey="xxxxx", clust_size=20,
#' #                  DPID="test_run", agency="omd", networkId="530",
#' #                  conv_type = "C", data_source = "DC", chan_merge = "L", 
#' #                  advertiserIds=c(856279,1040292), 
#' #                  campaignIds=c(8608700,8685166,8275648,8704058,8581290,6158304,7945110,8722740,7888562,8373606),
#' #                  activity_sub_type=c("2014_000","2014_001"), 
#' #                  startDate="2015-06-01", endDate="2015-06-30", startPre="2015-05-20", endPre="2015-05-31", 
#' #                  sample = 20,
#' #                  file_save_location="P:/DPR/test_run/output", hierarchy_location="P:/DPR/test_run/Hierarchy/Hierarchy_file.csv")
#' #                  
#' #

gold_red_function <- function(
                         username, pw, dbname="annalectuk", dbHost="54.77.118.78", 
                         KeyID, SecretAccessKey, clust_size,
                         DPID, conv_type="C", data_source="DC", chan_merge="S", join_type="Left",
                         agency, networkId, advertiserIds, campaignIds, 
                         activity_sub_type, activity_type=NULL, eventid.ign=NULL, 
                         startDate, endDate, startPre, endPre, NVprior=90,
                         sample=20, 
                         file_save_location, hierarchy_location, 
                         ppc_hierarchy_location=NULL, ppc_match_col=NULL,
                         group=F) {
  
start.time <- Sys.time()

writeLines("WARNING THIS FUNCTION HAS BEEN DEPRECATED. PLEASE USE THE GOLD_FUNCTION INSTEAD")



  # idiot checks

  if(!paste0(agency, "-", networkId)%in%c("m2m-1707",  "m2m-5851",  "m2m-8001",  "m2m-8552",  
                                          "omd-530",  "omd-3434","omd-6631",  "omd-9041",
                                          "mg-1707",  "mg-33505",  "mg-5648",  "mg-9433",
                                          "phd-1137",  "phd-8971",  
                                          "roc-6050", 
                                          "grp-6631", 
                                          "at-33505", "be-33505", "ch-33505", "cz-33505", "de-33505", "hu-33505", "ie-33505", "nl-33505", "upl-33505", "ro-33505", "sk-33505", "uk-33505")
     ){stop("Not a correct agency and networkid")}

  if(is.numeric(conv_type)){
    if(conv_type<0 || conv_type>1){
      stop("The conversion type should be between 0 and 1 for a sampling run")
    }
  } else {
      if(!conv_type%in%c("C", "NV", "NVC")){
        stop("Not a correct conversion type")
      }
  }

  
  

  if(!chan_merge%in%c("S", "K", "KP")){stop("Not a correct merge type")}
  if(!tolower(join_type)%in%c("left", "inner")){stop("Not a join type")}
  if(chan_merge=="K" & is.null(ppc_hierarchy_location)){stop("If you are doing a Keyword join you need to specify a ppc_hierarchy_location")}
  if(chan_merge=="K" & is.null(ppc_match_col)){stop("If you are doing a Keyword join you need to specify a ppc_match_col")}


  sd <- try( as.Date( startDate, format= "%Y-%m-%d" ) )
  ed <- try( as.Date( endDate, format= "%Y-%m-%d" ) )
  sp <- try( as.Date( startPre, format= "%Y-%m-%d" ) )
  ep <- try( as.Date( endPre, format= "%Y-%m-%d" ) )
  
  if(class(sd)=="try-error" || is.na(sd)) {
    stop("WHAT WENT WRONG: startDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(sp)=="try-error" || is.na(sp)) {
    stop("WHAT WENT WRONG: startPre must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  
  if(class(ed)=="Date"&class(sd)=="Date" & ed<sd) {
    stop("WHAT WENT WRONG: startDate must be before endDate")
    failure <- 1}
  if(class(ep)=="Date"&class(sp)=="Date" & ep<sp) {
    stop("WHAT WENT WRONG: startPre must be before endPre")
    failure <- 1}
  if(class(ep)=="Date"&class(sd)=="Date" & sd<ep) {
    stop("WHAT WENT WRONG: startDate must be after endPre")
    failure <- 1
  }


  
  # parameter control -------------------------------------------------------
  

  startPrior  <- as.Date(startPre)-(NVprior+1)
  endPrior    <- as.Date(startPre) 

  #identify unique months between start and end dates for pulling in the relevant double click data and partitioning queries
  priorMonths<-unique(format(seq(as.Date(startPrior), as.Date(endPrior), by="days"), "%Y%m"))
  preMonths<-unique(format(seq(as.Date(startPre), as.Date(endPre), by="days"), "%Y%m"))
  Months<-unique(format(seq(as.Date(startDate), as.Date(endDate), by="days"), "%Y%m"))

  # needs to be one date either side
  startDate <- as.Date(startDate)-1
  endDate   <- as.Date(endDate)+1
  startPre  <- as.Date(startPre)-1
  endPre    <- as.Date(endPre)+1

  analyst   <- username

  if (conv_type=="C"||is.numeric(conv_type)){reqmonths<-unique(c(preMonths, Months))}
  if (conv_type %in% c("NV","NVC")){reqmonths<-unique(c(priorMonths, preMonths, Months))}
  
  currentdate<-format(Sys.Date(),"%Y%m%d")

  
# HIVE PART BEGIN ----------------------------------------------------------

#include the raws library. ideally it would be in the dependencies list on the description file, however it can't be as its not on CRAN 
library(RAWS)

#this will install the AWSCLI if not already installed
writeLines("Checking for AWSCLI")
installAWSCLI()


#define s3 bucket
if(dbname=="lgiemea"){bucket=paste0("lgi-staging/staging-",agency )}

if(dbname=="annalectuk"){
  if (group==FALSE){bucket=paste0(agency, "-uk-staging")}
  if (group==TRUE){bucket="uk-staging"}
}


#create bespoke activity type where clause depending on whether a activity type is specified and a eventid.ign is specified
activity_type_set<-paste0("and activity_sub_type in ('", paste0(activity_sub_type, collapse = "','") , "')")

if(!is.null(activity_type)){
  activity_type_set<-paste0(activity_type_set, 
                            " and activity_type in ('", paste0(activity_type, collapse = "','") , "')"
  )
}

if(!is.null(eventid.ign)){
  activity_type_set<-paste0(activity_type_set, " and event_id<>",eventid.ign)
}

activity_type_set<-paste0(activity_type_set, ";\n")



#generate series of global variables for Hive code based on inputs
set<-paste0(
  "\n\nset analyst=", username, ";\n",
  "set agency=", agency, ";\n",
  "set networkId=", networkId, ";\n",
  "set advertiserIds=(", paste0(advertiserIds, collapse = ","), ");\n",
  "set campaignIds=(", paste0(campaignIds, collapse = ",") , ");\n",
  "set activity_sub_type=", activity_type_set,
  "set startDate='", startDate, "';\n",
  "set endDate='", endDate, "';\n",
  "set months=", paste0(Months, collapse=','),";\n",
  "set startPre='", startPre, "';\n",
  "set endPre='", endPre, "';\n",
  "set premonths=", paste0(preMonths, collapse=','),";\n",
  "set startPrior='", startPrior, "';\n",
  "set endPrior='", endPrior, "';\n",  
  "set priormonths=", paste0(priorMonths, collapse=','),";\n",
  "set currentdate=", currentdate, ";\n",
  "set DPID=", DPID, ";\n", 
  "set s3location=s3://", bucket, "/dp/${hiveconf:analyst}/${hiveconf:agency}_${hiveconf:networkId}/${hiveconf:DPID}/${hiveconf:currentdate};\n",
  "set jointype=", join_type, ";\n"
    )


if (length(sample)==1){
  set<-c(set, paste0("set sampleratio=",sample, ";\n"))
}

if (length(sample)==2){
  set<-c(set, paste0("set conv_limit=", sample[1], ";\n set non_conv_limit=", sample[2], ";\n"))
}

if (chan_merge%in%c("K", "KP")){
  set<-c(set, paste0("set ppc_match_col=", ppc_match_col,";\n"))
}

if (is.numeric(conv_type)){
  set<-c(set, paste0("set sample=", conv_type,";\n"))
} 
  


#Plug and Play Hive script sections

#data source: use scriptHiveDC to generate hive code to pull in the relevent data for those months, agency and networkID
if (data_source=="DC"){
  importdata<-scriptHiveDC(Agency=paste0(agency, "-", networkId), Month=reqmonths, Type=c("c", "i", "na", "match_dartsearch"), group=group)
}
if (data_source=="SZ"){
  importdata<-scriptHiveSZ(Agency=agency, Month=reqmonths, Type="ALL", group=group)
  data(hiveSizmek)
  importdata<-c(importdata, hiveSizmek)
}
if (data_source=="PSA_DEIT"){
  importdata<-scriptHivePSADEIT(advertiserIds, Month=reqmonths)
  data(hivePSADEIT)
  importdata<-c(importdata, hivePSADEIT)
}


#Hive Code - just hard coded in a data object as part of the package. See the scripts in Dev to see what they contain

#Conversion type : Three versions standard conversion, new visitors, new visitor conversions
if (conv_type=="C"){
        data(hiveConvCode)
        typecode<-hiveConvCode
}
if (conv_type=="NV"){
       data(hiveNVCode)
       typecode<-hiveNVCode  
} 
if (conv_type=="NVC"){
  data(hiveNVCCode)
  typecode<-hiveNVCCode  
} 

if (is.numeric(conv_type)){
  data(hiveConvSampleCode)
  typecode<-hiveConvSampleCode  
} 



#ICM channel merge methods

# if (chan_merge=="L"){
#   data(hiveMergeLeft1)
#   mergecode1<-hiveMergeLeft1
#   data(hiveMergeLeft2)
#   mergecode2<-hiveMergeLeft2
# }
# if (chan_merge=="I"){
#   data(hiveMergeInner1)
#   mergecode1<-hiveMergeInner1
#   data(hiveMergeInner2)
#   mergecode2<-hiveMergeInner2
# }


if (chan_merge=="S"){
  data(hiveMergeStd1)
  mergecode1<-hiveMergeStd1
  data(hiveMergeStd2)
  mergecode2<-hiveMergeStd2
}

if (chan_merge=="K"){
  data(hiveMergeKeyword1)
  mergecode1<-hiveMergeKeyword1
  data(hiveMergeKeyword2)
  mergecode2<-hiveMergeKeyword2
}

if (chan_merge=="KP"){
  data(hiveMergeKeywordAndPage1)
  mergecode1<-hiveMergeKeywordAndPage1
  data(hiveMergeKeywordAndPage2)
  mergecode2<-hiveMergeKeywordAndPage2
}





#pull in bulk processing code
data(hiveMainCode)


#sampling method code
if (length(sample)==1){
  data(hiveSampleRatio)
  samplecode<-hiveSampleRatio
}
if (length(sample)==2){
  data(hiveSampleLimit)
  samplecode<-hiveSampleLimit
}



#auto generate pivoting code from the hierarchy table

hierachy<-read.csv(file=hierarchy_location)
colnames(hierachy) <- tolower(colnames(hierachy))

#hierarchy checks
if(!all(colnames(hierachy)==c("page_id","buy_id","channel","sub_channel"))){stop("Your Hierarchy file is in the wrong format. It must contain the columns page_id,buy_id,channel,sub_channel in that order")}
if(any(duplicated(hierachy$page_id))) {warning("WARNING!!! There are duplicate page_ids in your hierarchy. This may cause duplication issues when merging onto your events.")}

if(any(grepl("[:;&+()' !/&-]", hierachy[,1]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in page_id. Please remove them before continuing")}
if(any(grepl("[:;&+()' !/&-]", hierachy[,2]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in buy_id. Please remove them before continuing")}
if(any(grepl("[:;&+()' !/&-]", hierachy[,3]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in channel. Please remove them before continuing")}
if(any(grepl("[:;&+()' !/&-]", hierachy[,4]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in sub_channel. Please remove them before continuing")}

chanLkup<-aggregate(hierachy$page_id, by=list(hierachy$channel,hierachy$sub_channel), FUN=mean)
chanLkup<-chanLkup[,1:2]
colnames(chanLkup)<-c("channel", "sub_channel")

if(any(duplicated(chanLkup$sub_channel))){stop("Your Hierarchy File contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again")}


transpose<-"drop table if exists icm_transpose;
CREATE EXTERNAL TABLE IF NOT EXISTS icm_transpose
(user_id STRING,"

for (i in 1:nrow(chanLkup)){
  temp<-paste0(chanLkup[i,2], "_1 BIGINT,")
  temp2<-paste0(chanLkup[i,2], "_2 BIGINT,")  
  transpose<-c(transpose, temp, temp2)
}


if ("other"%in%tolower(chanLkup$sub_channel)){
transpose<-c(transpose,"Blank BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/icm_transpose/'
;")
}

if (!"other"%in%tolower(chanLkup$sub_channel)){
  transpose<-c(transpose,"Other_1 BIGINT, Other_2 BIGINT) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
               STORED AS TEXTFILE
               LOCATION '${hiveconf:s3location}/icm_transpose/'
               ;")
}




transpose<-c(transpose,"INSERT OVERWRITE TABLE icm_transpose select user_id, ")

for (i in 1:nrow(chanLkup)){
  temp<-paste0("sum(case when Sub_Channel='", chanLkup[i,2], "' and eventind=1 then 1 else 0 end) as ", chanLkup[i,2], "_1,")
  temp2<-paste0("sum(case when Sub_Channel='", chanLkup[i,2], "' and eventind=2 then 1 else 0 end) as ", chanLkup[i,2], "_2,")  
  transpose<-c(transpose, temp, temp2)
}

if ("other"%in%tolower(chanLkup$sub_channel)){
transpose<-c(transpose, "sum(case when Sub_Channel is NULL then 1 else 0 end) as Blank
                        from dc_icm_sample group by user_id;")}

if (!"other"%in%tolower(chanLkup$sub_channel)){
  transpose<-c(transpose, "sum(case when Sub_Channel='Other' and eventind=1 then 1 else 0 end) as Other_1,
                        sum(case when Sub_Channel='Other' and eventind=2 then 1 else 0 end) as Other_2
                        from dc_icm_sample group by user_id;")}  
  
  
  
  


#testing purposes
#scriptSaveLocal(scripts=c(importdata,set,typecode,hiveMainCode,transpose), loc="P:/")

#generate s3 location for script and logs to be added to 
s3loc=paste0("s3://", bucket, "/dp/", username, "/", agency, "_", networkId, "/", DPID, "/", currentdate, "/" )
scriptloc=paste0(s3loc, "script/" )
logsloc=paste0(s3loc, "logs" )


#write script and upload to s3
writeLines("Generating Hive Script and Uploading it to s3")
scriptSaveS3(KeyID, SecretAccessKey, scripts=c(importdata, set, typecode, mergecode1, hiveMainCode, samplecode, transpose, mergecode2), loc=scriptloc)

#upload hierarchy to s3
writeLines("Uploading Hierarchy File to s3")
s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "hierarchy_file/"), hierarchy_location)

if (chan_merge=="K"){

  #checks
  ppc_hierachy<-read.csv(file=ppc_hierarchy_location)
  colnames(ppc_hierachy) <- tolower(colnames(ppc_hierachy))
  
  if(!all(colnames(ppc_hierachy)==c("ppc_match_col","channel","sub_channel"))){stop("Your PPC Hierarchy file is in the wrong format. It must contain the columns ppc_match_col,channel,sub_channel in that order")}
  if(any(duplicated(ppc_hierachy$ppc_match_col))) {warning("WARNING!!! There are duplicate ppc_match_col in your PPC hierarchy. This may cause duplication issues when merging onto your events.")}
  
  if(any(grepl("[:;&+()' !/&-]", ppc_hierachy[,2]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in buy_id. Please remove them before continuing")}
  if(any(grepl("[:;&+()' !/&-]", ppc_hierachy[,3]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in channel. Please remove them before continuing")}

  ppcLkup<-aggregate(ppc_hierachy$ppc_match_col, by=list(ppc_hierachy$channel,ppc_hierachy$sub_channel), FUN=nrow)
  ppcLkup<-ppcLkup[,1:2]
  colnames(ppcLkup)<-c("channel", "sub_channel")
  
  if(any(duplicated(ppcLkup$sub_channel))){stop("Your PPC Hierarchy File contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again")}

writeLines("Uploading PPC Hierarchy File to s3")
s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "ppc_hierarchy_file/"), ppc_hierarchy_location)
}


if (chan_merge=="KP"){
  
  #checks
  ppc_hierachy<-read.csv(file=ppc_hierarchy_location)
  colnames(ppc_hierachy) <- tolower(colnames(ppc_hierachy))
  
  if(!all(colnames(ppc_hierachy)==c("page_id", "ppc_match_col","channel","sub_channel"))){stop("Your PPC Hierarchy file is in the wrong format. It must contain the columns page_id,ppc_match_col,channel,sub_channel in that order")}
  if(any(duplicated(ppc_hierachy$ppc_match_col))) {warning("WARNING!!! There are duplicate ppc_match_col in your PPC hierarchy. This may cause duplication issues when merging onto your events.")}
  
  if(any(grepl("[:;&+()' !/&-]", ppc_hierachy[,3]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in channel. Please remove them before continuing")}
  if(any(grepl("[:;&+()' !/&-]", ppc_hierachy[,4]))==TRUE){stop("There are illegal [' !/-:;&+()] characters in sub_channel. Please remove them before continuing")}
  
  ppcLkup<-aggregate(ppc_hierachy$ppc_match_col, by=list(ppc_hierachy$channel,ppc_hierachy$sub_channel), FUN=nrow)
  ppcLkup<-ppcLkup[,1:2]
  colnames(ppcLkup)<-c("channel", "sub_channel")
  
  if(any(duplicated(ppcLkup$sub_channel))){stop("Your PPC Hierarchy File contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again")}
  
  writeLines("Uploading PPC Hierarchy File to s3")
  s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "ppc_hierarchy_file/"), ppc_hierarchy_location)
}




EMRStartTime<-Sys.time()

writeLines(paste0("Started HiveEMR job at ", Sys.time(),  " This make take a while"))

if (group==FALSE) {
clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
        s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency=paste0("Annalect UK - ", toupper(agency)))
}

if (group==TRUE) {
clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
        s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency="Annalect UK")
}


#need to monitor job in order to know when its finished
x<-1
while(x==1){
  state<-EMRMonitor(KeyID, SecretAccessKey, clusterID, out=T)
  if(any(state%like%"TERMINATED")){x=2}
}

EMREndTime<-Sys.time()

writeLines(paste0("HiveEMR job complete at ", Sys.time()))

#check output is there
EMROut<-s3List(KeyID, SecretAccessKey, s3loc)
if(!any(EMROut$Folders%like%'dc_icm')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'dc_na')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'icm_transpose')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'dc_icm_sample')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'hierarchy_file')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'dc_icm_merged2')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'dc_icm_conv')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'dc_na_filt_LC')){stop("EMRHive job has not run properly. Function stopped")}
if(!any(EMROut$Folders%like%'LC_merged')){stop("EMRHive job has not run properly. Function stopped")}

EMROut<-s3List(KeyID, SecretAccessKey, paste0(s3loc, "dc_icm_sample/"))
if(nrow(EMROut$Files)==0){stop("EMRHive job has not run properly. Function stopped")}

EMROut<-s3List(KeyID, SecretAccessKey, paste0(s3loc, "icm_transpose/"))
if(nrow(EMROut$Files)==0){stop("EMRHive job has not run properly. Function stopped")}

# REDSHIFT TRANSFER ----------------------------------------------------------

writeLines(paste0("Clearing any old tables with the same DPID from redshift at ",Sys.time()))

# DB control --------------------------------------------------------------

#a redshift version of the DPID for easy identfication of tables
DPID_RS <- tolower(gsub("_","",DPID))

#date code to identify when the tables were created on redshift and enable easy cleanup
date_code <- paste0(substr(year(Sys.Date()),3,4), str_pad(month(Sys.Date()),width = 2,pad="0"), str_pad(mday(Sys.Date()),width = 2,pad="0"))

#redshift connection
port <-"5439"
drv <- dbDriver("PostgreSQL")
redshift_con <- dbConnect(drv, dbname = dbname, user = username, password = pw, 
                          host = dbHost, port = port)


if(dbname=="annalectuk"){
  #id for each user
  analyst_lookup_Q <- dbSendQuery(redshift_con,paste0("select distinct(rs_user_id) from ds_",agency,".rs_users where user_name = '", username, "'"))
  analyst_lookup <- fetch(analyst_lookup_Q,n=-1)
  
  #if this DPID has been run on a previous date remove those tables before running
  existing_tables_Q <- dbSendQuery(redshift_con,paste0("select table_name from information_schema.tables where table_schema = 'ds_",agency,"'AND table_name LIKE 'dp%';"))
  existing_tables <- fetch(existing_tables_Q,n=-1)
  
  tables_to_remove <- existing_tables[grep(paste0("_",DPID_RS,"_"), existing_tables$table_name),1]
  
  if(length(tables_to_remove)>0) {
    dbSendQuery(redshift_con,paste0("DROP TABLE IF EXISTS ds_", agency,".",tables_to_remove,";", collapse=" \n"))
  }
  
  #construct redshift table naming convention
  rs_tb_name_conv<-paste0(dbname, ".ds_", agency, ".dp",date_code,"_", analyst_lookup, "_", DPID_RS)
}



if(dbname=="lgiemea"){

  #id for each user
  analyst_lookup_Q <- dbSendQuery(redshift_con,paste0("select distinct(rs_user_id) from dc_",agency,".rs_users where user_name = '", username, "'"))
  analyst_lookup <- fetch(analyst_lookup_Q,n=-1)
  
  #if this DPID has been run on a previous date remove those tables before running
  existing_tables_Q <- dbSendQuery(redshift_con,paste0("select table_name from information_schema.tables where table_schema = 'dc_",agency,"'AND table_name LIKE 'dp%';"))
  existing_tables <- fetch(existing_tables_Q,n=-1)
  tables_to_remove <- existing_tables[grep(paste0("_",DPID_RS,"_"), existing_tables$table_name),1]
  if(length(tables_to_remove)>0) {
    dbSendQuery(redshift_con,paste0("DROP TABLE IF EXISTS dc_", agency,".",tables_to_remove,";", collapse=" \n"))
  }
  
  #construct redshift table naming convention
  rs_tb_name_conv<-paste0(dbname, ".dc_", agency, ".dp",date_code,"_", analyst_lookup, "_", DPID_RS)
}




writeLines(paste0("Transfering data from s3 to redshift at ",Sys.time()))


#conversions
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_na;",
                                "create table ",rs_tb_name_conv,"_na(
                                user_id VARCHAR,
                                time TIMESTAMP,
                                activity_sub_type VARCHAR,
                                os_id BIGINT,  
                                browser_id BIGINT, 
                                keyword_id BIGINT,
                                page_id BIGINT,
                                buy_id BIGINT,
                                site_id BIGINT,
                                advertiser_id BIGINT,
                                record_month BIGINT,
                                record_week BIGINT,
                                record_year BIGINT
);",
                                "copy  ",rs_tb_name_conv,"_na from '", paste0(s3loc, "dc_na/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))						


#icm_merged2
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_icm_merged2;",
                                "create table ",rs_tb_name_conv,"_icm_merged2(
  user_id	VARCHAR,
  time	TIMESTAMP,
                                city_id	BIGINT,
                                os_id	BIGINT,  
                                page_id	BIGINT,  
                                buy_id	BIGINT,
                                browser_id	BIGINT,
                                site_id	BIGINT,  
                                advertiser_id	BIGINT,
                                record_month	BIGINT,
                                record_week	BIGINT,  
                                record_year	BIGINT,
                                keyword_id VARCHAR,
                                eventind INT,
                                channel VARCHAR,
                                sub_channel VARCHAR,
                                row_rank INT
                                	);",
                                "copy  ",rs_tb_name_conv,"_icm_merged2 from '", paste0(s3loc, "dc_icm_merged2/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))


#na_filt_LC
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_na_filt_LC;",
                                "create table ",rs_tb_name_conv,"_na_filt_LC(
  user_id VARCHAR,
  time TIMESTAMP,
                                activity_sub_type VARCHAR,
                                os_id BIGINT,  
                                browser_id BIGINT, 
                                keyword_id VARCHAR,
                                page_id BIGINT,
                                buy_id BIGINT,
                                site_id BIGINT,
                                advertiser_id BIGINT,
                                record_month BIGINT,
                                record_week BIGINT,
                                record_year BIGINT,
                                row_rank INT
                                	);",
                                "copy  ",rs_tb_name_conv,"_na_filt_LC from '", paste0(s3loc, "dc_na_filt_LC/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))



#LC_merged
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_LC_merged;",
                                "create table ",rs_tb_name_conv,"_LC_merged(
  user_id VARCHAR,
  time TIMESTAMP,
                                channel VARCHAR,
                                sub_channel VARCHAR,
                                record_month BIGINT,
                                record_week BIGINT,
                                record_year BIGINT
);",
                                "copy  ",rs_tb_name_conv,"_LC_merged from '", paste0(s3loc, "LC_merged/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))


#icm_conv
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_icm_conv;",
                                "create table ",rs_tb_name_conv,"_icm_conv(
  user_id	VARCHAR,
  time	TIMESTAMP,
                                city_id	BIGINT,
                                os_id	BIGINT,  
                                page_id	BIGINT,  
                                buy_id	BIGINT,
                                browser_id	BIGINT,
                                site_id	BIGINT,  
                                advertiser_id	BIGINT,
                                record_month	BIGINT,
                                record_week	BIGINT,  
                                record_year	BIGINT,
                                keyword_id VARCHAR,
                                eventind INT,
                                channel VARCHAR,
                                sub_channel VARCHAR,
                                row_rank INT
                                	);",
                                "copy  ",rs_tb_name_conv,"_icm_conv from '", paste0(s3loc, "dc_icm_conv/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))


#icm_sample
dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_icm_20;",
                                "create table ",rs_tb_name_conv,"_icm_20(
  user_id	VARCHAR,
  time	TIMESTAMP,
                                city_id	BIGINT,
                                os_id	BIGINT,  
                                page_id	BIGINT,  
                                buy_id	BIGINT,
                                browser_id	BIGINT,
                                site_id	BIGINT,  
                                advertiser_id	BIGINT,
                                record_month	BIGINT,
                                record_week	BIGINT,  
                                record_year	BIGINT,
                                keyword_id VARCHAR,
                                eventind INT,
                                channel VARCHAR,
                                sub_channel VARCHAR,
                                row_rank INT
);",
                                "copy  ",rs_tb_name_conv,"_icm_20 from '", paste0(s3loc, "dc_icm_sample/" ),"'",
                                "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))


#icm_transpose

cols<-("user_id VARCHAR,")
for (i in 1:nrow(chanLkup)){
  temp<-paste0(chanLkup[i,2], "_1 BIGINT,")
  temp2<-paste0(chanLkup[i,2], "_2 BIGINT,")  
  cols<-c(cols, temp, temp2)
}
if ("other"%in%tolower(chanLkup$sub_channel)){cols<-c(cols, "Blank BIGINT")}
if (!"other"%in%tolower(chanLkup$sub_channel)){cols<-c(cols, "Other_1 BIGINT, Other_2 BIGINT")}

cols_text<-paste0(cols, collapse="")


dbSendQuery(redshift_con,paste0("drop table if exists ",rs_tb_name_conv,"_icm_transpose;",
                                "create table ", rs_tb_name_conv,"_icm_transpose(", cols_text, ");",
                                "copy  ",rs_tb_name_conv,"_icm_transpose from '", paste0(s3loc, "icm_transpose/" ),"'",
                                 "credentials 'aws_access_key_id=", KeyID,";aws_secret_access_key=",SecretAccessKey,
                                "' delimiter '\t' dateformat 'auto' timeformat 'auto' ACCEPTINVCHARS TRUNCATECOLUMNS REMOVEQUOTES FILLRECORD 
                                MAXERROR AS 1; 

                                "))


####REDSHIFT PROCESSING

writeLines("Processing data on redshift")


  #___Filtered_conversions by os - used for one of the outputs__________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv, "_na_filt_os;
                                  CREATE TABLE ", rs_tb_name_conv,"_na_filt_os as
                                  SELECT s.user_id, s.time, u.os_id
                                  FROM ",rs_tb_name_conv,"_na_filt_LC AS s
                                  left JOIN ", rs_tb_name_conv,"_na AS u
                                  ON s.user_id = u.user_id and s.time = u.time")) 
  

  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM ", rs_tb_name_conv,"_na_filt_os
                                  WHERE user_id not in (select user_id from ", rs_tb_name_conv,"_icm_conv);"))
  
  
  #____Get_na's________________________________________
  na_os_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ", rs_tb_name_conv,"_na_filt_os
                                                  "))
  na_os <- fetch(na_os_Q, n = -1)
  # this is used for outputs later on
  
  ##########
  message(paste0("Audience, LC,  ",Sys.time()))
  #___Audience__________________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_audience;
                                  create table ", rs_tb_name_conv,"_audience as(
                                  select distinct user_id from ", rs_tb_name_conv,"_icm_merged2);"))
  
  aud_length_Q <- dbSendQuery(redshift_con,paste0("
                                                  select count(*) from ", rs_tb_name_conv,"_audience;"))
  aud_length <- as.numeric(fetch(aud_length_Q, n = -1))
  
  ### new last click - taken from na file
  
  #______MATCH LC ONTO HIERARCHY

  lc_na_Q <- dbSendQuery(redshift_con,paste0("
                                             select * from ", rs_tb_name_conv,"_LC_merged"))
  lc_na <- fetch(lc_na_Q,n=-1)
  
  lc_na$lc <- 1
  lc_na$record_month <- paste0(lc_na$record_year,str_pad(lc_na$record_month,width=2,pad="0"))
  lc_na$record_week  <- paste0(lc_na$record_year,str_pad(lc_na$record_week,width=2,pad="0"))
  
  lc <- list(
    weekly_sub   = aggregate(lc~channel+sub_channel+record_week,data=lc_na,FUN=sum),
    weekly_chan  = aggregate(lc~channel+record_week,data=lc_na,FUN=sum),
    monthly_sub  = aggregate(lc~channel+sub_channel+record_month,lc_na,FUN=sum),
    monthly_chan = aggregate(lc~channel+record_month,lc_na,FUN=sum),
    total_sub    = aggregate(lc~channel+sub_channel,lc_na,FUN=sum),
    total_chan   = aggregate(lc~channel,lc_na,FUN=sum)
  )
  
  
  lc$total_sub$level    <- "1_s"
  lc$total_chan$level   <- "1_c"
  lc$monthly_sub$level  <- "2_s"
  lc$monthly_chan$level <- "2_c"
  lc$weekly_sub$level   <- "3_s"
  lc$weekly_chan$level  <- "3_c"
  
  lc        <- do.call(rbind.fill,lc)
  
  lc <- lc[,c("channel","sub_channel",
              "record_month","record_week","lc","level")]
  
  
  message(paste0("OS/city,  ",Sys.time()))
  #__os_distribution
  ## perhaps neater to have city_id as city_id - avoid renaming later.
  # same for os
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_os_distr1;
                                create table ", rs_tb_name_conv,"_os_distr1 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as users
                                from ", rs_tb_name_conv,"_icm_merged2
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                drop table if exists ", rs_tb_name_conv,"_os_distr2;
                                create table ", rs_tb_name_conv,"_os_distr2 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from ", rs_tb_name_conv,"_icm_conv
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                drop table if exists ", rs_tb_name_conv,"_os_distr;
                                create table ", rs_tb_name_conv,"_os_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from ", rs_tb_name_conv,"_os_distr1 as a
                                left join ", rs_tb_name_conv,"_os_distr2 as b
                                on a.os=b.os
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))

  
  #____ City_distribution
dbSendQuery(redshift_con,paste0("
                                drop table if exists ", rs_tb_name_conv,"_city_distr1;
                                create table ", rs_tb_name_conv,"_city_distr1 as(
                                select city_id as city,
                                channel, sub_channel,  count(distinct user_id) as users
                                from ", rs_tb_name_conv,"_icm_merged2
                                group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                drop table if exists ", rs_tb_name_conv,"_city_distr2;
                                create table ", rs_tb_name_conv,"_city_distr2 as(
                                select city_id as city,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from ", rs_tb_name_conv,"_icm_conv
                                group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                drop table if exists ", rs_tb_name_conv,"_city_distr;
                                create table ", rs_tb_name_conv,"_city_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from ", rs_tb_name_conv,"_city_distr1 as a
                                left join ", rs_tb_name_conv,"_city_distr2 as b
                                on a.city=b.city
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))

  city_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ", rs_tb_name_conv,"_city_distr"))
  city_distr <- fetch(city_distr_Q, n = -1)
  
  os_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                select * from ", rs_tb_name_conv,"_os_distr"))
  os_distr <- fetch(os_distr_Q, n = -1)
  
  #___Get match tables___
 
  
  
  
  
  
  
if(dbname=="annalectuk"){ 
    os_match_Q <- dbSendQuery(redshift_con,paste0("select * from ds_",agency,".dc_match_operating_systems"))
    os_match <- fetch(os_match_Q, n = -1)
    city_match_Q <- dbSendQuery(redshift_con,paste0("select * from ds_",agency,".dc_match_cities"))
    city_match <- fetch(city_match_Q, n = -1)
}    
    
 
if(dbname=="lgiemea"){  
    os_match_Q <- dbSendQuery(redshift_con,paste0("select * from dc_",agency,".dc_match_operating_systems"))
    os_match <- fetch(os_match_Q, n = -1)
    city_match_Q <- dbSendQuery(redshift_con,paste0("select * from dc_",agency,".dc_match_cities"))
    city_match <- fetch(city_match_Q, n = -1)
}       
    

    city_distr$city_id <- city_distr$city
    city_distr <- city_distr[,-which(names(city_distr)=="city")]
    
    os_distr$os_id <- os_distr$os
    os_distr <- os_distr[,-which(names(os_distr)=="os")]
    
    city_distr <- merge(city_distr,city_match,all.x=T)
    os_distr <- merge(os_distr,os_match,all.x=T)
  


      
  
####need to think about how this whole section will work with nv and nvc
  message(paste0("Event distr,  ",Sys.time()))
  
  #__Imp_distribution____________
  imp_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, count(*) as imps
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 where eventind = 2
                                                 group by sub_channel;"))
  imp_distr <- fetch(imp_distr_Q, n = -1)
  imp_distr <- imp_distr[order(imp_distr$imps, decreasing = T),]
  #__Imp_time_distribution______________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_imp_time_distr;
                                  create table ", rs_tb_name_conv,"_imp_time_distr as(
                                  select m.time::date as imp_dom, m.channel as imp_chan,
                                  m.sub_channel as imp_sub,m.os_id as imp_os, EXTRACT(HOUR FROM m.time) as imp_hod, 
                                  EXTRACT(DOW FROM m.time) as imp_dow, count(*) as impressions,
                                  CASE WHEN user_id in (select user_id from ", rs_tb_name_conv,"_icm_conv) THEN 'C' ELSE 'NC' END as imp_conv_path
                                  from ", rs_tb_name_conv,"_icm_merged2 as m
                                  where eventind = 2
                                  group by m.channel, m.sub_channel,m.os_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from ", rs_tb_name_conv,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))  
  
    
if (conv_type=="C"||is.numeric(conv_type)) {  
  #__Click_distribution____________
  click_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                   select sub_channel, count(*) as clicks
                                                   from ", rs_tb_name_conv,"_icm_merged2
                                                   where eventind = 1
                                                   group by sub_channel;
                                                   "))
  click_distr <- fetch(click_distr_Q, n = -1)
  click_distr <- click_distr[order(click_distr$clicks, decreasing = T), ]
  #__Click_time_distribution______________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_click_time_distr;
                                  create table ", rs_tb_name_conv,"_click_time_distr as(
                                  select m.time::date as click_dom, m.channel as click_chan, 
                                  m.sub_channel as click_sub,m.os_id as click_os, EXTRACT(HOUR FROM m.time) as click_hod,
                                  EXTRACT(DOW FROM m.time) as click_dow, count(*) as clicks,
                                  CASE WHEN user_id in (select user_id from ", rs_tb_name_conv,"_icm_conv) THEN 'C' ELSE 'NC' END as click_conv_path
                                  from ", rs_tb_name_conv,"_icm_merged2 as m
                                  where eventind = 1
                                  group by m.channel, m.sub_channel,m.os_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from ", rs_tb_name_conv,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))
  
  #__Event_time_distribution___________INTERMEDIARY___
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_event_by_date;
                                  create table ", rs_tb_name_conv,"_event_by_date as(
                                  select imp_chan, imp_sub, imp_os, imp_dom, imp_hod, imp_dow, impressions,
                                  click_chan, click_sub, click_os, click_dom, click_hod, click_dow, clicks, imp_conv_path, click_conv_path 
                                  from ", rs_tb_name_conv,"_imp_time_distr as i
                                  FULL OUTER JOIN ", rs_tb_name_conv,"_click_time_distr as c
                                  ON imp_sub = click_sub
                                  AND imp_chan = click_chan
                                  AND imp_hod = click_hod
                                  AND imp_dow = click_dow
                                  AND imp_dom = click_dom
                                  AND imp_conv_path = click_conv_path
                                  AND imp_os = click_os);
                                  "))
  
}

  if (conv_type %in% c("NV","NVC")){
    
     #__Event_time_distribution___________INTERMEDIARY___
    dbSendQuery(redshift_con,paste0("
                                  drop table if exists ", rs_tb_name_conv,"_event_by_date;
                                  create table ", rs_tb_name_conv,"_event_by_date as(
                                  select imp_chan, imp_sub, imp_os, imp_dom, imp_hod, imp_dow, impressions,
                                  NULL as click_chan, NULL as click_sub, NULL as click_os, NULL as click_dom, 
                                  NULL as click_hod, NULL as click_dow, NULL as clicks, imp_conv_path, NULL as click_conv_path 
                                  from ", rs_tb_name_conv,"_imp_time_distr as i);
                                  "))    

    
  }  
  
  

  
  
  
  
  
  #_Monday:1__Tuesday:2__...__Saturday:6__Sunday:0_______
  #____Events_per_date_dow_&_hod___________________________
  ## why not select *? this whole bit could probably be cleaner
  event_dow_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                       select imp_chan, imp_sub, imp_os, imp_dom, imp_dow, imp_hod, sum(impressions) as impressions,
                                                       click_chan, click_sub, click_os, click_dom, click_dow, click_hod, sum(clicks) as clicks, imp_conv_path, click_conv_path
                                                       from ", rs_tb_name_conv,"_event_by_date
                                                       GROUP BY imp_chan, imp_sub, imp_os, imp_dom, imp_dow, imp_hod,
                                                       click_chan, click_sub, click_os, click_dom, click_dow, click_hod, imp_conv_path, click_conv_path"))
  event_dow_distr <- fetch(event_dow_distr_Q, n = -1)
  #rm(event_dow_distr_Q)
  
  event_dow_distr <- within(event_dow_distr, {
    # fill in the blanks in the corresponding columns
    imp_sub[which(is.na(imp_sub))]         <- click_sub[which(is.na(imp_sub))]   
    imp_chan[which(is.na(imp_chan))]       <- click_chan[which(is.na(imp_chan))] 
    imp_os[which(is.na(imp_os))] <- click_os[which(is.na(imp_os))]
    imp_dow[which(is.na(imp_dow))]         <- click_dow[which(is.na(imp_dow))]
    imp_hod[which(is.na(imp_hod))]         <- click_hod[which(is.na(imp_hod))]
    imp_dom[which(is.na(imp_dom))]         <- click_dom[which(is.na(imp_dom))]
    imp_conv_path[which(is.na(imp_conv_path))] <- click_conv_path[which(is.na(imp_conv_path))]
  })
  #### reread this to make sure - remove which for speed
  # clean up - remove unnecessary columns
  event_dow_distr <- event_dow_distr[, -which(names(event_dow_distr) %in% c(
    'click_chan',   'click_sub', 
    'click_dow',   'click_hod', 
    'click_dom',   'click_conv_path','click_os'))]
  
  event_dow_distr$impressions <- na.zero(event_dow_distr$impressions)
  event_dow_distr$clicks      <- na.zero(event_dow_distr$clicks)
  
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_chan')]      <- 'channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_sub')]       <- 'sub_channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dow')]       <- 'day_of_week'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dom')]       <- 'date'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_hod')]       <- 'hour_of_day'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_conv_path')] <- 'conv_path'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_os')] <- 'os_id'
  
  event_dow_distr <- as.data.table(event_dow_distr)
  #setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path)
  setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path,os_id)
  event_dow_distr <- as.data.frame(event_dow_distr[,list(impressions=sum(impressions),
                                                         clicks=sum(clicks)),
                                                   by=key(event_dow_distr)])
  
  
  # now for clicks/imps from event_dow
  event_dow_distr$record_week <- paste0(year(event_dow_distr$date),
                                        substr(ISOweek(event_dow_distr$date),7,8))
  event_dow_distr$record_month <- paste0(year(event_dow_distr$date),
                                         str_pad(month(event_dow_distr$date),2,pad="0"))
  
  ###
  events <- list(total_sub  = aggregate(cbind(clicks,impressions)~sub_channel+channel,
                                        event_dow_distr,FUN=sum),
                 total_chan = aggregate(cbind(clicks,impressions)~channel,
                                        event_dow_distr,FUN=sum),
                 monthly_sub = aggregate(cbind(clicks,impressions)~sub_channel+channel+record_month,
                                         event_dow_distr,FUN=sum),
                 monthly_chan = aggregate(cbind(clicks,impressions)~channel+record_month,
                                          event_dow_distr,FUN=sum),
                 weekly_sub  = aggregate(cbind(clicks,impressions)~sub_channel+channel+record_week,
                                         event_dow_distr,FUN=sum),
                 weekly_chan = aggregate(cbind(clicks,impressions)~channel+record_week,
                                         event_dow_distr,FUN=sum)
  )
  
  
  events$total_sub$level    <- "1_s"
  events$total_chan$level   <- "1_c"
  events$monthly_sub$level  <- "2_s"
  events$monthly_chan$level <- "2_c"
  events$weekly_sub$level   <- "3_s"
  events$weekly_chan$level  <- "3_c"
  
  
  events <- do.call(rbind.fill,events)
  events  <- events[,c("channel","sub_channel","record_month",
                       "record_week","clicks","impressions","level")]
  

  
  
  
  
  
  
  
    
  
  
  
  
  
  message(paste0("Aud Distr,  ",Sys.time()))
  #___aud_distribution___________combo____________ = double check the year is required for all these
  ## can you put channel in the sub_channel level as a group by column, then avoid the matching later???
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, count(distinct user_id) as audience
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by sub_channel"))
  aud_distr_sub <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, count(distinct user_id) as audience 
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by channel"))
  aud_distr_chan <- fetch(aud_distr_Q, n = -1)
  
  #___aud_distribution___________combo______by_Month______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_month, count(distinct user_id) as audience
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by sub_channel, record_month, record_year"))
  aud_distr_sub_month <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_month, count(distinct user_id) as audience
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by channel, record_month, record_year"))
  aud_distr_chan_month <- fetch(aud_distr_Q, n = -1)
  
  
  #___aud_distribution___________combo______by_Week______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_week, count(distinct user_id) as audience
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by sub_channel, record_week, record_year"))
  aud_distr_sub_week <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_week, count(distinct user_id) as audience
                                                 from ", rs_tb_name_conv,"_icm_merged2
                                                 group by channel, record_week, record_year"))
  aud_distr_chan_week <- fetch(aud_distr_Q, n = -1)
  
  #####################
  
  aud_distr_sub$channel <- chanLkup$channel[match(aud_distr_sub$sub_channel,
                                                  chanLkup$sub_channel)]
  aud_distr_sub_week$channel <- chanLkup$channel[match(aud_distr_sub_week$sub_channel,
                                                       chanLkup$sub_channel)]
  aud_distr_sub_month$channel <- chanLkup$channel[match(aud_distr_sub_month$sub_channel,
                                                        chanLkup$sub_channel)]
  
  
  aud_distr_chan_month$record_month <- paste0(aud_distr_chan_month$record_year,
                                              str_pad(aud_distr_chan_month$record_month,
                                                      width=2,pad="0"))
  aud_distr_sub_month$record_month  <- paste0(aud_distr_sub_month$record_year,
                                              str_pad(aud_distr_sub_month$record_month,
                                                      width=2,pad="0"))
  
  aud_distr_sub_week$record_week <- paste0(aud_distr_sub_week$record_year,
                                           str_pad(aud_distr_sub_week$record_week,width=2,pad="0"))
  aud_distr_chan_week$record_week <- paste0(aud_distr_chan_week$record_year,
                                            str_pad(aud_distr_chan_week$record_week,width=2,pad="0"))
  
  ####
  
  aud_distr <- list(total_sub=aud_distr_sub,
                    total_chan=aud_distr_chan,
                    monthly_sub=aud_distr_sub_month,
                    monthly_chan=aud_distr_chan_month,
                    weekly_sub=aud_distr_sub_week,
                    weekly_chan=aud_distr_chan_week)
  
  aud_distr$total_sub$level    <- "1_s"
  aud_distr$total_chan$level   <- "1_c"
  aud_distr$monthly_sub$level  <- "2_s"
  aud_distr$monthly_chan$level <- "2_c"
  aud_distr$weekly_sub$level   <- "3_s"
  aud_distr$weekly_chan$level  <- "3_c"
  
  aud_distr <- do.call(rbind.fill,aud_distr)
  aud_distr <- aud_distr[,c("channel","sub_channel","record_month",
                            "record_week","audience","level")]
  

### here get uniques. Moved from blue function

# make table of counts by channel and sub channel
dbSendQuery(redshift_con,paste0("
  drop table if exists ",rs_tb_name_conv,"_channel_count;
  create table ",rs_tb_name_conv,"_channel_count as(
    select user_id, count(distinct channel) from ", rs_tb_name_conv,"_icm_merged2
    group by user_id);
  "))
dbSendQuery(redshift_con,paste0("
  drop table if exists ",rs_tb_name_conv,"_sub_channel_count;
  create table ",rs_tb_name_conv,"_sub_channel_count as(
    select user_id, count(distinct sub_channel) from ", rs_tb_name_conv,"_icm_merged2
    group by user_id);
  "))

# make icm table by channel and sub channel where users have only 1 count in the above tables
dbSendQuery(redshift_con,paste0("
  drop table if exists ",rs_tb_name_conv,"_icm_unique_sub;
  create table ",rs_tb_name_conv,"_icm_unique_sub as(
    select * from ",rs_tb_name_conv,"_icm_merged2
    where user_id in (select a.user_id from ", rs_tb_name_conv,"_sub_channel_count as a
      where count = 1));
  "))
dbSendQuery(redshift_con,paste0("
  drop table if exists  ", rs_tb_name_conv,"_icm_unique_chan;
  create table ", rs_tb_name_conv,"_icm_unique_chan as(
    select * from ", rs_tb_name_conv,"_icm_merged2
    where user_id in (select a.user_id from ", rs_tb_name_conv,"_channel_count as a
      where count = 1));
  "))

uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, channel, count(distinct user_id) as uniques
                                                 from ", rs_tb_name_conv,"_icm_unique_sub
                                                 group by sub_channel, channel"))
  uni_distr_sub <- fetch(uni_distr_Q, n = -1)
  
  uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, count(distinct user_id) as uniques 
                                                 from ", rs_tb_name_conv,"_icm_unique_chan
                                                 group by channel"))
  uni_distr_chan <- fetch(uni_distr_Q, n = -1)
  
  #___uni_distribution___________combo______by_Month______
  uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel,channel, record_year,
                                                 record_month, count(distinct user_id) as uniques
                                                 from ", rs_tb_name_conv,"_icm_unique_sub
                                                 group by sub_channel, channel, record_month, record_year"))
  uni_distr_sub_month <- fetch(uni_distr_Q, n = -1)
  
  uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_month, count(distinct user_id) as uniques
                                                 from ", rs_tb_name_conv,"_icm_unique_chan
                                                 group by channel, record_month, record_year"))
  uni_distr_chan_month <- fetch(uni_distr_Q, n = -1)
  
  
  #___uni_distribution___________combo______by_Week______
  uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel,channel, record_year,
                                                 record_week, count(distinct user_id) as uniques
                                                 from ", rs_tb_name_conv,"_icm_unique_sub
                                                 group by sub_channel, channel, record_week, record_year"))
  uni_distr_sub_week <- fetch(uni_distr_Q, n = -1)
  
  uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_week, count(distinct user_id) as uniques
                                                 from ", rs_tb_name_conv,"_icm_unique_chan
                                                 group by channel, record_week, record_year"))
  uni_distr_chan_week <- fetch(uni_distr_Q, n = -1)
  
  #####################
  
  
  uni_distr_chan_month$record_month <- paste0(uni_distr_chan_month$record_year,
                                              str_pad(uni_distr_chan_month$record_month,
                                                      width=2,pad="0"))
  uni_distr_sub_month$record_month  <- paste0(uni_distr_sub_month$record_year,
                                              str_pad(uni_distr_sub_month$record_month,
                                                      width=2,pad="0"))
  
  uni_distr_sub_week$record_week <- paste0(uni_distr_sub_week$record_year,
                                           str_pad(uni_distr_sub_week$record_week,width=2,pad="0"))
  uni_distr_chan_week$record_week <- paste0(uni_distr_chan_week$record_year,
                                            str_pad(uni_distr_chan_week$record_week,width=2,pad="0"))
  
  ####
  
  uni_distr <- list(total_sub=uni_distr_sub,
                    total_chan=uni_distr_chan,
                    monthly_sub=uni_distr_sub_month,
                    monthly_chan=uni_distr_chan_month,
                    weekly_sub=uni_distr_sub_week,
                    weekly_chan=uni_distr_chan_week)
  
  uni_distr$total_sub$level    <- "1_s"
  uni_distr$total_chan$level   <- "1_c"
  uni_distr$monthly_sub$level  <- "2_s"
  uni_distr$monthly_chan$level <- "2_c"
  uni_distr$weekly_sub$level   <- "3_s"
  uni_distr$weekly_chan$level  <- "3_c"
  
  uni_distr <- do.call(rbind.fill,uni_distr)
  uni_distr <- uni_distr[,c("channel","sub_channel","record_month",
                            "record_week","uniques","level")]
  ############ END OF UNIQUES















 
  message(paste0("ICM sample,  ",Sys.time()))  
  icM_20_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ", rs_tb_name_conv,"_icm_20"))
  icM_20 <- fetch(icM_20_Q, n = -1)
  rm(icM_20_Q)
  
  
  
  message(paste0("ICM transpose,  ",Sys.time()))    
  trans1M_20_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ", rs_tb_name_conv,"_icm_transpose"))
  trans1M_20 <- fetch(trans1M_20_Q, n = -1)
  rm(trans1M_20_Q) 
  
  
  
  cols<-("user_id")
  for (i in 1:nrow(chanLkup)){
    temp<-paste0(chanLkup[i,2], "_1")
    temp2<-paste0(chanLkup[i,2], "_2")  
    cols<-c(cols, temp, temp2)
  }
  if ("Other"%in%chanLkup$sub_channel){cols<-c(cols, "Blank")}
  if ("!Other"%in%chanLkup$sub_channel){cols<-c(cols, c("Other_1" ,"Other_2"))}
  
  colnames(trans1M_20) <- cols
  
  
  
  
  message(paste0("ICM converters,  ",Sys.time())) 
  
  #for some reason the same file is created with two different names. need to check use in blue function
  convers_Q <- dbSendQuery(redshift_con, paste0("
                                                select distinct user_id from ", rs_tb_name_conv,"_icm_conv"))
  convers <- unlist(fetch(convers_Q, n = -1))
  
  icM_conv_sec_filt_Q <- dbSendQuery(redshift_con,paste0("
                                                         select * from ", rs_tb_name_conv,"_icm_conv
                                                         "))
  icM_conv_sec_filt <- fetch(icM_conv_sec_filt_Q, n = -1)
  
  message(paste0("NA,  ",Sys.time())) 
  na_Q <- dbSendQuery(redshift_con,paste0("
                                          select * from ", rs_tb_name_conv,"_na_filt_lc
                                          "))
  na <- fetch(na_Q, n = -1)
  na <- na[,1:2]
  

  message(paste0("export,  ",Sys.time()))
  #__export_________________________________________________________
  # visitors <- visitors$user_id
  
  
  
  #export benchmarking fo EMR process
  icm_count_Q <- dbSendQuery(redshift_con,paste0("
                                          select count(1) from ", rs_tb_name_conv,"_icm_merged2
                                          "))
  icm_count <- fetch(icm_count_Q, n = -1)
  


  
  benchmarking<-data.frame(username=username, DPID=DPID, Agency=agency, network=networkId, date=currentdate,
                           run_function="gold_red_function", version=1, clusters=clust_size, conv_type=conv_type,
                           icm_records=as.numeric(icm_count), conv=length(convers), trans_records=dim(trans1M_20)[1], trans_cols=dim(trans1M_20)[2],
                           run_time_minutes=as.numeric(difftime(EMREndTime, EMRStartTime, units="mins"))
  )
  
  filename=paste0("dp_benchmarking_", currentdate,"_", round(runif(1), 6)*1000000 ,".txt")
  write.table(benchmarking, file = filename, sep="\t", row.names = FALSE, col.names = FALSE, quote=FALSE)                                                
  s3UploadFile(KeyID, SecretAccessKey, s3Loc = paste0("s3://", bucket, "/dp/benchmarking/"), localFile=filename)
  unlink(filename, force=TRUE)
  
  
  
  #export files for blue_function
  
  prefix<-paste0(analyst,"_",DPID)
  
  save(aud_distr, file = file.path(file_save_location,paste0(prefix,"_aud_distr.RData")))
  save(uni_distr, file = file.path(file_save_location,paste0(prefix,"_uni_distr.RData")))
  if(conv_type=="C" || is.numeric(conv_type)){save(click_distr, file = file.path(file_save_location,paste0(prefix,"_click_distr.RData")))}
  save(imp_distr, file = file.path(file_save_location,paste0(prefix,"_imp_distr.RData")))
  save(na_os, file = file.path(file_save_location,paste0(prefix,"_na_os.RData")))
  save(chanLkup, file = file.path(file_save_location,paste0(prefix,"_chanLkup.RData")))
  save(aud_length, file = file.path(file_save_location,paste0(prefix,"_aud_length.RData")))
  save(event_dow_distr, file = file.path(file_save_location,paste0(prefix,"_event_dow_distr.RData")))
  save(events, file = file.path(file_save_location,paste0(prefix,"_events.RData")))
  save(os_distr,file = file.path(file_save_location,paste0(prefix,"_os_distr.RData")))
  save(city_distr,file = file.path(file_save_location,paste0(prefix,"_city_distr.RData")))
  save(lc, file = file.path(file_save_location,paste0(prefix,"_lc.RData")))
  save(lc_na, file = file.path(file_save_location,paste0(prefix,"_lc_na.RData"))) 
  save(na, file = file.path(file_save_location,paste0(prefix,"_na.RData")))
 
  save(icM_conv_sec_filt, file = file.path(file_save_location,paste0(prefix,"_icM_conv_sec_filt.RData")))
  save(convers, file = file.path(file_save_location,paste0(prefix,"_convers.RData")))
  
  save(icM_20, file = file.path(file_save_location,paste0(prefix,"_icM_20.RData")))
  save(trans1M_20, file = file.path(file_save_location,paste0(prefix,"_trans1M_20.RData")))

  
  end.time <- Sys.time()
  function_summary <- cat(paste0("Red funtion Runtime Stats:"),
                             paste0("Start Time: ",start.time),
                             paste0("Finish Time: ",end.time),
                             paste0("Run Time: ",difftime(end.time,start.time)),sep="\n")
  return(function_summary)
}



# s3toRedshift(
#   username, pw, dbname, dbHost, port = "5439", schema = paste0("ds_", agency),  tablename = paste0(username, "_", DPID, "_icm"),
#   column_names = c("user_id", "time", "os_id", "city_id", "page_id", "buy_id", "site_id", "browser_id", "advertiser_id", "record_month", "record_week", "record_year", "keyword_id", "eventind"),
#   column_types = c("varchar", "timestamp", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric", "numeric","varchar", "integer"),
#   KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "script/" ), ,delim="\t")


######test run



# gold_red_function(username=username, pw=pw, dbname = "annalectuk", dbHost = "54.77.118.78", 
#                   KeyID=KeyID, SecretAccessKey=SecretAccessKey, clust_size=30,
#                   conv_type="C", DPID="ET_CHRR_TEST", agency="omd",networkId="530", advertiserIds=c("856279", "1040292"), 
#                   campaignIds=c('8608700','8685166','8275648','8704058','8581290','6158304','7945110','8722740','7888562','8373606'),
#                   activity_sub_type=c("2014_000","2014_001"), startDate="2015-06-01", endDate="2015-06-30",
#                   startPre="2015-05-20", endPre="2015-05-31", NVprior=93, sample = 5, 
#                   file_save_location="P:/DPR/ET_test_run/CHRR", hierarchy_location="P:/DPR/ET_test_run/Hierachy/category_jun15.csv",
#                   group = T)


# blue_function(username=username,file_save_location="P:/DPR/ET_test_run/CH2",DPID="ET_CHRR_TEST",
#               cost_file_location="P:/DPR/ET_test_run/Costs/costs.csv",
#               output_dir="P:/DPR/ET_test_run/CHRR",
#               click_cutoff = 5,imp_cutoff=250,revenue_per_conv=10, extra_csv_outputs = F)


