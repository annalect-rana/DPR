

#' Pulls the data from s3 then runs the initial data processing
#'
#' Pulls the data from s3 then processes fully as an EMR job. 
#' The function offers a number of options including new visitor analysis, keyword matching, joining, and sampling. It requires a hierarchy file in a precise format. See the Arguments section for more details.
#' 
#' @param KeyID AWS Access Key ID
#' @param SecretAccessKey AWS Secret Access Key
#' @param clust_size AWS size of cluster
#' @param username optional username. the function doesn't need this and it is only used for naming the output files. if left NULL it will pick it up from the system. but on an EC2 this will be "Administrator"
#' @param DPID Unique identifier for this DP run. Must not contain spaces or any special characters
#' @param data_source The data source. "DC" for Double Click. "SZ" for Sizmek.
#' @param conv_type The conversion type of run you want to do. "C" for standard conversion, "NV" for new visitors, and "NVC" for new visitors conversions. When using "NV" and "NVC" don't forget to specify your prior period with the NVPrior option. Inputing a number between 0 and 1 will perform standard conversion but will sample the number of user_ids to the ratio you specify. i.e. 0.25 would give you a quarter of all the user_ids.     
#' @param chan_merge The channel merge method. "S" for a standard join to the Hierarchy File on page_id only.  "K" is for Keyword matching, and "KP" for keyword and page_id matching. If you select these you need to include ppc_hierarchy_location, and ppc_match_col 
#' @param join_type The join type between the clicks and impressions to the hierarchy. "left" for a left join. Any pageIDs not in the hierarchy file will be filled in with "Other". "inner" for an inner join. Only channels in the hierarchy file will be included in the analysis. 
#' @param searchgateway_sub A list of search related sub_channels to force as impressions when they are the last click event (gateway event), in a pathway, prior to conversion. Once forced to impressions you can force their model coeffecients to a tiny value to reduce their influence on the weightings by using the argument searchgateway_sub in the blue_function
#' @param agency What agency are you? eg "m2m", "omd", "mg", "phd". Except when using networkIDs 33505 and 6631. In that case specify the country of interest eg. "uk"
#' @param networkId The network ID for the Double Click data. Setting this to 33505 will direct the function to the LGI database or 6631 for the PSA EMEA database. Please make sure you provide the correct access keys. In this case please use agencyID argument to identify the country of interest
#' @param advertiserIds A character vector of advertiser ids to select
#' @param campaignIds A character vector of campaign ids to select. If left as NULL these will be populated from the buy_id column in the hierarchy file
#' @param activity_id A list of activity_id that specifiy the conversion events you wish to model on. If you a running a DP from pre May2016 you will need to specify activity_sub_types instead
#' @param activity_sub_type A list of what activity sub types will identify convertors
#' @param activity_type By default NULL. You can specify this if you need both a sub_activity_type and an activity_type to identify your converters
#' @param eventid.ign By default NULL. You can specify an eventid type you wish to exclude from the convertors.
#' @param startDate the start date of the DP in "YYYY-MM-DD" format
#' @param endDate the end date of the DP in "YYYY-MM-DD" format
#' @param startPre the start date of the warmup period for the DP in "YYYY-MM-DD" format
#' @param endPre the end date of the warmup period for the DP in "YYYY-MM-DD" format 
#' @param NVprior when running for New Visitors i.e conv_type is either "NV" or "NVC", this is the number of days in the prior period in which "new" visitors must not have been seen in
#' @param sample The sample sizes to feed into the model. Specify a multiple of converters you want from the non_converters i.e 20. Will give you 20 non-converters for every converter. Or specify a maximum limit for converters and non-converters i.e. c(50000, 500000) This would ensure you get a maximum of 50K converters and 500K non-converters. However if there are fewer than that available, it will just take as many as are available.
#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param costs_output By default FALSE. If set to TRUE will output a dummy cost file to be populated before inputing into the blue function
#' @param hierarchy_location Where the hierarchy file is saved. The hierachy file must contain 4 columns in this order page_id, buy_id, channel, sub_channel. It should also contain headers in the first row only. sub_channels must be unique. i.e. you can't have the same sub_channel under two different channels. channel and sub_channel names must not begin with numeric characters, and must only contain alphanumerics or underscores '_'. For example "Display Channel" would be "Display_Channel"
#' @param ppc_hierarchy_location The location of your keyword matching file. For keyword matching it should be a csv containing following three columns in this order: ppc_match_col, channel, sub_channel. For keyword and page_id matching it should be a csv containing following four columns in this order: page_id, ppc_match_col, channel, sub_channel. In both cases it should also contain headers in the first row only. sub_channels must be unique. i.e. you can't have the same sub_channel under two different channels. channel and sub_channel names must not begin with numeric characters, and must only contain alphanumerics or underscores '_'. For example "Display Channel" would be "Display_Channel"
#' @param ppc_match_col The name of the column in the dartsearch table you wish you perform keyword matching with. Either paid_search_campaign or paid_search_ad_group on Double Click data or keyword_id for Sizmek data.
#' @param group If you are part of the central team and don't have access to agency s3 buckets set this to true to run in the group area. By default FALSE 
#' 
#' @author James Thomson, \email{james.thomson@@omnicommediagroup.com}
#' @references \url{https://bitbucket.org/omguk-annalect/dpr/wiki/Home}
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location ready for use with the blue_function, rather than sent to the console.

#' @examples
#' 
#' #gold_function(username, pw=, 
#' #                  KeyID, SecretAccessKey, clust_size=30,
#' #                  conv_type="C", DPID="TEST", 
#' #                  agency="omd", networkId="530", advertiserIds=c("856279", "1040292"), 
#' #                  campaignIds=c('8608700','8685166','8275648','8704058','8581290','6158304','7945110','8722740','7888562','8373606'),
#' #                  activity_sub_type=c("2014_000","2014_001"), 
#' #                  startDate="2015-06-01", endDate="2015-06-30", startPre="2015-05-20", endPre="2015-05-31", 
#' #                  sample = 5,
#' #                  file_save_location="P:/DPR/test_run/", hierarchy_location="P:/DPR/test_run/Hierarchy/Hierarchy_file.csv")
#' #                  
#' #

gold_function <- function(
                         KeyID, SecretAccessKey, clust_size, username=NULL,
                         DPID, conv_type="C", data_source="DC", chan_merge="S", join_type="Left", searchgateway_sub=NULL,
                         agency, networkId, advertiserIds, campaignIds=NULL, 
                         activity_id=NULL,
                         activity_sub_type=NULL, activity_type=NULL, eventid.ign=NULL, 
                         startDate, endDate, startPre, endPre, NVprior=90,
                         sample=20, 
                         file_save_location, costs_output=FALSE,
                         hierarchy_location, 
                         ppc_hierarchy_location=NULL, ppc_match_col=NULL,
                         group=F) {
  
TTZ<-Sys.timezone()
Sys.setenv(TZ='UTC')

start.time <- Sys.time()


########################
### idiot checks########
########################

if(data_source=="DC" & !paste0(agency, "-", networkId)%in%c(
                                        "m2m-1707",  "m2m-5851",  "m2m-8001",  "m2m-8552",  
                                        "omd-530",  "omd-3434", "omd-9041", "omd-50502",
                                        "mg-1707",  "mg-5648",  "mg-9433",
                                        "phd-1137",  "phd-8971",  
                                        "roc-6050", "roc-244601", 
                                        "grp-6631", 
                                        "at-33505", "be-33505", "ch-33505", "cz-33505", "de-33505", "hu-33505", "ie-33505", "nl-33505", "pl-33505", "ro-33505", "sk-33505", "uk-33505",
                                        "at-6631", "ch-6631", "de-6631", "it-6631", "ru-6631", "tr-6631", "uk-6631", "za-6631"
                                        )
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

  if(is.null(activity_id) & is.null(activity_sub_type)){stop("You need to specify either a list of activity_ids or a list of activity_sub_types")}



  

  if(!chan_merge%in%c("S", "K", "KP")){stop("Not a correct merge type")}
  if(!tolower(join_type)%in%c("left", "inner")){stop("Not a join type")}
  if(chan_merge%in%c("K", "KP") & is.null(ppc_hierarchy_location)){stop("If you are doing a Keyword join you need to specify a ppc_hierarchy_location")}
  if(chan_merge%in%c("K", "KP") & is.null(ppc_match_col)){stop("If you are doing a Keyword join you need to specify a ppc_match_col")}
  if(chan_merge%in%c("K", "KP")){if(!ppc_match_col%in%c("paid_search_campaign", "paid_search_ad_group", "keyword_id")){stop("Not a correct ppc_match_col value")}}
  if(chan_merge%in%c("K", "KP") & is.null(campaignIds)){stop("If you are doing Keyword matching you need to declare your campaignIds")}


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
  
  #hierarchy files checks
  
  #import hierarchy file
  hierachy<-read.csv(file=hierarchy_location)
  colnames(hierachy) <- tolower(colnames(hierachy))
  

  if(!all(colnames(hierachy)==c("page_id","buy_id","channel","sub_channel"))){stop("Your Hierarchy file is in the wrong format. It must contain the columns page_id,buy_id,channel,sub_channel in that order")}
  if(any(duplicated(hierachy$page_id))) {warning("WARNING!!! There are duplicate page_ids in your hierarchy. This may cause duplication issues when merging onto your events.")}
  
  if(any(grepl("[:;&+()' !|/&-]", hierachy[,1]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in page_id. Please remove them before continuing")}
  if(any(grepl("[:;&+()' !|/&-]", hierachy[,2]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in buy_id. Please remove them before continuing")}
  if(any(grepl("[:;&+()' !|/&-]", hierachy[,3]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in channel. Please remove them before continuing")}
  if(any(grepl("[:;&+()' !|/&-]", hierachy[,4]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in sub_channel. Please remove them before continuing")}
  
  chanLkup<-aggregate(hierachy$page_id, by=list(hierachy$channel,hierachy$sub_channel), FUN=mean)
  chanLkup<-chanLkup[,1:2]
  colnames(chanLkup)<-c("channel", "sub_channel")
  
  if(any(duplicated(chanLkup$sub_channel))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(chanLkup$sub_channel)],collapse=",") , "\nYour Hierarchy File contains contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again"))}
  if(any(duplicated(tolower(chanLkup$sub_channel)))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(tolower(chanLkup$sub_channel))],collapse=",") , "\nYour Hierarchy File contains a sub_channel that is the same name but with a mixture of upper and lower cases. This will cause the process to fail. Please correct this and try again"))}
  
  #write clean version ready for upload to s3
  write.csv(hierachy, file=hierarchy_location, row.names = FALSE, quote=FALSE)
  
  if (chan_merge=="K"){
    
    #checks on the hierarchy file
    ppc_hierachy<-read.csv(file=ppc_hierarchy_location)
    colnames(ppc_hierachy) <- tolower(colnames(ppc_hierachy))
    
    if(!all(colnames(ppc_hierachy)==c("ppc_match_col","channel","sub_channel"))){stop("Your PPC Hierarchy file is in the wrong format. It must contain the columns ppc_match_col,channel,sub_channel in that order")}
    if(any(duplicated(ppc_hierachy$ppc_match_col))) {warning("WARNING!!! There are duplicate ppc_match_col in your PPC hierarchy. This may cause duplication issues when merging onto your events.")}
    
    if(any(grepl("[:;&+()' !|/&-]", ppc_hierachy[,2]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in buy_id. Please remove them before continuing")}
    if(any(grepl("[:;&+()' !|/&-]", ppc_hierachy[,3]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in channel. Please remove them before continuing")}
    
    ppcLkup<-aggregate(ppc_hierachy$ppc_match_col, by=list(ppc_hierachy$channel,ppc_hierachy$sub_channel), FUN=nrow)
    ppcLkup<-ppcLkup[,1:2]
    colnames(ppcLkup)<-c("channel", "sub_channel")
    
    if(any(duplicated(ppcLkup$sub_channel))){stop(paste0("Check sub_channel:", paste0(ppcLkup$sub_channel[duplicated(ppcLkup$sub_channel)],collapse=",") , "\nYour PPC Hierarchy File contains contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again"))}
    if(any(duplicated(tolower(ppcLkup$sub_channel)))){stop(paste0("Check sub_channel:", paste0(ppcLkup$sub_channel[duplicated(tolower(ppcLkup$sub_channel))],collapse=",") , "\nYour PPC Hierarchy File contains a sub_channel that is the same name but with a mixture of upper and lower cases. This will cause the process to fail. Please correct this and try again"))}
    
        
    #reconcile the two hierarchy files into one channel lookup

    all_chan<-rbind(ppc_hierachy[,2:3], hierachy[,3:4])

    chanLkup<-aggregate(all_chan$channel, by=list(all_chan$channel,all_chan$sub_channel), FUN=nrow)
    chanLkup<-chanLkup[,1:2]
    colnames(chanLkup)<-c("channel", "sub_channel")
    
    if(any(duplicated(chanLkup$sub_channel))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(chanLkup$sub_channel)],collapse=",") , "\nYour two Hierarchy Files contain contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again"))}
    if(any(duplicated(tolower(chanLkup$sub_channel)))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(tolower(chanLkup$sub_channel))],collapse=",") , "\nYour two Hierarchy Files contain a sub_channel that is the same name but with a mixture of upper and lower cases. This will cause the process to fail. Please correct this and try again"))}
 
    #write clean version ready for upload to s3
    write.csv(ppc_hierachy, file=ppc_hierarchy_location, row.names = FALSE, quote=FALSE)
    
  }
  
  
  if (chan_merge=="KP"){
    
    #checks
    ppc_hierachy<-read.csv(file=ppc_hierarchy_location)
    colnames(ppc_hierachy) <- tolower(colnames(ppc_hierachy))
    
    if(!all(colnames(ppc_hierachy)==c("page_id", "ppc_match_col","channel","sub_channel"))){stop("Your PPC Hierarchy file is in the wrong format. It must contain the columns page_id,ppc_match_col,channel,sub_channel in that order")}
    if(any(duplicated(ppc_hierachy$ppc_match_col))) {warning("WARNING!!! There are duplicate ppc_match_col in your PPC hierarchy. This may cause duplication issues when merging onto your events.")}
    
    if(any(grepl("[:;&+()' !|/&-]", ppc_hierachy[,3]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in channel. Please remove them before continuing")}
    if(any(grepl("[:;&+()' !|/&-]", ppc_hierachy[,4]))==TRUE){stop("There are illegal [' !|/-:;&+()] characters in sub_channel. Please remove them before continuing")}
    
    ppcLkup<-aggregate(ppc_hierachy$ppc_match_col, by=list(ppc_hierachy$channel,ppc_hierachy$sub_channel), FUN=nrow)
    ppcLkup<-ppcLkup[,1:2]
    colnames(ppcLkup)<-c("channel", "sub_channel")
    
    if(any(duplicated(ppcLkup$sub_channel))){stop(paste0("Check sub_channel:", paste0(ppcLkup$sub_channel[duplicated(ppcLkup$sub_channel)],collapse=",") , "\nYour PPC Hierarchy File contains contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again"))}
    if(any(duplicated(tolower(ppcLkup$sub_channel)))){stop(paste0("Check sub_channel:", paste0(ppcLkup$sub_channel[duplicated(tolower(ppcLkup$sub_channel))],collapse=",") , "\nYour PPC Hierarchy File contains a sub_channel that is the same name but with a mixture of upper and lower cases. This will cause the process to fail. Please correct this and try again"))}
    
    #reconcile the two hierarchy files into one channel lookup
    
    all_chan<-rbind(ppc_hierachy[,3:4], hierachy[,3:4])
    
    chanLkup<-aggregate(all_chan$channel, by=list(all_chan$channel,all_chan$sub_channel), FUN=nrow)
    chanLkup<-chanLkup[,1:2]
    colnames(chanLkup)<-c("channel", "sub_channel")
    
    if(any(duplicated(chanLkup$sub_channel))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(chanLkup$sub_channel)],collapse=",") , "\nYour two Hierarchy Files contain contains the same sub_channel for multiple channels. This will cause the process to fail. Please correct this and try again"))}
    if(any(duplicated(tolower(chanLkup$sub_channel)))){stop(paste0("Check sub_channel:", paste0(chanLkup$sub_channel[duplicated(tolower(chanLkup$sub_channel))],collapse=",") , "\nYour two Hierarchy Files contain a sub_channel that is the same name but with a mixture of upper and lower cases. This will cause the process to fail. Please correct this and try again"))}
    
    #write clean version ready for upload to s3
    write.csv(ppc_hierachy, file=ppc_hierarchy_location, row.names = FALSE, quote=FALSE)
    
   }
  
  
  
  
  
  
  
###########################################################
  
  #Sizmek data issues and warnings
  
  if(data_source=="SZ" & chan_merge=="KP")(stop("Due to the Sizmek data structure Keyword and Page matching will not work. Please consider just doing Keyword matching"))
  if(data_source=="SZ" & chan_merge=="S")(writeLines("WARNING - There are no page_ids for paid search in the sizmek data. All paid search will fall under other. Please consider doing Keyword matching instead"))
  
  
############################################################
  
 
  
  ########################
  ###parameter control####
  ########################
  
  #create campaignids
  if(is.null(campaignIds)){campaignIds<-unique(hierachy$buy_id)}
  
  #create username
  if(is.null(username)){username<-sub("[.]", "_", Sys.getenv("USERNAME"))}
  
  #sort out dates
  startPrior  <- as.Date(startPre)-(NVprior+1)
  endPrior    <- as.Date(startPre) 

  #identify unique months between start and end dates for pulling in the relevant double click data and partitioning queries
  priorMonths<-unique(format(seq(as.Date(startPrior), as.Date(endPrior), by="days"), "%Y%m"))
  preMonths<-unique(format(seq(as.Date(startPre), as.Date(endPre), by="days"), "%Y%m"))
  Months<-unique(format(seq(as.Date(startDate), as.Date(endDate), by="days"), "%Y%m"))

  #needs to be one date either side
  startDate <- as.Date(startDate)-1
  endDate   <- as.Date(endDate)+1
  startPre  <- as.Date(startPre)-1
  endPre    <- as.Date(endPre)+1

  if (conv_type=="C"||is.numeric(conv_type)){reqmonths<-unique(c(preMonths, Months))}
  if (conv_type %in% c("NV","NVC")){reqmonths<-unique(c(priorMonths, preMonths, Months))}
  
  currentdate<-format(Sys.Date(),"%Y%m%d")




  ########################
  ###### hive part #######
  ########################
  
  
  

#include the raws library. ideally it would be in the dependencies list on the description file, however it can't be as its not on CRAN 
library(RAWS)

#this will install the AWSCLI if not already installed
writeLines("Checking for AWSCLI")
installAWSCLI()


#define s3 bucket to store results depending on the agency and networkid

if(networkId=="33505"){bucket=paste0("lgi-staging/staging-",agency )}
if(networkId=="6631"){bucket=paste0("psa-staging/staging-",agency )}

if(!networkId%in%c("33505", "6631")){
  if (group==FALSE){bucket=paste0(agency, "-uk-staging")}
  if (group==TRUE){bucket="uk-staging"}
}



#create bespoke conversions table where clause. always include the sub_activity_type, then options include:
#if an activity_type list is specified it will add a clause to include only the activity_types in the list
#an eventid.ign is specified it will add a clause to ignore the eventid specigfied
#if nullpage.ign is specified it will add a clause to remove where page_id is not null

#need to select activity_sub_type from a.dc_na when DT1 and from b.dc_match_activity_cats when DT2
#this needs to be a case statement rather than coalesce as coalesce doesn't work on strings in hive
activity_type_set<-NULL

if(!is.null(activity_id)){
  activity_type_set<-paste0(activity_type_set,
                            "and a.DT2_Activity_ID in ('", paste0(activity_id, collapse = "','") , "')")
  
}


if(!is.null(activity_sub_type)){
  activity_type_set<-paste0(activity_type_set,
                            "and case when a.activity_sub_type='' then b.activity_sub_type else a.activity_sub_type end in ('", paste0(activity_sub_type, collapse = "','") , "')")

  }


if(!is.null(activity_type)){
  activity_type_set<-paste0(activity_type_set, 
                            " and case when a.activity_type='' then b.activity_type else a.activity_type end in ('", paste0(activity_type, collapse = "','") , "')"
  )
  }

if(!is.null(eventid.ign)){
  activity_type_set<-paste0(activity_type_set, " and event_id<>",eventid.ign)
}


#if(nullpage.ign){
#  activity_type_set<-paste0(activity_type_set, " and page_id is not null")
#}



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


#additioanl global variables dependent on inputs


if (length(sample)==1){
  set<-c(set, paste0("set sampleratio=",sample, ";\n"))
}

if (length(sample)==2){
  set<-c(set, paste0("set conv_limit=", format(sample[1], scientific = FALSE), ";\nset non_conv_limit=", format(sample[2], scientific = FALSE), ";\n"))
}

if (chan_merge%in%c("K", "KP")){
  set<-c(set, paste0("set ppc_match_col=", ppc_match_col,";\n"))
}

if (is.numeric(conv_type)){
  set<-c(set, paste0("set sample=", conv_type,";\n"))
} 


if (!is.null(searchgateway_sub)){
  set<-c(set, paste0("set searchgateway=('", paste0(searchgateway_sub, collapse = "','") , "');\n"))
} 

  


#Plug and Play Hive script sections

#data source: use scriptHiveDC to generate hive code to pull in the relevent data for those months, agency and networkID
if (data_source=="DC"){
  importdata<-scriptHiveDC(Agency=paste0(agency, "-", networkId), Month=reqmonths, colHeaders = "DT1",
                           Type=c("c","i","na","match_paid_search","match_cities","match_operating_systems","match_activity_cats"),
                           group=group)
}
if (data_source=="SZ"){
  importdata<-scriptHiveSZ(Agency=agency, Month=reqmonths, Type="ALL", group=group)
  data(hiveSizmek)
  importdata<-c(importdata, hiveSizmek)
}
#if (data_source=="PSA_DEIT"){
#  importdata<-scriptHivePSADEIT(advertiserIds, Month=reqmonths)
#  data(hivePSADEIT)
#  importdata<-c(importdata, hivePSADEIT)
#}





#All the following Hive Code Objects are just hard coded in a data object as part of the package. See the scripts in Dev to see what they contain




#Conversion type : 4 versions standard conversion, new visitors, new visitor conversions, sampling version
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

#standard main code v seo gateway version

if (!is.null(searchgateway_sub)){
  data(hiveMainCodeSEOGateway)
  hiveMainCode<-hiveMainCodeSEOGateway
}

if (is.null(searchgateway_sub)){
  data(hiveMainCode)
}


#pull in reporting code

data(hiveReportingCode)


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
#generate transpose hive code. which will perform a huge transpose of the icm table, ready as input to the model

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
  
  
  
  

#generate s3 location for script and logs to be added to 
s3loc=paste0("s3://", bucket, "/dp/", username, "/", agency, "_", networkId, "/", DPID, "/", currentdate, "/" )
scriptloc=paste0(s3loc, "script/" )
logsloc=paste0(s3loc, "logs" )


#write script and upload to s3
writeLines("Generating Hive Script and Uploading it to s3")
scriptSaveS3(KeyID, SecretAccessKey, scripts=c(importdata, set, typecode, mergecode1, hiveMainCode, samplecode, transpose, mergecode2, hiveReportingCode), loc=scriptloc)

#upload hierarchy to s3
writeLines("Uploading Hierarchy File to s3")
s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "hierarchy_file/"), hierarchy_location)

if (chan_merge=="K"){
  writeLines("Uploading PPC Hierarchy File to s3")
  s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "ppc_hierarchy_file/"), ppc_hierarchy_location)
}


if (chan_merge=="KP"){
  writeLines("Uploading PPC Hierarchy File to s3")
  s3UploadFile(KeyID, SecretAccessKey, paste0(s3loc, "ppc_hierarchy_file/"), ppc_hierarchy_location)
}


##### RUN THE HIVE EMR JOB #################################

#check cluster availability
free_clusters<-EC2Usage(KeyID, SecretAccessKey, output="Free")
if(free_clusters<clust_size){stop("There aren't enough EC2s available to run this. Please use the EC2Usage function to check this and adjust your clust_size argument accordingly")}


EMRStartTime<-Sys.time()
writeLines(paste0("Started HiveEMR job at ", Sys.time(),  " This make take a while"))

if (networkId==33505) {
  clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
                     s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency=paste0("LGI - ", toupper(agency)))
}

if (networkId==6631) {
  clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
                     s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency=paste0("PSA - ", toupper(agency)))
}


if (!networkId%in%c(33505, 6631) & group==FALSE) {
clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
        s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency=paste0("Annalect UK - ", toupper(agency)))
}

if (!networkId%in%c(33505, 6631) & group==TRUE) {
clusterID<-EMRHive(KeyID, SecretAccessKey, instances=clust_size, name=paste0(username, " - DP HiveEMR"), logLoc=logsloc, 
        s3HiveLoc=paste0(scriptloc, "script.q"), Project=paste0("DP_",DPID), Agency="Annalect UK")
}


#need to monitor job in order to know when its finished
x<-1
while(x==1){
  state<-EMRMonitor(KeyID, SecretAccessKey, clusterID, out=T)
  if(any(state%like%"TERMINATED")){x=2}
}
writeLines(paste0("HiveEMR job complete at ", Sys.time()))
EMREndTime<-Sys.time()

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






###########################################################################

message(paste0("download files from s3,  ",Sys.time()))

#download the relevent files
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "icm_transpose/"), fileLoc=paste0(file_save_location, "/froms3"), filename='icm_transpose')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "dc_icm_sample/"), fileLoc=paste0(file_save_location, "/froms3"), filename='icm_sample')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "dc_icm_conv/"), fileLoc=paste0(file_save_location, "/froms3"), filename='icm_conv')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "LC_merged/"), fileLoc=paste0(file_save_location, "/froms3"), filename='LC_merged')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "dc_na_filt_LC/"), fileLoc=paste0(file_save_location, "/froms3"), filename='na_filt_LC')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "audience_count/"), fileLoc=paste0(file_save_location, "/froms3"), filename='audience_count')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "os_distr/"), fileLoc=paste0(file_save_location, "/froms3"), filename='os_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "city_distr/"), fileLoc=paste0(file_save_location, "/froms3"), filename='city_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "imp_distr/"), fileLoc=paste0(file_save_location, "/froms3"), filename='imp_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "click_distr/"), fileLoc=paste0(file_save_location, "/froms3"), filename='click_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "click_imp_time_distr/"), fileLoc=paste0(file_save_location, "/froms3"), filename='click_imp_time_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "events/"), fileLoc=paste0(file_save_location, "/froms3"), filename='events')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "aud_distr_final/"), fileLoc=paste0(file_save_location, "/froms3"), filename='aud_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "unq_distr_final/"), fileLoc=paste0(file_save_location, "/froms3"), filename='unq_distr')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "na_filt_browser/"), fileLoc=paste0(file_save_location, "/froms3"), filename='na_filt_browser')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "na_filt_os/"), fileLoc=paste0(file_save_location, "/froms3"), filename='na_filt_os')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "icm_count/"), fileLoc=paste0(file_save_location, "/froms3"), filename='icm_count')
s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "costs/"), fileLoc=paste0(file_save_location, "/froms3"), filename='costs')


if (length(sample)==2){
  s3CreateFile(KeyID, SecretAccessKey, s3Loc=paste0(s3loc, "dc_icm_conv_counts/"), fileLoc=file_save_location, filename='conv_ratio_counts')
}


###################################################################


message(paste0("importing files into R,  ",Sys.time()))
#import files into R and define the formats and column names

trans1M_20<-read.csv(paste0(file_save_location, "/froms3/", "icm_transpose.txt"), sep="\t", header=F, stringsAsFactors=FALSE, na.strings="\\N")

icM_20<-read.csv(paste0(file_save_location, "/froms3/", "icm_sample.txt"), sep="\t", header=F, 
                 colClasses = c("character","POSIXct","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","character","integer","character","character","integer"), na.strings="\\N")

icm_conv<-read.csv(paste0(file_save_location, "/froms3/", "icm_conv.txt"), sep="\t", header=F, 
                   colClasses = c("character","POSIXct","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","numeric","character","integer","character","character","integer", "numeric", "numeric"), na.strings="\\N")

lc_na<-read.csv(paste0(file_save_location, "/froms3/", "LC_merged.txt"), sep="\t", header=F, 
                colClasses=c("character", "POSIXct", "character", "character", "numeric", "numeric", "numeric"), na.strings="\\N")

na_filt_LC<-read.csv(paste0(file_save_location, "/froms3/", "na_filt_LC.txt"), sep="\t", header=F, 
                     colClasses = c("character","POSIXct","character","numeric","numeric","character","numeric","numeric","numeric","numeric","numeric", "numeric","numeric","numeric","integer"), na.strings="\\N")

aud_length<-read.csv(paste0(file_save_location, "/froms3/", "audience_count.txt"), sep="\t", header=F, 
                     colClasses = "numeric", na.strings="\\N")

icm_length<-read.csv(paste0(file_save_location, "/froms3/", "icm_count.txt"), sep="\t", header=F, 
                     colClasses = "numeric", na.strings="\\N")

os_distr<-read.csv(paste0(file_save_location, "/froms3/", "os_distr.txt"), sep="\t", header=F,
                  colClasses = c("numeric","character","character","numeric","numeric", "character"), na.strings="\\N")

city_distr<-read.csv(paste0(file_save_location, "/froms3/", "city_distr.txt"), sep="\t", header=F, 
                     colClasses = c("numeric","character","character","numeric","numeric", "character"), na.strings="\\N")

imp_distr<-read.csv(paste0(file_save_location, "/froms3/", "imp_distr.txt"), sep="\t", header=F, 
                    colClasses = c("character","numeric"), na.strings="\\N")

if (conv_type=="C"||is.numeric(conv_type)) {  
click_distr<-read.csv(paste0(file_save_location, "/froms3/", "click_distr.txt"), sep="\t", header=F, 
                      colClasses = c("character","numeric"), na.strings="\\N")
}

event_dow_distr<-read.csv(paste0(file_save_location, "/froms3/", "click_imp_time_distr.txt"), sep="\t", header=F, 
                          colClasses=c("character","character","Date", "integer", "integer", "character", "numeric","numeric","numeric", "character","character"), na.strings="\\N")


events<-read.csv(paste0(file_save_location, "/froms3/", "events.txt"), sep="\t", header=F, 
                    colClasses=c("character","character","character","character","numeric","numeric", "character"), na.strings=c("\\N", ""))

aud_distr<-read.csv(paste0(file_save_location, "/froms3/", "aud_distr.txt"), sep="\t", header=F, 
                    colClasses=c("character","character","character","character", "numeric", "character"), na.strings=c("\\N", ""))

uni_distr<-read.csv(paste0(file_save_location, "/froms3/", "unq_distr.txt"), sep="\t", header=F, 
                    colClasses=c("character","character","character","character", "numeric", "character"), na.strings=c("\\N", ""))

na_browser<-read.csv(paste0(file_save_location, "/froms3/", "na_filt_browser.txt"), sep="\t", header=F, 
                     colClasses = c("character","POSIXct","numeric"), na.strings="\\N")

na_os<-read.csv(paste0(file_save_location, "/froms3/", "na_filt_os.txt"), sep="\t", header=F, 
                     colClasses = c("character","POSIXct","numeric"), na.strings="\\N")

costs<-read.csv(paste0(file_save_location, "/froms3/", "costs.txt"), sep="\t", header=F, 
                colClasses = c("character", "character", "character", "integer", "integer"), na.strings="\\N")







#add headers
  cols<-("user_id")
  for (i in 1:nrow(chanLkup)){
    temp<-paste0(chanLkup[i,2], "_1")
    temp2<-paste0(chanLkup[i,2], "_2")  
    cols<-c(cols, temp, temp2)
  }
  if ("Other"%in%chanLkup$sub_channel){cols<-c(cols, "Blank")}
  if (!"Other"%in%chanLkup$sub_channel){cols<-c(cols, c("Other_1" ,"Other_2"))}


colnames(trans1M_20) <- cols
colnames(icM_20)<-c("user_id","time","city_id","os_id","page_id", "buy_id","browser_id","site_id", "advertiser_id", "record_month", "record_week", "record_year", "keyword_id", "eventind", "channel", "sub_channel", "row_rank")
colnames(icm_conv)<-c("user_id","time","city_id","os_id","page_id", "buy_id","browser_id","site_id", "advertiser_id", "record_month", "record_week", "record_year", "keyword_id", "eventind", "channel", "sub_channel", "row_rank", "initial_revenue", "all_revenue")
colnames(na_filt_LC)<-c("user_id","time","activity_sub_type","os_id","browser_id","keyword_id","page_id", "buy_id","site_id", "advertiser_id","revenue","record_month", "record_week", "record_year", "row_rank")
colnames(lc_na)<-c("user_id","time","channel", "sub_channel", "record_month", "record_week", "record_year")   
colnames(aud_length)<-c("total") 
colnames(icm_length)<-c("total") 
colnames(os_distr)<-c("os_id","channel","sub_channel","users","conv", "os")   
colnames(city_distr)<-c("city_id","channel","sub_channel","users","conv", "city")  
colnames(imp_distr)<-c("sub_channel","imps")   
if(conv_type=="C"||is.numeric(conv_type)) {colnames(click_distr)<-c("sub_channel","clicks")}
colnames(event_dow_distr)<-c("channel", "sub_channel", "date", "day_of_week", "hour_of_day", "conv_path", "os_id", "impressions", "clicks", "record_week", "record_month")   
colnames(events)<-c("channel", "sub_channel", "record_month", "record_week", "clicks", "impressions", "level" )   
colnames(aud_distr)<-c("channel","sub_channel", "record_month", "record_week","audience", "level") 
colnames(uni_distr)<-c("channel","sub_channel", "record_month", "record_week","uniques", "level") 
colnames(na_browser)<-c("user_id","time","browser_id") 
colnames(na_os)<-c("user_id","time","os_id")

colnames(costs)<-c("date","channel", "sub_channel", "impressions", "clicks")
costs$media_cost<-0
costs$revenue<-0
costs$date<-format(as.Date(costs$date), "%d/%m/%Y")

##########################################


#Process R data
#NA
 
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
  
  


  icM_conv_sec_filt <- icm_conv
  convers<-unique(icm_conv$user_id)

  na <- na_filt_LC
  na <- na[,1:2]
  
  #force this to a numeric to keep the blue_function happy
  aud_length<-as.numeric(aud_length)
  icm_length<-as.numeric(icm_length)
  

  
#########################################################################  
  
  benchmarking<-data.frame(username=username, DPID=DPID, Agency=agency, network=networkId, date=currentdate,
                           run_function="gold_function", version=3, clusters=clust_size, conv_type=conv_type,
                           icm_records=icm_length, conv=length(convers), trans_records=dim(trans1M_20)[1], trans_cols=dim(trans1M_20)[2],
                           run_time_minutes=as.numeric(difftime(EMREndTime, EMRStartTime, units="mins"))
  )
  
  filename=paste0("dp_benchmarking_", currentdate,"_", round(runif(1), 6)*1000000 ,".txt")
  write.table(benchmarking, file = paste0(file_save_location, "/", filename), sep="\t", row.names = FALSE, col.names = FALSE, quote=FALSE)                                                
  s3UploadFile(KeyID, SecretAccessKey, s3Loc = paste0("s3://", bucket, "/dp/benchmarking/"), localFile=paste0(file_save_location, "/", filename))
  unlink(paste0(file_save_location, "/", filename), force=TRUE)
  
  
  
  
  
  
  message(paste0("save R files,  ",Sys.time()))
  #__export_________________________________________________________
  # visitors <- visitors$user_id
  
  prefix<-paste0(username,"_",DPID)
  
  save(aud_distr, file = file.path(file_save_location,paste0(prefix,"_aud_distr.RData")))
  save(uni_distr, file = file.path(file_save_location,paste0(prefix,"_uni_distr.RData")))
    if(conv_type=="C" || is.numeric(conv_type)){save(click_distr, file = file.path(file_save_location,paste0(prefix,"_click_distr.RData")))}
  save(imp_distr, file = file.path(file_save_location,paste0(prefix,"_imp_distr.RData")))
  save(na_browser, file = file.path(file_save_location,paste0(prefix,"_na_browser.RData")))
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

  if(costs_output){write.csv(costs, file=paste0(file_save_location, "/costs_file.csv"), row.names=FALSE, quote=FALSE)}
  
  if (length(sample)==2){
    writeLines("As you have selected to sample both convertors and non-converters an additional file will be created in your file_save_location called conv_ratio_counts.txt. It contains counts of total converters and sampled converters. This can be used to upweight the output if required")
  }
  
  
  
  
  unlink(paste0(file_save_location, "/froms3"), force=T, recursive=T)
  
  
  end.time <- Sys.time()
  function_summary <- cat(paste0("Gold funtion Runtime Stats:"),
                             paste0("Start Time: ",start.time),
                             paste0("Finish Time: ",end.time),
                             paste0("Run Time: ",difftime(end.time,start.time)),sep="\n")
  Sys.setenv(TZ=TTZ)
  return(function_summary)
}

