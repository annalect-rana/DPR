#' Runs the initial red process - missing out creation of i, c na and pre_na_uid tables

#'

#' Runs the initial red process - missing out creation of i, c na and pre_na_uid tables

#'

#' @param username The username for redshift
#' @param pw The password for that username
#' @param DPID A string which describes the DP run. eg 'HTC_JUNE'
#' @param dbname Database name, defaults to "annalectuk"
#' @param dbHost Database host, defaults to "54.77.118.78"
#' @param agency What agency are you? eg "m2m", "omd", "mg", "phd"
#' @param advertiserIds A character vector of advertiser ids to select
#' @param campaignIds A character vector of campaign ids to select
#' @param activity_sub_type what activity sub type are we interested in?
#' @param startDate the start date of the DP in "YYYY-MM-DD" format
#' @param endDate the end date of the DP in "YYYY-MM-DD" format
#' @param startPre the start date of the warmup period for the DP in "YYYY-MM-DD" format
#' @param endPre the end date of the warmup period for the DP in "YYYY-MM-DD" format 
#' @param sample_factor The multiple of converters you want from the non_converters - defaults to 20. This may be too large for some large clients (could cause memory prblems)
#' @param sample_seed This is a number you can specify so the sampling is exactly the same each time. Defaults to NULL.
#' @param parallel Defaults to TRUE, whether you want to run some processes in parallel on your machine. Set this to false if you want to leave some cores available for other processes. The parallel operations are not running throughout so should cause minimal disruption.
#' @param reupload_hierarchy Defaults to TRUE. If the hierarchy is already uploaded and is current and correct then this can be set to FALSE and a new one will not be uploaded.
#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param hierarchy_location Where the hierarchy is saved.
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' @examples
#' 
#' #red_function_DK(username="tim_scholtes",pw="",
#' DPID="HTC_JUNE",
#' agency="m2m",networkId="5851",
#' advertiserIds= c(
#' "2326173",    "2326266",    "4304358",    "4705211",
#' "4705212",    "4705613",    "4705617",    "4705805",
#' "4705811",    "4705815",    "4705826"),
#' campaignIds=c(
#' "7941825",  "8574112",  "8561809",
#' "8515806",    "8590693",    "8551379",    "8684699",
#' "8612335",    "8516389",    "8522817",    "8591270",
#' "8565152",    "8514189",    "8590689",    "8462957",
#' "8590872",    "8676743",    "8591271",    "8495905",
#' "8684932",    "8590678",    "8640691",    "8405167",
#' "8622631",    "8462956",    "8512842",    "8676843"),
#' activity_sub_type="hpuks0",
#' startDate="2015-04-01",endDate="2015-04-30",
#' startPre="2015-03-18",endPre="2015-03-31",
#' parallel=T, reupload_hierarchy = T,
#' file_save_location="Z:/GROUP/DPR/TIM_test/",
#' hierarchy_location="Z:/GROUP/DPR/TIM_test/hierarchy.csv") # not run

#' 

red_function_DK <- function(username,pw,DPID,dbname="annalectuk",dbHost="54.77.118.78",
                         agency,networkId,advertiserIds,campaignIds,activity_sub_type,
                         startDate,endDate,startPre,endPre,
                         sample_factor=20,sample_seed=NULL,
                         parallel=TRUE, reupload_hierarchy = TRUE,
                         file_save_location,hierarchy_location=NULL) {
  
  start.time <- Sys.time()
  
  ### checks
  
  if(missing(username)) {
    stop("argument username is missing")}
  if(missing(pw)) {
    stop("argument pw is missing")}
  if(missing(DPID)) {
    stop("argument DPID is missing")}
  if(missing(agency)) {
    stop("argument agency is missing")}
  if(missing(networkId)) {
    stop("argument networkId is missing")}
  if(missing(advertiserIds)) {
    stop("argument advertiserIds is missing")}
  if(missing(campaignIds)) {
    stop("argument campaignIds is missing")}
  if(missing(activity_sub_type)) {
    stop("argument activity_sub_type is missing")}
  if(missing(startDate)) {
    stop("argument startDate is missing")}
  if(missing(endDate)) {
    stop("argument endDate is missing")}
  if(missing(startPre)) {
    stop("argument startPre is missing")}
  if(missing(endPre)) {
    stop("argument endPre is missing")}
  if(missing(file_save_location)) {
    stop("argument file_save_location is missing")}
options(warn=1)
  failure <- checks_function1(startDate = startDate,
                              reupload_hierarchy = reupload_hierarchy,
                              hierarchy_location=hierarchy_location,
                              endDate=endDate,
                              startPre=startPre,
                              endPre=endPre)
  
  # hierarchy upload
  # hierarchy column names

  message(paste0("Red process started at ",start.time))
  # DB control --------------------------------------------------------------
  #dbname <- "annalectuk" 
  #dbHost <- "54.77.118.78"
  port <-"5439"
  drv <- dbDriver("PostgreSQL")
  redshift_con <- dbConnect(drv, dbname = dbname, user = username, password = pw, 
                            host = dbHost, port = port)
  
#   # parameter control -------------------------------------------------------
  
  # needs to be one date either side
  startDate <- as.Date(startDate)-1
  endDate   <- as.Date(endDate)+1
  startPre  <- as.Date(startPre)-1
  endPre    <- as.Date(endPre)+1
  
  analyst   <- username
  
  advertiserIds     <- paste0(advertiserIds,collapse=", ")
  campaignIds       <- paste0(campaignIds,collapse=", ")
  activity_sub_type <- paste0(activity_sub_type, collapse = "','")
  
  # REDSHIFT BEGIN ----------------------------------------------------------

  # BASE TABLES
  #__na__Table________________________________
  message(paste0("NA, pre_na_uid, click and impression tables must be present already. Proceeding from creation of icm table:  ",Sys.time()) ) 

  
  #_______icM________________
  message(paste0("icM,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_icm;
                                  create table ds_",agency,".",analyst,"_",DPID,"_icm as
                                  SELECT user_id, c.time, os_id, city_id,page_id,
                                  buy_id, site_id,browser_id,
                                  advertiser_id, record_month, record_week,
                                  record_year,keyword_id, 1 as eventind
                                  from ds_",agency,".",analyst,"_",DPID,"_c as c
                                  where user_id != 0
                                  UNION ALL
                                  SELECT user_id, i.time, os_id,city_id, page_id,
                                  buy_id, site_id,browser_id, 
                                  advertiser_id, record_month, record_week, 
                                  record_year, '0' as keyword_id ,2 as eventind
                                  from ds_",agency,".",analyst,"_",DPID,"_i as i
                                  where user_id != 0;
                                  "))
  
  
  
  #______________ UPLOAD HIERARCHY IN BLOCKS _____________
  if(reupload_hierarchy) {
    message(paste0("Upload Hierarchy,  ", Sys.time()))
    hierarchy <- read.csv(hierarchy_location)
    colnames(hierarchy) <- tolower(colnames(hierarchy))
    colnames(hierarchy) <- gsub("[^a-z_]*","",colnames(hierarchy))

    # clean hierarchy to make the channel and sub channel names R friendly
    hierarchy$channel <- gsub('\\-', '_',
                          gsub('\\+', '', 
                               gsub(' ', '_',
                                 gsub("'","", hierarchy$channel))))
        hierarchy$sub_channel <- gsub('\\-', '_',
                          gsub('\\+', '', 
                               gsub(' ', '_',
                                 gsub("'","",  hierarchy$sub_channel))))
    
    #___create category table_____________________
    dbSendQuery(redshift_con,paste0("
                                    drop table if exists ds_",agency,".",analyst,"_",DPID,"_category;
                                    create table ds_",agency,".",analyst,"_",DPID,"_category (
                                    page_id numeric(18,0),
                                    buy_id numeric(18,0),
                                    channel varchar(500),
                                    sub_channel varchar(500));
                                    "))
    
    N <- nrow(hierarchy)
    n <- 1000
    reps <- N%/%n
    
    if(reps>=1) {
      #____insert into category table___ to be sped up by inserting multiple lines at once
      for(i in 1:reps) {
        print(paste0("Inserting hierarchy row block",i*n))
        dbSendQuery(redshift_con,paste0(" 
                                        INSERT INTO ds_",agency,".",analyst,"_",DPID,"_category (page_id
                                        , buy_id
                                        , channel
                                        , sub_channel
                                        ) VALUES (",
                                  do.call(paste0,list(lapply((i-1)*n+1:n,function(k) {
                                    paste0(paste0(hierarchy[k,c("page_id","buy_id")],collapse=", "), 
                                           ", '",
                                           hierarchy[k,c("channel")],
                                           "', '",
                                           hierarchy[k,c("sub_channel")],
                                           "'"
                                    )}),collapse=" ), (")),
                                  ");"))
        
    }
    
    dbSendQuery(redshift_con,paste0(" 
        INSERT INTO ds_",agency,".",analyst,"_",DPID,"_category (page_id
        , buy_id
        , channel
        , sub_channel
        ) VALUES (",
      do.call(paste0,list(lapply((reps*n+1):N,function(i) {
        paste0(paste0(hierarchy[i,c("page_id","buy_id")],collapse=", "), 
               ", '",
               hierarchy[i,c("channel")],
               "', '",
               hierarchy[i,c("sub_channel")],
               "'"
        )}),collapse=" ), (")),
                      ");"))
  } else {
    dbSendQuery(redshift_con,paste0(" 
                                    INSERT INTO ds_",agency,".",analyst,"_",DPID,"_category (page_id
                                    , buy_id
                                    , channel
                                    , sub_channel
                                    ) VALUES (",
                                    do.call(paste0,list(lapply(1:N,function(i) {
                                      paste0(paste0(hierarchy[i,c("page_id","buy_id")],collapse=", "), 
                                             ", '",
                                             hierarchy[i,c("channel")],
                                             "', '",
                                             hierarchy[i,c("sub_channel")],
                                             "'"
                                      )}),collapse=" ), (")),
                                    ");"))
    }
  
  } ### write an else to check if the hierarchy table exists
  
  #______MATCH ONTO HIERARCHY
  message(paste0("Match Hierarchy,  ", Sys.time()))
  dbSendQuery(redshift_con,paste0("
  DROP table if exists ds_",agency,".",analyst,"_",DPID,"_icm_merged;
  CREATE table ds_",agency,".",analyst,"_",DPID,"_icm_merged as
  select *, row_number() over(partition by user_id,sub_channel,time order by user_id) as row_rank
  from(
  SELECT user_id, ic.time, coalesce(channel,'Other') as channel,
  coalesce(sub_channel,'Other') as sub_channel, os_id,city_id,browser_id,
  eventind, record_month, record_week, record_year
  FROM ds_",agency,".",analyst,"_",DPID,"_icm as ic
  LEFT JOIN ds_",agency,".",analyst,"_",DPID,"_category as cat
  ON ic.page_id = cat.page_id)
  "))

  ####  remove duplicate EVENTS by sub_channel, UID, time
  dbSendQuery(redshift_con,paste0(
    "DELETE FROM ds_",agency,".",analyst,"_",DPID,"_icm_merged
    WHERE row_rank > 1"))

  
  #___Channel_lookup_______________________________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_channel_lookup;
                                  create table ds_",agency,".",analyst,"_",DPID,"_channel_lookup as
                                  select channel, sub_channel
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_merged
                                  group by channel, sub_channel;
                                  "))
  
  channel_lookup_Q <- dbSendQuery(redshift_con,paste0("
                                                      select * from ds_",agency,".",analyst,"_",DPID,"_channel_lookup
                                                      "))
  chanLkup <- as.data.frame(fetch(channel_lookup_Q, n = -1))
  
  #___Filtered_conversions_______________________________
  message(paste0("Filter conversions,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_na_filt;
                                  CREATE TABLE ds_",agency,".",analyst,"_",DPID,"_na_filt AS(
                                  SELECT s.user_id, min(s.time) as time
                                  FROM ds_",agency,".",analyst,"_",DPID,"_na AS s
                                  INNER JOIN ds_",agency,".",analyst,"_",DPID,"_icm AS u
                                  ON s.user_id = u.user_id
                                  GROUP BY s.user_id)")) 
  # users outside intersection na and icm tables filtered from na
  # to be discussed as to whether zero interaction UIDs should be kept.
  
  
  #___Filtered conversions for Last click - left join is because I have already cleaned out dups
  message(paste0("LC conversions,  ", Sys.time()))
  dbSendQuery(redshift_con,paste0(" 
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_na_filt_LC; 
                                  CREATE TABLE ds_",agency,".",analyst,"_",DPID,"_na_filt_LC AS( 
                                  SELECT s.user_id, s.time,
                                  u.page_id,u.buy_id,u.site_id,u.record_month,u.record_week,u.record_year 
                                  from ds_",agency,".",analyst,"_",DPID,"_na_filt 
                                  AS s 
                                  LEFT JOIN ds_",agency,".",analyst,"_",DPID,"_na AS u 
                                  ON s.user_id = u.user_id 
                                  AND s.time = u.time)"))
  
  
  
  #___Filtered_conversions by browser - used for one of the outputs__________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_na_filt_browser;
                                  CREATE TABLE ds_",agency,".",analyst,"_",DPID,"_na_filt_browser as
                                  SELECT s.user_id, s.time, u.browser_id
                                  FROM ds_",agency,".",analyst,"_",DPID,"_na_filt AS s
                                  left JOIN ds_",agency,".",analyst,"_",DPID,"_na AS u
                                  ON s.user_id = u.user_id and s.time = u.time")) 
  
  #_____Converters_with_time_differences____________DONE___
  message(paste0("icm_conv,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_icm_conv;
                                  create table ds_",agency,".",analyst,"_",DPID,"_icm_conv as(
                                  select m.*
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_merged as m
                                  INNER JOIN ds_",agency,".",analyst,"_",DPID,"_na_filt as s
                                  on m.user_id = s.user_id
                                  where (extract(epoch from s.time) - extract(epoch from m.time)) > 0
                                  );")) # icm for converters only with post-conversion events filtered out.
  
  icM_conv_sec_filt_Q <- dbSendQuery(redshift_con,paste0("
                                                         select * from ds_",agency,".",analyst,"_",DPID,"_icm_conv
                                                         "))
  icM_conv_sec_filt <- fetch(icM_conv_sec_filt_Q, n = -1)
  
  # This should be the final flat table for all subsequent calculations:
  message(paste0("icm_merged,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_icm_merged2;
                                  create table ds_",agency,".",analyst,"_",DPID,"_icm_merged2 as
                                  select *, row_number() over(order by user_id) as row_rank 
                                  from(
                                  select user_id, m.time, channel, sub_channel, os_id,city_id,browser_id,
                                  eventind, record_month, record_week, record_year
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_merged as m
                                  WHERE user_id NOT IN (select user_id FROM ds_",agency,".",analyst,"_",DPID,"_icm_conv)
                                  UNION ALL
                                  select user_id, c.time, channel, sub_channel, os_id,city_id,browser_id,
                                  eventind, record_month, record_week, record_year
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_conv as c);"))
    
  #################################################################
  
  
  
  message(paste0("Clean NAs,  ",Sys.time()))
  #___Filter_na's______________________________________
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM ds_",agency,".",analyst,"_",DPID,"_na_filt
                                  WHERE user_id not in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv);"))
  
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM ds_",agency,".",analyst,"_",DPID,"_na_filt_lc
                                  WHERE user_id not in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv);"))
  
  
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM ds_",agency,".",analyst,"_",DPID,"_na_filt_browser
                                  WHERE user_id not in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv);"))
  ## have to do this again because when we remove post conversion events, we might have some users with no events left.
  
  #____Get_na's________________________________________
  na_Q <- dbSendQuery(redshift_con,paste0("
                                          select * from ds_",agency,".",analyst,"_",DPID,"_na_filt
                                          "))
  na <- fetch(na_Q, n = -1)
  
  
  #____Get_na's________________________________________
  na_browser_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ds_",agency,".",analyst,"_",DPID,"_na_filt_browser
                                                  "))
  na_browser <- fetch(na_browser_Q, n = -1)
  # this is used for outputs later on
  
  ##########
  message(paste0("Audience, LC,  ",Sys.time()))
  #___Audience__________________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_audience;
                                  create table ds_",agency,".",analyst,"_",DPID,"_audience as(
                                  select distinct user_id from ds_",agency,".",analyst,"_",DPID,"_icm_merged2);"))
  
  aud_length_Q <- dbSendQuery(redshift_con,paste0("
                                                  select count(*) from ds_",agency,".",analyst,"_",DPID,"_audience;"))
  aud_length <- as.numeric(fetch(aud_length_Q, n = -1))
  
  ### new last click - taken from na file
  
  #______MATCH LC ONTO HIERARCHY
  dbSendQuery(redshift_con,paste0("
                                  DROP table if exists ds_",agency,".",analyst,"_",DPID,"_LC_merged;
                                  CREATE table ds_",agency,".",analyst,"_",DPID,"_LC_merged as
                                  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
                                  record_month,record_week,record_year
                                  FROM ds_",agency,".",analyst,"_",DPID,"_na_filt_LC as lc
                                  LEFT JOIN ds_",agency,".",analyst,"_",DPID,"_category as cat
                                  ON lc.page_id = cat.page_id
                                  "))
  
  
  lc_na_Q <- dbSendQuery(redshift_con,paste0("
                                             select * from ds_",agency,".",analyst,"_",DPID,"_LC_merged"))
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
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1;
                                create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as users
                                from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2;
                                  create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr;
                                  create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1 as a
                                left join ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2 as b
                                on a.os=b.os
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))




#____ City_distribution
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1;
                                  create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1 as(
                                  select city_id as city,
                                  channel, sub_channel,  count(distinct user_id) as users
                                  from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                  group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2;
                                  create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2 as(
                                select city_id as city,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv
                                group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr;
                                  create table ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1 as a
                                left join ds_",agency,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2 as b
                                on a.city=b.city
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))

  city_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ds_",agency,".",analyst,"_",DPID,"_city_distr"))
  city_distr <- fetch(city_distr_Q, n = -1)
  
  os_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                select * from ds_",agency,".",analyst,"_",DPID,"_os_distr"))
  os_distr <- fetch(os_distr_Q, n = -1)
  
  #___Get match tables___
  {
    os_match_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from ds_",agency,".dc_match_operating_systems"))
    os_match <- fetch(os_match_Q, n = -1)
    
    city_match_Q <- dbSendQuery(redshift_con,paste0("
                                                    select * from ds_",agency,".dc_match_cities"))
    city_match <- fetch(city_match_Q, n = -1)
    
    city_distr$city_id <- city_distr$city
    city_distr <- city_distr[,-which(names(city_distr)=="city")]
    
    os_distr$os_id <- os_distr$os
    os_distr <- os_distr[,-which(names(os_distr)=="os")]
    
    city_distr <- merge(city_distr,city_match,all.x=T)
    os_distr <- merge(os_distr,os_match,all.x=T)
  }
  
  
  message(paste0("Event distr,  ",Sys.time()))
  #__Click_distribution____________
  click_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                   select sub_channel, count(*) as clicks
                                                   from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                   where eventind = 1
                                                   group by sub_channel;
                                                   "))
  click_distr <- fetch(click_distr_Q, n = -1)
  click_distr <- click_distr[order(click_distr$clicks, decreasing = T), ]
  #__Click_time_distribution______________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_click_time_distr;
                                  create table ds_",agency,".",analyst,"_",DPID,"_click_time_distr as(
                                  select m.time::date as click_dom, m.channel as click_chan, 
                                  m.sub_channel as click_sub,m.browser_id as click_browser, EXTRACT(HOUR FROM m.time) as click_hod,
                                  EXTRACT(DOW FROM m.time) as click_dow, count(*) as clicks,
                                  CASE WHEN user_id in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END as click_conv_path
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_merged2 as m
                                  where eventind = 1
                                  group by m.channel, m.sub_channel,m.browser_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))
  
  #__Imp_distribution____________
  imp_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, count(*) as imps
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 where eventind = 2
                                                 group by sub_channel;"))
  imp_distr <- fetch(imp_distr_Q, n = -1)
  imp_distr <- imp_distr[order(imp_distr$imps, decreasing = T),]
  #__Imp_time_distribution______________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_imp_time_distr;
                                  create table ds_",agency,".",analyst,"_",DPID,"_imp_time_distr as(
                                  select m.time::date as imp_dom, m.channel as imp_chan,
                                  m.sub_channel as imp_sub,m.browser_id as imp_browser, EXTRACT(HOUR FROM m.time) as imp_hod, 
                                  EXTRACT(DOW FROM m.time) as imp_dow, count(*) as impressions,
                                  CASE WHEN user_id in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END as imp_conv_path
                                  from ds_",agency,".",analyst,"_",DPID,"_icm_merged2 as m
                                  where eventind = 2
                                  group by m.channel, m.sub_channel,m.browser_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))
  
  #__Event_time_distribution___________INTERMEDIARY___
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_event_by_date;
                                  create table ds_",agency,".",analyst,"_",DPID,"_event_by_date as(
                                  select imp_chan, imp_sub, imp_browser, imp_dom, imp_hod, imp_dow, impressions,
                                  click_chan, click_sub, click_browser, click_dom, click_hod, click_dow, clicks, imp_conv_path, click_conv_path 
                                  from ds_",agency,".",analyst,"_",DPID,"_imp_time_distr as i
                                  FULL OUTER JOIN ds_",agency,".",analyst,"_",DPID,"_click_time_distr as c
                                  ON imp_sub = click_sub
                                  AND imp_chan = click_chan
                                  AND imp_hod = click_hod
                                  AND imp_dow = click_dow
                                  AND imp_dom = click_dom
                                  AND imp_conv_path = click_conv_path
                                  AND imp_browser = click_browser);
                                  "))
  
  #_Monday:1__Tuesday:2__...__Saturday:6__Sunday:0_______
  
  #____Events_per_date_dow_&_hod___________________________
  ## why not select *? this whole bit could probably be cleaner
  event_dow_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                       select imp_chan, imp_sub, imp_browser, imp_dom, imp_dow, imp_hod, sum(impressions) as impressions,
                                                       click_chan, click_sub, click_browser, click_dom, click_dow, click_hod, sum(clicks) as clicks, imp_conv_path, click_conv_path
                                                       from ds_",agency,".",analyst,"_",DPID,"_event_by_date
                                                       GROUP BY imp_chan, imp_sub, imp_browser, imp_dom, imp_dow, imp_hod,
                                                       click_chan, click_sub, click_browser, click_dom, click_dow, click_hod, imp_conv_path, click_conv_path"))
  event_dow_distr <- fetch(event_dow_distr_Q, n = -1)
  #rm(event_dow_distr_Q)
  
  event_dow_distr <- within(event_dow_distr, {
    # fill in the blanks in the corresponding columns
    imp_sub[which(is.na(imp_sub))]         <- click_sub[which(is.na(imp_sub))]   
    imp_chan[which(is.na(imp_chan))]       <- click_chan[which(is.na(imp_chan))] 
    imp_browser[which(is.na(imp_browser))] <- click_browser[which(is.na(imp_browser))]
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
    'click_dom',   'click_conv_path','click_browser'))]
  
  event_dow_distr$impressions <- na.zero(event_dow_distr$impressions)
  event_dow_distr$clicks      <- na.zero(event_dow_distr$clicks)
  
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_chan')]      <- 'channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_sub')]       <- 'sub_channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dow')]       <- 'day_of_week'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dom')]       <- 'date'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_hod')]       <- 'hour_of_day'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_conv_path')] <- 'conv_path'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_browser')] <- 'browser_id'
  
  event_dow_distr <- as.data.table(event_dow_distr)
  #setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path)
  setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path,browser_id)
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
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 group by sub_channel"))
  aud_distr_sub <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, count(distinct user_id) as audience 
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 group by channel"))
  aud_distr_chan <- fetch(aud_distr_Q, n = -1)
  
  #___aud_distribution___________combo______by_Month______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_month, count(distinct user_id) as audience
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 group by sub_channel, record_month, record_year"))
  aud_distr_sub_month <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_month, count(distinct user_id) as audience
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 group by channel, record_month, record_year"))
  aud_distr_chan_month <- fetch(aud_distr_Q, n = -1)
  
  
  #___aud_distribution___________combo______by_Week______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_week, count(distinct user_id) as audience
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
                                                 group by sub_channel, record_week, record_year"))
  aud_distr_sub_week <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_week, count(distinct user_id) as audience
                                                 from ds_",agency,".",analyst,"_",DPID,"_icm_merged2
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
  
# get the unique non converters 
dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_unq_non_cons;
                                create table ds_",agency,".",analyst,"_",DPID,"_unq_non_cons as(
                                select a.user_id, random() as rand
                                FROM ds_",agency,".",analyst,"_",DPID,"_icm_merged2 as a
                                LEFT JOIN ds_",agency,".",analyst,"_",DPID,"_icm_conv as b
                                ON a.user_id = b.user_id
                                where b.user_id is null
                                group by a.user_id);
                                "))

message(paste0("Sampling,  ", Sys.time()))
### need to run this in R
#____Converter___user_ids_____________________________________________
convers_Q <- dbSendQuery(redshift_con, paste0("
                                                select distinct user_id from ds_",agency,".",analyst,"_",DPID,"_icm_conv"))
convers <- unlist(fetch(convers_Q, n = -1))
names(convers) <- NULL

#____Count_of_unique_non-converters___________________________________
lenNonCons_Q <- dbSendQuery(redshift_con, paste0("
                                                   select count(*) from ds_",agency,".",analyst,"_",DPID,"_unq_non_cons"))
lenNonCons <- as.integer(fetch(lenNonCons_Q, n = -1))

#____R__sampling______________________________________________________
#sample_factor <- 20
if(!is.null(sample_seed)) {
  set.seed(sample_seed)  
} else {}

cutoff<-length(convers)*sample_factor/lenNonCons

#_____sampled_unique_non-converters___________________________________

dbSendQuery(redshift_con,paste0("
                                  drop table if exists ds_",agency,".",analyst,"_",DPID,"_icm_20;
                                  create table ds_",agency,".",analyst,"_",DPID,"_icm_20 as(
                                  select a.*
                                  FROM ds_",agency,".",analyst,"_",DPID,"_icm_merged2 as a
                                  inner JOIN ds_",agency,".",analyst,"_",DPID,"_unq_non_cons as b
                                  ON a.user_id = b.user_id
                                  where rand<", cutoff,
                                "union all
                                  select * 
                                  FROM ds_",agency,".",analyst,"_",DPID,"_icm_conv as b
                                  );
                                  "))

#______icM_20______________________________________________!___________
message(paste0("icM_20,  ",Sys.time()))
icM_20_Q <- dbSendQuery(redshift_con, paste0("
                                               select * from ds_",agency,".",analyst,"_",DPID,"_icm_20"))

icM_20 <- fetch(icM_20_Q, n = -1)


  
  #_____Tidy_up_________________________________________________________
    
  icM_dt <- as.data.table(icM_20)
  setkey(icM_dt,user_id,sub_channel,eventind)
  icM_dt <- icM_dt[,lapply(.SD,head,100),by=key(icM_dt)]

  icM_20_backup <- icM_20 <- as.data.frame(icM_dt)


  rm(aud_distr_Q, lenNonCons_Q, imp_distr_Q, icM_20_Q, convers_Q, click_distr_Q, nonConsSample20_Q, icM_conv_sec_filt_Q, na_Q, channel_lookup_Q)
  rm(sample_20)
  
  dups <- duplicated(icM_20)
  
  if(sum(dups) > 0 ){
    icM_20 <- icM_20[-which(dups), ] # slowish
  }
  NA_s <- is.na(icM_20$sub_channel)
  if(sum(NA_s) > 0 ){
    icM_20 <- icM_20[-which(NA_s), ]
  }
  
  #============LEW=LOOP=====START============
  message(paste0("transpose,  ",Sys.time()))
  icC <- icM_20[, c("user_id", "sub_channel", "eventind")] # quirk: column order matters
  icC$count <- rep(1, nrow(icC)) # this is probably unnecessary + badly coded
  
  icCdt <- as.data.table(icC)
  setkey(icCdt,user_id)
  icC <- data.frame(icCdt)
    
  
  totLen = nrow(icC)
  tempL = 100
  seed = 1
  reps = totLen %/% tempL
  transL = vector("list", length = reps)
  
  if(!parallel) {
    for(i in 0:(reps-1)){
      tmp = icC[(seed+i*tempL):(seed-1+(i+1)*tempL), ]
      transIC = suppressMessages( dcast(tmp, user_id ~ sub_channel + eventind, length) )
      transL[[i+1]] <- transIC
      if(i %% 10 == 0){print(i)}
    } 
    message('Append to the list the remainder of the lines')
    tmp = icC[(reps * tempL + 1):nrow(icC), ]
    transIC = suppressMessages(dcast(tmp, user_id ~ sub_channel + eventind, length))
    transL[[length(transL) + 1]] <- transIC
    
    rm(transIC, tmp)
  } else {
    ############
    message("Cluster")
    library(parallel)
    cl <- makeCluster(detectCores())
    clusterExport(cl,c("icC","seed","transL","tempL"), envir=environment())
    transL <- parLapply(cl,0:(reps-1),function(i) {
      require(reshape2)
      tmp = icC[(seed+i*tempL):(seed-1+(i+1)*tempL), ]
      transIC = suppressMessages( dcast(tmp, user_id ~ sub_channel + eventind, length) )
      transIC
    })
    stopCluster(cl)
    
    message('Append to the list the remainder of the lines')
    tmp = icC[(reps * tempL + 1):nrow(icC), ]
    transIC = suppressMessages(dcast(tmp, user_id ~ sub_channel + eventind, length))
    transL[[length(transL) + 1]] <- transIC
  }
  ############
  
  message(paste0("transp_combine,  ",Sys.time()))
  #=============LEW=LOOP===END==============
  timeStart <- Sys.time()
  # Loop-through merge
  is.even <- function(x){ x %% 2 == 0 } 
  
  expo <- floor(log(reps,2))-1
  
  supL = vector("list", length = expo+1)
  supL[[1]] = transL
  for(j in 1:expo){
    tempL1 <- vector("list", length = ceiling(length(supL[[j]])/2))
    if(is.even(length( supL[[j]] ))){
      for(i in seq(1, length(supL[[j]]), by = 2)){
        tempL2 <- supL[[j]][i:(i+1)]
        tempTrans <- Reduce(rbind.fill, tempL2)
        tempL1[[(i+1)/2]] <- tempTrans
      }
    } else{
      for(i in seq(1, length(supL[[j]])-1, by = 2)){
        tempL2 <- supL[[j]][i:(i+1)]
        tempTrans <- Reduce(rbind.fill, tempL2)
        tempL1[[(i+1)/2]] <- tempTrans
      }
      tempL1[[length(tempL1)]] <- supL[[j]][[length( supL[[j]] )]]
    }
    supL[[j+1]] <- tempL1
    print(j)
    gc()
  }
  trans1M_20 = Reduce(rbind.fill, supL[[ length(supL) ]]) 
  rm(supL, tempL1, tempTrans, tempL2, transL, tempL)
  
  
  #_Tidy_up____________________________________
  rm(timeStart, seed, is.even, expo,  i)
  
  message(paste0("export,  ",Sys.time()))
  #__export_________________________________________________________
  # visitors <- visitors$user_id
  
  save(aud_distr, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_aud_distr.RData")))
  save(click_distr, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_click_distr.RData")))
  save(imp_distr, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_imp_distr.RData")))
  save(convers, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_convers.RData")))
  save(icM_20, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_icM_20.RData")))
  save(lc, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_lc.RData")))
  save(lc_na, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_lc_na.RData")))
  save(icM_conv_sec_filt, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_icM_conv_sec_filt.RData")))
  save(imp_distr, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_imp_distr.RData")))
  save(trans1M_20, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_trans1M_20.RData")))
  save(na, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_na.RData")))
  save(na_browser, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_na_browser.RData")))
  save(chanLkup, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_chanLkup.RData")))
  save(aud_length, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_aud_length.RData")))
  
  save(event_dow_distr, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_event_dow_distr.RData")))
  save(events, file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_events.RData")))
  save(os_distr,file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_os_distr.RData")))
  save(city_distr,file = file.path(file_save_location,paste0("dc_",analyst,"_",DPID,"_city_distr.RData")))
  
  end.time <- Sys.time()
  function_summary <- cat(paste0("Red funtion Runtime Stats:"),
                             paste0("Start Time: ",start.time),
                             paste0("Finish Time: ",end.time),
                             paste0("Run Time: ",difftime(end.time,start.time)),sep="\n")
  return(function_summary)
}

