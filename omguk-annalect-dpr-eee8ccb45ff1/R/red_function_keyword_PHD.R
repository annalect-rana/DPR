#' Runs the initial red process

#'

#' Runs the initial red process

#'

#' @param username The username for redshift
#' @param pw The password for that username
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
#' @param parallel Defaults to TRUE, whether you want to run some processes in parallel on your machine. Set this to false if you want to leave some cores available for other processes. The parallel operations are not running throughout so should cause minimal disruption.
#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param hierarchy_location Where the hierarchy is saved. Defaults to safe gibberish.
#' @param ppc_hierarchy_location Where the ppc_hierarchy is saved. Defaults to safe gibberish.
#' @param ppc_match are we matching ppc separately?
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' @examples
#' 
#' #red_function_keyword_PHD(username="tim_scholtes",pw="",
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

red_function_keyword_PHD <- function(username,pw,DPID,country_code="be",
                         advertiserIds,campaignIds,activity_sub_type,
                         startDate,endDate,startPre,endPre, join_type="Left",
                         sample_factor=20,sample_seed=NULL,
                         parallel=T, 
                         file_save_location,hierarchy_location=NULL,
                         ppc_hierarchy_location=NULL,
                         ppc_match=FALSE,
                         ppc_match_col=c("ds_campaign_name",
                          "kw_ad_group_name"),
                         report_name) # new arg base on JF requirement to filter business and consumer-references a field in dc_match_activity_cat) 
{
  
  start.time <- Sys.time()
  reupload_hierarchy = TRUE
  
  if(missing(username)) {
    stop("argument username is missing")}
  if(missing(pw)) {
    stop("argument pw is missing")}
  if(missing(DPID)) {
    stop("argument DPID is missing")}
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
  if(missing(report_name)) {
    stop("argument report_name is missing")}
  options(warn=1)
  ppc_match_col <- match.arg(ppc_match_col)
  failure <- checks_function_keyword_PHD(startDate,endDate,
                                         startPre,endPre,
                                         reupload_hierarchy,
                                         ppc_match,
                                         hierarchy_location,
                                         ppc_hierarchy_location,
                                         ppc_match_col)
   if(!tolower(join_type)%in%c("left", "inner")){stop("Not a join type")}
  
message(paste0("Red process started at ",start.time))
  # DB control --------------------------------------------------------------
  dbname <- "lgiemea" 
  dbHost <- "54.154.112.90"
  port <-"5439"
  drv <- dbDriver("PostgreSQL")
  redshift_con <- dbConnect(drv, dbname = dbname, user = username, password = pw, 
                            host = dbHost, port = port)
  # parameter control -------------------------------------------------------
  

  # needs to be one date either side
  startDate <- as.Date(startDate)-1
  endDate   <- as.Date(endDate)+1
  startPre  <- as.Date(startPre)-1
  endPre    <- as.Date(endPre)+1
  
  analyst   <- username
  
  
advertiserIds     <- paste0(advertiserIds,collapse=", ")
campaignIds       <- paste0(campaignIds,collapse=", ")
activity_sub_type <- paste0(activity_sub_type, collapse = "','")
report_name       <-  paste0("'", paste(report_name, collapse="','"), "'") 
  

  # REDSHIFT BEGIN ----------------------------------------------------------

# load in analyst lookup table

analyst_lookup_Q <- dbSendQuery(redshift_con,paste0(
  "select * from dc_",country_code,".rs_users"))
analyst_lookup <- fetch(analyst_lookup_Q,n=-1)

analyst_code <- analyst_lookup$rs_user_id[match(analyst,analyst_lookup$user_name)]
date_code <- paste0(substr(year(Sys.Date()),3,4),
                    str_pad(month(Sys.Date()),width = 2,pad="0"),
                    str_pad(mday(Sys.Date()),width = 2,pad="0"))


existing_tables_Q <- dbSendQuery(redshift_con,
                                 paste0("
      select table_name from information_schema.tables
      where table_schema = 'dc_",country_code,"'
      AND table_name LIKE 'dp%';"))

existing_tables <- fetch(existing_tables_Q,n=-1)

DPID_save <- DPID
DPID <- tolower(gsub("_","",DPID))

tables_to_remove <- existing_tables[grep(paste0("_",DPID,"_"),
                                         existing_tables$table_name),1]
if(length(tables_to_remove)>0) {
  dbSendQuery(redshift_con,paste0("DROP TABLE IF EXISTS dc_",country_code,".",tables_to_remove,";",
                                  collapse=" \n"))
}
  # BASE TABLES
  #__na__Table________________________________ THIS QUERY HAS CHANGED TO INCORPRATE REPORT NAME FROM ACTIVITY_CAT TABLE SO THAT WE CAN DISTINGUISH BETWEEN 
  # BUS AND COSUMER CONVERSIONS
  message(paste0("NA Table, ",Sys.time()) ) 

    dbSendQuery(redshift_con,paste0("
    DROP TABLE IF EXISTS dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs;
    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as
    select s.user_id
    from dc_",country_code,".dc_na as s
    left join dc_",country_code,".dc_match_activity_cat as b
    on s.spot_id=b.spot_id and 
    s.activity_type=b.activity_type and 
    s.activity_sub_type=b.activity_sub_type
    where s.record_date > '",startPre,"'
    and s.record_date < '",endPre,"'
    and s.advertiser_id in (",advertiserIds,")
    and s.activity_sub_type in ('",activity_sub_type,"')
    and b.report_name in (",report_name,")
    "))

  dbSendQuery(redshift_con,paste0("
    DROP TABLE IF EXISTS dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na;
    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na as
    select s.user_id, min(s.time) as time, s.activity_sub_type, s.os_id, s.browser_id,
    s.page_id, s.buy_id, s.site_id, s.advertiser_id, s.record_month,
    s.record_week, s.record_year, s.keyword_id, b.report_name,s.revenue as initial_revenue
    from dc_",country_code,".dc_na as s
    left join dc_",country_code,".dc_match_activity_cat as b
    on s.spot_id=b.spot_id and 
    s.activity_type=b.activity_type and 
    s.activity_sub_type=b.activity_sub_type
    where s.record_date > '",startDate,"'
    and s.record_date < '",endDate,"'
    and s.advertiser_id in (",advertiserIds,")
    and s.activity_sub_type in ('",activity_sub_type,"')
    and b.report_name in (",report_name,")
    and s.user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e)
group by user_id,  s.activity_sub_type,b.report_name, os_id,browser_id, keyword_id,
                                  page_id, buy_id, site_id, advertiser_id, record_month, record_week, record_year,revenue
    "))

#### calc for the revenue across all conversion types now:

  dbSendQuery(redshift_con,paste0("
    DROP TABLE IF EXISTS dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_allrevenue;
    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_allrevenue as
    select s.user_id, sum(s.revenue) as all_revenue
    from dc_",country_code,".dc_na as s
    left join dc_",country_code,".dc_match_activity_cat as b
    on s.spot_id=b.spot_id and 
    s.activity_type=b.activity_type and 
    s.activity_sub_type=b.activity_sub_type
    where s.record_date > '",startDate,"'
    and s.record_date < '",endDate,"'
    and s.advertiser_id in (",advertiserIds,")
    and s.activity_sub_type in ('",activity_sub_type,"')
    and b.report_name in (",report_name,")
    and s.user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e)
group by user_id
    "))


  # workaround for crazy union all extra records thing
  dbSendQuery(redshift_con,paste0("DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na
                                  WHERE buy_id IN (5,25,26,27,28,29,30,31);"))
  
  #-- Click table______________________________
  message(paste0("Click Table, ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
    DROP TABLE if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_c;
    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_c as(
    SELECT user_id, c1.time, os_id, city_id,page_id, buy_id, site_id,browser_id,
    c1.advertiser_id, record_month, record_week, record_year, keyword_id, cat.report_name
    FROM dc_",country_code,".dc_c as c1
    join dc_",country_code,".dc_match_advertiser as ad
    on c1.advertiser_id=ad.advertiser_id
    join dc_",country_code,".dc_match_activity_cat as cat
    on ad.spot_id=cat.spot_id
    where c1.record_date > '",startDate,"'
    and c1.record_date < '",endDate,"'
    and c1.advertiser_id in (",advertiserIds,")
    and buy_id in (",campaignIds,")
    and cat.report_name in (",report_name,")
    and user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e)
    UNION ALL
    SELECT c.user_id, c.time, c.os_id,c.city_id, c.page_id, c.buy_id,c.site_id,c.browser_id,
     c.advertiser_id, c.record_month, c.record_week, c.record_year,c.keyword_id,  cat.report_name
    FROM dc_",country_code,".dc_c as c
    join dc_",country_code,".dc_match_advertiser as ad
    on c.advertiser_id=ad.advertiser_id
    join dc_",country_code,".dc_match_activity_cat as cat
    on ad.spot_id=cat.spot_id
    where c.record_date > '",startPre,"'
    and c.record_date < '",endPre,"'
    and c.advertiser_id in (",advertiserIds,")
    and c.buy_id in (",campaignIds,")
    and c.user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e));
    "))
# workaround for crazy union all extra records thing
  dbSendQuery(redshift_con,paste0("DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_c
                                  WHERE buy_id IN (5,25,26,27,28,29,30,31);"))
  
  #-- Imps table_______________________________
  message(paste0("Imp Table,  ", Sys.time()))
  dbSendQuery(redshift_con,paste0("
    DROP TABLE if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_i;
    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_i as(
    SELECT user_id, i1.time, city_id, os_id, page_id, buy_id,browser_id,
    site_id, i1.advertiser_id, record_month, record_week, record_year, cat.report_name
    FROM dc_",country_code,".dc_i as i1
    join dc_",country_code,".dc_match_advertiser as ad
    on i1.advertiser_id=ad.advertiser_id
    join dc_",country_code,".dc_match_activity_cat as cat
    on ad.spot_id=cat.spot_id
    where i1.record_date > '",startDate,"'
    and i1.record_date < '",endDate,"'
    and i1.advertiser_id in (",advertiserIds,")
    and buy_id in (",campaignIds,")
    and cat.report_name in (",report_name,")
    and user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e)
    UNION ALL
    SELECT i.user_id, i.time, i.city_id, i.os_id, i.page_id, i.buy_id, i.browser_id,
    i.site_id, i.advertiser_id, i.record_month, i.record_week, i.record_year, cat.report_name
    FROM dc_",country_code,".dc_i as i
    join dc_",country_code,".dc_match_advertiser as ad
    on i.advertiser_id=ad.advertiser_id
    join dc_",country_code,".dc_match_activity_cat as cat
    on ad.spot_id=cat.spot_id
    where i.record_date > '",startPre,"'
    and i.record_date < '",endPre,"'
    and i.advertiser_id in (",advertiserIds,")
    and i.buy_id in (",campaignIds,")
    and i.user_id not in (
    select e.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_pre_na_UIDs as e));
    "))

  # workaround for crazy union all extra records thing
  dbSendQuery(redshift_con,paste0("DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_i
                                  WHERE buy_id IN (5,25,26,27,28,29,30,31);"))
  
  
  
  #_______icM________________
  message(paste0("icM,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm;
  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm as
  SELECT user_id, c.time, os_id, city_id,page_id, buy_id, site_id, browser_id,
  advertiser_id, record_month, record_week, record_year, keyword_id, report_name, 1 as eventind
  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_c as c
  where user_id != 0
  UNION ALL
  SELECT user_id, i.time, os_id,city_id, page_id, buy_id, site_id, browser_id,
  advertiser_id, record_month, record_week, record_year, '0' as keyword_id , report_name,  2 as eventind
  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_i as i
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
                                    drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category;
                                    create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category (
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
                                        INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category (page_id
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
        INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category (page_id
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
                                    INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category (page_id
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
  
if(ppc_match) {
  message(paste0("Upload PPC Hierarchy,  ", Sys.time()))
    hierarchy <- read.csv(ppc_hierarchy_location)

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
    
    ## dedupe the hierarchy at the ppc_match_col level
    hierarchy <- hierarchy[,c(ppc_match_col,"sub_channel","channel")]
    hierarchy <- unique(hierarchy)


    dbSendQuery(redshift_con,
      paste0("
        drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs;
        create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs (
        ppc_match_col varchar(1000),
        sub_channel varchar(500),
        channel varchar(500));
        "))

  N <- nrow(hierarchy)
  n <- 1000
  reps <- N%/%n
  if(reps>=1) {
    #____insert into category table___ to be sped up by inserting multiple lines at once
    for(i in 1:reps) {
      print(paste0("Inserting hierarchy row block",i*n))

      dbSendQuery(redshift_con,
        paste0(" 
          INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs (
          ppc_match_col
          , sub_channel
          , channel
          ) VALUES (",
    do.call(paste0,list(lapply((i-1)*n+1:n,function(k) {
      paste0("'",hierarchy[k,ppc_match_col], 
             "', '",
             hierarchy[k,c("sub_channel")],
             "', '",
             hierarchy[k,c("channel")], 
             "'"
      )}),collapse=" ), (")),
    ");"))
  }
  
  
  dbSendQuery(redshift_con,paste0(" 
         INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs (
        ppc_match_col
        , sub_channel
        , channel
        ) VALUES (",
  do.call(paste0,list(lapply((reps*n+1):N,function(k) {
    paste0("'",hierarchy[k,ppc_match_col], 
             "', '",
             hierarchy[k,c("sub_channel")],
             "', '",
             hierarchy[k,c("channel")], 
             "'"
      )}),collapse=" ), (")),
  ");"))

  
} else {
  
  dbSendQuery(redshift_con,paste0(" 
        INSERT INTO dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs (
        ppc_match_col
        , sub_channel
        , channel
        ) VALUES (",
      do.call(paste0,list(lapply(1:N,function(k) {
        paste0("'",hierarchy[k,ppc_match_col], 
             "', '",
             hierarchy[k,c("sub_channel")],
             "', '",
             hierarchy[k,c("channel")], 
             "'"
      )}),collapse=" ), (")),
      ");"))
  
}}}
  
  #______MATCH ONTO HIERARCHY
  if(!ppc_match) {
  message(paste0("Match Hierarchy,  ", Sys.time()))
  dbSendQuery(redshift_con,paste0("
  DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged;
  CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged as
  select *, row_number() over(partition by user_id,sub_channel,time order by user_id) as row_rank 
  from(
  SELECT user_id, ic.time, coalesce(channel,'Other') as channel,
  coalesce(sub_channel,'Other') as sub_channel, os_id,city_id,browser_id,
  eventind, record_month, record_week, record_year
  FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm as ic
  ",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category as cat
  ON ic.page_id = cat.page_id)
  "))
} else {

dbSendQuery(redshift_con,paste0("
    DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_ppc;
    CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_ppc as
    SELECT ic.user_id, ic.time, coalesce(cat.channel,'Other') as channel,
    coalesce(cat.sub_channel,'Other') as sub_channel, ic.os_id,ic.city_id,ic.browser_id,
    ic.eventind, ic.record_month, ic.record_week, ic.record_year
    FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm as ic
    join dc_",country_code,".dc_match_dartsearch as d
    on ic.keyword_id=d.keyword_id
    ",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs as cat
    ON d.",ppc_match_col,"=cat.ppc_match_col
    where ic.keyword_id <> 0
    "))

# non-ppc clicks
dbSendQuery(redshift_con,paste0("
DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_nonppc;
CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_nonppc as
SELECT user_id, ic.time, coalesce(channel,'Other') as channel,
coalesce(sub_channel,'Other') as sub_channel, os_id,city_id,browser_id,
eventind, record_month, record_week, record_year
FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm as ic
",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category as cat
ON ic.page_id = cat.page_id
where ic.keyword_id in (0,'')
"))

dbSendQuery(redshift_con,paste0("
DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged;
CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged as
select *,
row_number() over(partition by user_id,sub_channel,time order by user_id) as row_rank
from 
(select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_ppc
union all
select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged_nonppc) 
"))
  }

  #### remove duplicate events
  dbSendQuery(redshift_con,paste0("DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged
                                  WHERE row_rank >1
                                  "))

  
  #___Channel_lookup_______________________________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_lookup;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_lookup as
                                  select channel, sub_channel
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged
                                  group by channel, sub_channel;
                                  "))
  
  channel_lookup_Q <- dbSendQuery(redshift_con,paste0("
                                                      select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_lookup
                                                      "))
  chanLkup <- as.data.frame(fetch(channel_lookup_Q, n = -1))
  
  #___Filtered_conversions_______________________________
  message(paste0("Filter conversions,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt;
                                  CREATE TABLE dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt AS(
                                  SELECT s.user_id, min(s.time) as time
                                  FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na AS s
                                  INNER JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm AS u
                                  ON s.user_id = u.user_id
                                  GROUP BY s.user_id)")) 
  # users outside intersection na and icm tables filtered from na
  # to be discussed as to whether zero interaction UIDs should be kept.
  
  
  #___Filtered conversions for LC
  message(paste0("LC conversions,  ", Sys.time()))
  dbSendQuery(redshift_con,paste0(" 
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_LC; 
                                  CREATE TABLE dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_LC AS( 
                                  SELECT s.user_id, s.time,
                                  u.page_id,u.buy_id,u.site_id,u.keyword_id,u.record_month,u.record_week,u.record_year 
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt 
                                  AS s 
                                  LEFT JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na AS u 
                                  ON s.user_id = u.user_id 
                                  AND s.time = u.time)"))
  
  
  
  #___Filtered_conversions by os - used for one of the outputs__________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_os;
                                  CREATE TABLE dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_os as
                                  SELECT s.user_id, s.time, u.os_id
                                  FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt AS s
                                  left JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na AS u
                                  ON s.user_id = u.user_id and s.time = u.time")) 
  
  #_____Converters_with_time_differences____________DONE___
  message(paste0("icm_conv,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv as(
                                  select m.*, i.initial_revenue, j.all_revenue
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged as m
                                  INNER JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt as s
                                  on m.user_id = s.user_id
                                  
                                  LEFT JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na as i
                                  on m.user_id = i.user_id
                                  LEFT JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_allrevenue as j
                                  on m.user_id = j.user_id
                                  where (extract(epoch from s.time) - extract(epoch from m.time)) > 0
                                  );")) # icm for converters only with post-conversion events filtered out.
  
  icM_conv_sec_filt_Q <- dbSendQuery(redshift_con,paste0("
                                                         select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv
                                                         "))
  icM_conv_sec_filt <- fetch(icM_conv_sec_filt_Q, n = -1)
  
  # This should be the final flat table for all subsequent calculations:
  message(paste0("icm_merged,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2 as
                                  select *, row_number() over(order by user_id) as row_rank 
                                  from(
                                  select user_id, m.time, channel, sub_channel, os_id,city_id,browser_id,
                                  eventind, record_month, record_week, record_year
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged as m
                                  WHERE user_id NOT IN (select user_id FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv)
                                  UNION ALL
                                  select user_id, c.time, channel, sub_channel, os_id,city_id,browser_id,
                                  eventind, record_month, record_week, record_year
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv as c);"))
    
  #################################################################
  
  
  
  message(paste0("Clean NAs,  ",Sys.time()))
  #___Filter_na's______________________________________
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt
                                  WHERE user_id not in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv);"))
  
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_lc
                                  WHERE user_id not in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv);"))
  
  
  dbSendQuery(redshift_con,paste0("
                                  DELETE FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_os
                                  WHERE user_id not in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv);"))
  ## have to do this again because when we remove post conversion events, we might have some users with no events left.
  
  #____Get_na's________________________________________
  na_Q <- dbSendQuery(redshift_con,paste0("
                                          select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt
                                          "))
  na <- fetch(na_Q, n = -1)
  
  
  #____Get_na's________________________________________
  na_os_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_os
                                                  "))
  na_os <- fetch(na_os_Q, n = -1)
  # this is used for outputs later on
  
  ##########
  message(paste0("Audience, LC,  ",Sys.time()))
  #___Audience__________________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_audience;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_audience as(
                                  select distinct user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2);"))
  
  aud_length_Q <- dbSendQuery(redshift_con,paste0("
                                                  select count(*) from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_audience;"))
  aud_length <- as.numeric(fetch(aud_length_Q, n = -1))
  

### here get uniques. Moved from blue function

# make table of counts by channel and sub channel
dbSendQuery(redshift_con,paste0("
                                drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_count;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_count as(
                                select user_id, count(distinct channel) from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                group by user_id);
                                "))
dbSendQuery(redshift_con,paste0("
                                drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_sub_channel_count;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_sub_channel_count as(
                                select user_id, count(distinct sub_channel) from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                group by user_id);
                                "))

# make icm table by channel and sub channel where users have only 1 count in the above tables
dbSendQuery(redshift_con,paste0("
                                drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_sub;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_sub as(
                                select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                where user_id in (select a.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_sub_channel_count as a
                                where count = 1));
                                "))
dbSendQuery(redshift_con,paste0("
                                drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_chan;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_chan as(
                                select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                where user_id in (select a.user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_channel_count as a
                                where count = 1));
                                "))

uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select sub_channel, channel, count(distinct user_id) as uniques
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_sub
                                               group by sub_channel, channel"))
uni_distr_sub <- fetch(uni_distr_Q, n = -1)

uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select channel, count(distinct user_id) as uniques 
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_chan
                                               group by channel"))
uni_distr_chan <- fetch(uni_distr_Q, n = -1)

#___uni_distribution___________combo______by_Month______
uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select sub_channel,channel, record_year,
                                               record_month, count(distinct user_id) as uniques
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_sub
                                               group by sub_channel, channel, record_month, record_year"))
uni_distr_sub_month <- fetch(uni_distr_Q, n = -1)

uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select channel, record_year, record_month, count(distinct user_id) as uniques
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_chan
                                               group by channel, record_month, record_year"))
uni_distr_chan_month <- fetch(uni_distr_Q, n = -1)


#___uni_distribution___________combo______by_Week______
uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select sub_channel,channel, record_year,
                                               record_week, count(distinct user_id) as uniques
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_sub
                                               group by sub_channel, channel, record_week, record_year"))
uni_distr_sub_week <- fetch(uni_distr_Q, n = -1)

uni_distr_Q <- dbSendQuery(redshift_con,paste0("
                                               select channel, record_year, record_week, count(distinct user_id) as uniques
                                               from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_unique_chan
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


  ### new last click - taken from na file
  
  #______MATCH LC ONTO HIERARCHY
  if(!ppc_match) {
    dbSendQuery(redshift_con,paste0("
      DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged;
      CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged as
      SELECT user_id, lc.time, coalesce(channel,'Other') as channel,
       coalesce(sub_channel,'Other') as sub_channel,
      record_month,record_week,record_year
      FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_LC as lc
      ",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category as cat
      ON lc.page_id = cat.page_id
      "))   
  } else {
    
dbSendQuery(redshift_con,paste0("
      DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_ppc;
      CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_ppc as
      SELECT user_id, lc.time, coalesce(cat.channel,'Other') as channel,
       coalesce(cat.sub_channel,'Other') as sub_channel,
      record_month,record_week,record_year
      FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_LC as lc
      left join dc_",country_code,".dc_match_dartsearch as d
      on lc.keyword_id=d.keyword_id
      ",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_ppc_categs as cat
      ON d.",ppc_match_col,"=cat.ppc_match_col
      where lc.keyword_id <> 0
      and lc.keyword_id <> ''
      "))
    
    # non-ppc
    dbSendQuery(redshift_con,paste0("
      DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_nonppc;
      CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_nonppc as
      SELECT user_id, lc.time, coalesce(channel,'Other') as channel,
       coalesce(sub_channel,'Other') as sub_channel,
      record_month,record_week,record_year
      FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_na_filt_LC as lc
      ",toupper(join_type)," JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_category as cat
      ON lc.page_id = cat.page_id
      where lc.keyword_id in (0,'')
      "))

    # concatenate
    dbSendQuery(redshift_con,paste0("
      DROP table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged;
      CREATE table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged as
      select *, row_number() over(order by user_id) as row_rank 
      from 
      (select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_ppc
      union all
      select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged_nonppc) 
      "))
  }
  
  
  lc_na_Q <- dbSendQuery(redshift_con,paste0("
                                             select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_LC_merged"))
  lc_na <- fetch(lc_na_Q,n=-1)
  
  lc_na$lc <- 1
  lc_na$record_month <- paste0(lc_na$record_year,
    str_pad(lc_na$record_month,width=2,pad="0"))
  lc_na$record_week  <- paste0(lc_na$record_year,
    str_pad(lc_na$record_week,width=2,pad="0"))
  
  lc <- list(
    weekly_sub   = aggregate(lc~channel+sub_channel+record_week,data=lc_na,FUN=length),
    weekly_chan  = aggregate(lc~channel+record_week,data=lc_na,FUN=length),
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
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as users
                                from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2 as(
                                select os_id as OS,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv
                                group by os, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr1 as a
                                left join dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr2 as b
                                on a.os=b.os
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))




#____ City_distribution
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1 as(
                                  select city_id as city,
                                  channel, sub_channel,  count(distinct user_id) as users
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                  group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2 as(
                                select city_id as city,
                                channel, sub_channel,  count(distinct user_id) as conv
                                from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv
                                group by city, channel, sub_channel)"))
dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr as(
                                select a.*,coalesce(b.conv,0) as conv
                                from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr1 as a
                                left join dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr2 as b
                                on a.city=b.city
                                and a.channel=b.channel
                                and a.sub_channel=b.sub_channel)"))

  city_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_city_distr"))
  city_distr <- fetch(city_distr_Q, n = -1)
  
  os_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_os_distr"))
  os_distr <- fetch(os_distr_Q, n = -1)
  
  #___Get match tables___
  {
    os_match_Q <- dbSendQuery(redshift_con,paste0("
                                                  select * from dc_",country_code,".dc_match_operating_systems"))
    os_match <- fetch(os_match_Q, n = -1)
    
    city_match_Q <- dbSendQuery(redshift_con,paste0("
                                                    select * from dc_",country_code,".dc_match_cities"))
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
                                                   from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                   where eventind = 1
                                                   group by sub_channel;
                                                   "))
  click_distr <- fetch(click_distr_Q, n = -1)
  click_distr <- click_distr[order(click_distr$clicks, decreasing = T), ]
  #__Click_time_distribution______________
message(paste0("Event distr2,  ",Sys.time()))
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_click_time_distr;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_click_time_distr as(
                                  select m.time::date as click_dom, m.channel as click_chan, 
                                  m.sub_channel as click_sub,m.os_id as click_os, EXTRACT(HOUR FROM m.time) as click_hod,
                                  EXTRACT(DOW FROM m.time) as click_dow, count(*) as clicks,
                                  CASE WHEN user_id in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END as click_conv_path
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2 as m
                                  where eventind = 1
                                  group by m.channel, m.sub_channel,m.os_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))
message(paste0("Event distr3,  ",Sys.time()))
  #__Imp_distribution____________
  imp_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, count(*) as imps
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 where eventind = 2
                                                 group by sub_channel;"))
  imp_distr <- fetch(imp_distr_Q, n = -1)
  imp_distr <- imp_distr[order(imp_distr$imps, decreasing = T),]
  #__Imp_time_distribution______________
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_imp_time_distr;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_imp_time_distr as(
                                  select m.time::date as imp_dom, m.channel as imp_chan,
                                  m.sub_channel as imp_sub,m.os_id as imp_os, EXTRACT(HOUR FROM m.time) as imp_hod, 
                                  EXTRACT(DOW FROM m.time) as imp_dow, count(*) as impressions,
                                  CASE WHEN user_id in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END as imp_conv_path
                                  from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2 as m
                                  where eventind = 2
                                  group by m.channel, m.sub_channel,m.os_id, EXTRACT(HOUR FROM m.time), EXTRACT(DOW FROM m.time), m.time::date,
                                  CASE WHEN user_id in (select user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv) THEN 'C' ELSE 'NC' END);
                                  "))
message(paste0("Event distr4,  ",Sys.time()))
  #__Event_time_distribution___________INTERMEDIARY___
  dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_event_by_date;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_event_by_date as(
                                  select imp_chan, imp_sub, imp_os, imp_dom, imp_hod, imp_dow, impressions, click_chan, click_sub, click_os, click_dom, click_hod, click_dow, clicks, imp_conv_path, click_conv_path 
                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_imp_time_distr as i
                                  FULL OUTER JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_click_time_distr as c
                                  ON imp_sub = click_sub
                                  AND imp_chan = click_chan
                                  AND imp_hod = click_hod
                                  AND imp_dow = click_dow
                                  AND imp_dom = click_dom
                                  AND imp_conv_path = click_conv_path
                                  AND imp_os = click_os);
                                  "))
message(paste0("Event distr5,  ",Sys.time()))
  #_Monday:1__Tuesday:2__...__Saturday:6__Sunday:0_______
  
  #____Events_per_date_dow_&_hod___________________________
  event_dow_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                       select imp_chan, imp_sub, imp_os, imp_dom, imp_dow, imp_hod, sum(impressions) as impressions,
                                                       click_chan, click_sub, click_os, click_dom, click_dow, click_hod, sum(clicks) as clicks, imp_conv_path, click_conv_path
                                                       from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_event_by_date
                                                       GROUP BY imp_chan, imp_sub, imp_os, imp_dom, imp_dow, imp_hod, click_chan, click_sub, click_os, click_dom, click_dow, click_hod, imp_conv_path, click_conv_path"))
  event_dow_distr <- fetch(event_dow_distr_Q, n = -1)
  #rm(event_dow_distr_Q)
message(paste0("Event distr6,  ",Sys.time()))
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
message(paste0("Event distr7,  ",Sys.time()))
  #### reread this to make sure - remove which for speed
  # clean up - remove unnecessary columns
  event_dow_distr <- event_dow_distr[, -which(names(event_dow_distr) %in% c(
    'click_chan',   'click_sub', 
    'click_dow',   'click_hod', 
    'click_dom',   'click_conv_path','click_os'))]
  
  event_dow_distr$impressions <- na.zero(event_dow_distr$impressions)
  event_dow_distr$clicks      <- na.zero(event_dow_distr$clicks)
message(paste0("Event distr8,  ",Sys.time()))
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_chan')]      <- 'channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_sub')]       <- 'sub_channel'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dow')]       <- 'day_of_week'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_dom')]       <- 'date'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_hod')]       <- 'hour_of_day'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_conv_path')] <- 'conv_path'
  names(event_dow_distr)[which(names(event_dow_distr) == 'imp_os')] <- 'os_id'
message(paste0("Event distr9,  ",Sys.time()))
  event_dow_distr <- as.data.table(event_dow_distr)
  #setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path)
  setkey(event_dow_distr,channel,sub_channel,date,day_of_week,hour_of_day,conv_path,os_id)
  event_dow_distr <- as.data.frame(event_dow_distr[,list(impressions=sum(impressions),
                                                         clicks=sum(clicks)),
                                                   by=key(event_dow_distr)])
message(paste0("Event distr10,  ",Sys.time()))
  
  # now for clicks/imps from event_dow
  event_dow_distr$record_week <- paste0(year(event_dow_distr$date),
                                        substr(ISOweek(event_dow_distr$date),7,8))
  event_dow_distr$record_month <- paste0(year(event_dow_distr$date),
                                         str_pad(month(event_dow_distr$date),2,pad="0"))
message(paste0("Event distr11,  ",Sys.time()))
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
  
message(paste0("Event distr12,  ",Sys.time()))
  events$total_sub$level    <- "1_s"
  events$total_chan$level   <- "1_c"
  events$monthly_sub$level  <- "2_s"
  events$monthly_chan$level <- "2_c"
  events$weekly_sub$level   <- "3_s"
  events$weekly_chan$level  <- "3_c"
  
message(paste0("Event distr13,  ",Sys.time()))
  events <- do.call(rbind.fill,events)
  events  <- events[,c("channel","sub_channel","record_month",
                       "record_week","clicks","impressions","level")]
  
  
  message(paste0("Aud Distr,  ",Sys.time()))
  #___aud_distribution___________combo____________ = double check the year is required for all these
  aud_distr_Q<- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, count(distinct user_id) as audience
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 group by sub_channel"))
  aud_distr_sub <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, count(distinct user_id) as audience 
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 group by channel"))
  aud_distr_chan <- fetch(aud_distr_Q, n = -1)
  
  #___aud_distribution___________combo______by_Month______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_month, count(distinct user_id) as audience
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 group by sub_channel, record_month, record_year"))
  aud_distr_sub_month <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_month, count(distinct user_id) as audience
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 group by channel, record_month, record_year"))
  aud_distr_chan_month <- fetch(aud_distr_Q, n = -1)
  
  
  #___aud_distribution___________combo______by_Week______
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select sub_channel, record_year,
                                                 record_week, count(distinct user_id) as audience
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
                                                 group by sub_channel, record_week, record_year"))
  aud_distr_sub_week <- fetch(aud_distr_Q, n = -1)
  
  aud_distr_Q <- dbSendQuery(redshift_con,paste0("
                                                 select channel, record_year, record_week, count(distinct user_id) as audience
                                                 from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2
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
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_unq_non_cons;
                                create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_unq_non_cons as(
                                select a.user_id, random() as rand
                                FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2 as a
                                LEFT JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv as b
                                ON a.user_id = b.user_id
                                where b.user_id is null
                                group by a.user_id);
                                "))

message(paste0("Sampling,  ", Sys.time()))
### need to run this in R
#____Converter___user_ids_____________________________________________
convers_Q <- dbSendQuery(redshift_con, paste0("
                                                select distinct user_id from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv"))
convers <- unlist(fetch(convers_Q, n = -1))
names(convers) <- NULL

#____Count_of_unique_non-converters___________________________________
lenNonCons_Q <- dbSendQuery(redshift_con, paste0("
                                                   select count(*) from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_unq_non_cons"))
lenNonCons <- as.integer(fetch(lenNonCons_Q, n = -1))

#____R__sampling______________________________________________________
#sample_factor <- 20
if(!is.null(sample_seed)) {
  set.seed(sample_seed)  
} else {}

cutoff<-length(convers)*sample_factor/lenNonCons

#_____sampled_unique_non-converters___________________________________

dbSendQuery(redshift_con,paste0("
                                  drop table if exists dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_20;
                                  create table dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_20 as(
                                  select a.*
                                  FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_merged2 as a
                                  inner JOIN dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_unq_non_cons as b
                                  ON a.user_id = b.user_id
                                  where rand<", cutoff,
                                "union all
                                  select user_id,b.time,channel,sub_channel,os_id,city_id,browser_id,eventind,record_month,record_week,
                                  record_year,row_rank
                                  FROM dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_conv as b
                                  );
                                  "))

#______icM_20______________________________________________!___________
message(paste0("icM_20,  ",Sys.time()))
icM_20_Q <- dbSendQuery(redshift_con, paste0("
                                               select * from dc_",country_code,".","DP",date_code,"_",analyst_code,"_",DPID,"_icm_20"))

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
  icC$count <- rep(1, nrow(icC)) # this is probably unnecessary
  
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
    } # 10.5 mins
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
  trans1M_20 = Reduce(rbind.fill, supL[[ length(supL) ]]) # time: 167.33
rm(supL, tempL1, tempTrans, tempL2, transL, tempL)

#_Tidy_up____________________________________
rm(timeStart, seed, is.even, expo,  i)

message(paste0("export,  ",Sys.time()))
#__export_________________________________________________________
# visitors <- visitors$user_id

save(aud_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_aud_distr.RData")))
save(uni_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_uni_distr.RData")))
save(click_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_click_distr.RData")))
save(imp_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_imp_distr.RData")))
save(convers, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_convers.RData")))
save(icM_20, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_icM_20.RData")))
save(lc, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_lc.RData")))
save(lc_na, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_lc_na.RData")))
save(icM_conv_sec_filt, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_icM_conv_sec_filt.RData")))
save(imp_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_imp_distr.RData")))
save(trans1M_20, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_trans1M_20.RData")))
save(na, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_na.RData")))
save(na_os, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_na_os.RData")))
save(chanLkup, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_chanLkup.RData")))
save(aud_length, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_aud_length.RData")))

save(event_dow_distr, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_event_dow_distr.RData")))
save(events, file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_events.RData")))
save(os_distr,file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_os_distr.RData")))
save(city_distr,file = file.path(file_save_location,paste0(analyst,"_",DPID_save,"_city_distr.RData")))

end.time <- Sys.time()
function_summary <- cat(paste0("Red funtion Runtime Stats:"),
                           paste0("Start Time: ",start.time),
                           paste0("Finish Time: ",end.time),
                           paste0("Run Time: ",difftime(end.time,start.time)),sep="\n")
return(function_summary)
}
