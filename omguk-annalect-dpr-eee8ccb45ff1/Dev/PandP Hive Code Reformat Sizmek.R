


###########################################
######### Sizmek Data Prep ################
###########################################


hiveSizmek<-"



DROP TABLE IF EXISTS dc_i;
CREATE EXTERNAL TABLE dc_i
(
  user_id	STRING,
  time	STRING,
  city_id	BIGINT,
  os_id	BIGINT,  
  page_id	BIGINT,  
  buy_id	BIGINT,
  browser_id	BIGINT,
  site_id	BIGINT,  
  advertiser_id	BIGINT,
  record_date STRING,
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '/tmp/data/dc_i/'
  ;
  
  INSERT OVERWRITE TABLE dc_i
  
  SELECT 
  user_id, 
  EventDate as time, 
  city_id, 
  os_id, 
  placementid as page_id, 
  campaignid as buy_id, 
  browser_id,
  site_id, 
  advertiser_id, 
  record_date,
  record_month, 
  record_week, 
  record_year
  FROM sz_standard
  where eventtypeid=1
  
  ;
  
  
  
  
  
  
  DROP TABLE IF EXISTS dc_c;
  CREATE EXTERNAL TABLE dc_c
  (
  user_id STRING,
  time STRING,
  os_id BIGINT,  
  city_id BIGINT,
  page_id BIGINT, 
  buy_id BIGINT,  
  site_id BIGINT, 
  browser_id BIGINT,
  keyword_id STRING,
  advertiser_id BIGINT, 
  record_date STRING,
  record_month BIGINT,  
  record_week BIGINT,
  record_year BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '/tmp/data/dc_c/'
  ;
  
 INSERT OVERWRITE TABLE dc_c
  
  select * from (
  
  SELECT 
  user_id, 
  EventDate as time, 
  os_id,
  city_id, 
  placementid as page_id, 
  campaignid as buy_id, 
  site_id, 
  browser_id,
  0 as keyword_id,
  advertiser_id, 
  record_date,
  record_month, 
  record_week, 
  record_year
  FROM sz_standard
  where eventtypeid=2
  
  union all
  
  SELECT 
  user_id, 
  EventDate as time, 
  os_id,
  city_id, 
  placementid as page_id, 
  campaignid as buy_id, 
  site_id, 
  browser_id,
  keyword_id,
  advertiser_id, 
  record_date,
  record_month, 
  record_week, 
  record_year
  FROM sz_standard
  where eventtypeid=3
  
  union all
  
  SELECT 
  a.user_id, 
  a.EventDate as time, 
  a.os_id,
  a.city_id, 
  a.placementid as page_id, 
  b.campaign_id as buy_id, 
  a.site_id, 
  a.browser_id,
  b.keyword_id as keyword_id,
  a.advertiser_id, 
  a.record_date,
  a.record_month, 
  a.record_week, 
  a.record_year
  FROM sz_standard a
  left join sz_sem b
  on a.seunique_id=b.unique_id
  where a.eventtypeid=8
  
  
  ) a
  
  ;
  
  

  
  DROP TABLE IF EXISTS dc_na;
  CREATE EXTERNAL TABLE dc_na
  (
  user_id STRING,
  time STRING,
  activity_sub_type STRING,
  os_id BIGINT,  
  browser_id BIGINT, 
  keyword_id STRING,
  DT2_Activity_ID STRING,
  page_id BIGINT,
  buy_id BIGINT,
  site_id BIGINT,
  advertiser_id BIGINT,
  event_id BIGINT,
  revenue BIGINT,
  record_date STRING,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '/tmp/data/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na
  
  select * from (

  select 
  user_id, 
  conversiondate as time, 
  conversiontagid as activity_sub_type, 
  os_id, 
  '' as browser_id, 
  '' as keyword_id,
  '' as DT2_Activity_ID,
  placementid as page_id, 
  campaignid as buy_id, 
  site_id, 
  advertiser_id,
  EventTypeID,
  revenue,
  record_date,
  record_month, 
  record_week, 
  record_year
  from sz_conversion 
  where EventTypeID not in (3,8)

  union all

  select 
  user_id, 
  conversiondate as time, 
  conversiontagid as activity_sub_type, 
  os_id, 
  '' as browser_id, 
  coalesce(WinnerEntityID, '-99') as keyword_id,
  '' as DT2_Activity_ID,
  placementid as page_id, 
  campaignid as buy_id, 
  site_id, 
  advertiser_id,
  EventTypeID,
  revenue,
  record_date,
  record_month, 
  record_week, 
  record_year
  from sz_conversion 
  where EventTypeID=3

  union all

  select 
  a.user_id, 
  a.conversiondate as time, 
  a.conversiontagid as activity_sub_type, 
  a.os_id, 
  '' as browser_id, 
  coalesce(b.keyword_id, '-99') as keyword_id,
  '' as DT2_Activity_ID,  
  a.placementid as page_id, 
  a.campaignid as buy_id, 
  a.site_id, 
  a.advertiser_id,
  a.EventTypeID,
  a.revenue,
  a.record_date,
  a.record_month, 
  a.record_week, 
  a.record_year
  from sz_conversion a
  left join sz_sem b
  on a.winnerseunique_id=b.unique_id
  and length(a.winnerseunique_id)<>0
  where a.EventTypeID=8 

) a

  ;
  
  
  
  
  
  drop table if exists dc_match_paid_search;
  
  CREATE EXTERNAL TABLE IF NOT EXISTS dc_match_paid_search
  (
  keyword_id STRING, 
  paid_search_legacy_keyword_id STRING
  )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS textfile
  LOCATION '/tmp/data/match_dart/'
  ;
  
  
  INSERT OVERWRITE TABLE dc_match_paid_search

  select * from (
  select keyword_id as keyword_id, keyword_id as paid_search_legacy_keyword_id from sz_sem group by keyword_id
  union all
  select keywordid as keyword_id, keywordid as paid_search_legacy_keyword_id from sz_keywords
  ) a
  group by keyword_id, paid_search_legacy_keyword_id
  ;
  
  
  
  
  
  drop table if exists dc_match_operating_systems;
  
  CREATE EXTERNAL TABLE IF NOT EXISTS dc_match_operating_systems
  (os_id BIGINT,os  string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS textfile
  LOCATION '/tmp/data/match_os/'
  ;
  
  INSERT OVERWRITE TABLE dc_match_operating_systems
  select osid, osname from sz_ostype
  ;
  
  
  
  
  
  drop table if exists dc_match_cities;
  
  CREATE EXTERNAL TABLE IF NOT EXISTS dc_match_cities
  (city_id BIGINT,city  string)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS textfile
  LOCATION '/tmp/data/match_city/'
  ;
  
  
  INSERT OVERWRITE TABLE dc_match_cities
  select cityid, cityname from sz_city
  ;



  drop table if exists dc_match_activity_cats;
  
  CREATE EXTERNAL TABLE IF NOT EXISTS dc_match_activity_cats
  (activity_id STRING,
   activity_sub_type STRING,
   activity_type STRING
   )
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS textfile
  LOCATION '/tmp/data/dc_match_activity_cats/'
  ;
  
 
 

  "
  
  save(hiveSizmek, file="./data/hiveSizmek.RData")
  
  
  
