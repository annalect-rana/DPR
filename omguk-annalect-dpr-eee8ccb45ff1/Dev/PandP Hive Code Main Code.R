


######################################
####### HIVE MAIN PROCESS CODE #######
######################################


hiveMainCode<-"

  
  DROP TABLE IF EXISTS dc_na_filt_LC_temp;
  CREATE EXTERNAL TABLE dc_na_filt_LC_temp
  (
  user_id STRING,
  time STRING,
  activity_sub_type STRING,
  os_id BIGINT,  
  browser_id BIGINT, 
  keyword_id STRING,
  page_id BIGINT,
  buy_id BIGINT,
  site_id BIGINT,
  advertiser_id BIGINT,
  revenue BIGINT,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT,
  rank INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na_filt_LC_temp/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_filt_LC_temp
  
  select * from (
  
  select
  a.*,
  rank () over (partition by a.user_id order by a.time, rand()) as rank
  from dc_na_cut a
  inner join dc_icm b
  on a.user_id=b.user_id

  ${hiveconf:jointype} join hierarchy h
  on a.page_id=h.page_id

  
  ) a
  where rank=1
  ;
  
  
  
  
  DROP TABLE IF EXISTS dc_icm_conv;
  CREATE EXTERNAL TABLE dc_icm_conv
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT,
  initial_revenue BIGINT,
  all_revenue BIGINT

  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv/'
  ;
  
  INSERT OVERWRITE TABLE dc_icm_conv
  
  select
  a.*, b.revenue, tot_rev.tot_revenue
  from dc_icm_merged a
  
  inner join dc_na_filt_LC_temp b 
  on a.user_id=b.user_id

  left join (select user_id, sum(revenue) as tot_revenue from dc_na_cut group by user_id)  as tot_rev
  on a.user_id=tot_rev.user_id

  where a.time<b.time
  ;
  

  
  

  DROP TABLE IF EXISTS dc_icm_merged2;
  CREATE EXTERNAL TABLE dc_icm_merged2
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_merged2/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged2
  
  select * from (
  
  select a.* 
  from dc_icm_merged a
  left join dc_icm_conv b
  on a.user_id=b.user_id
  where b.user_id is NULL
  
  UNION ALL
  
  select user_id, time, city_id, os_id, page_id, buy_id, browser_id, site_id, advertiser_id, 
  record_month, record_week, record_year, keyword_id, eventind, channel, sub_channel, rank
  from dc_icm_conv
  
  )u; 
  
  
  
  DROP TABLE IF EXISTS dc_unq_non_cons;
  CREATE EXTERNAL TABLE dc_unq_non_cons
  (  user_id	STRING,
  rand	DOUBLE
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_unq_non_cons/'
  ;	
  
  
  INSERT OVERWRITE TABLE dc_unq_non_cons
  select a.user_id, rand() as rand
  from dc_icm_merged a
  left join dc_icm_conv b
  on a.user_id=b.user_id
  where b.user_id is NULL
  group by a.user_id;
  
  "
  


save(hiveMainCode, file="./data/hiveMainCode.RData")







######################################
####### HIVE MAIN PROCESS CODE #######
######################################

#this version of the code alters the path of convertors.
#if the last event is an seo/brand ppc event it get changed to an impression for down weighting in the model



hiveMainCodeSEOGateway<-"


DROP TABLE IF EXISTS dc_na_filt_LC_temp;
CREATE EXTERNAL TABLE dc_na_filt_LC_temp
(
  user_id STRING,
  time STRING,
  activity_sub_type STRING,
  os_id BIGINT,  
  browser_id BIGINT, 
  keyword_id STRING,
  page_id BIGINT,
  buy_id BIGINT,
  site_id BIGINT,
  advertiser_id BIGINT,
  revenue BIGINT,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT,
  rank INT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na_filt_LC_temp/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_filt_LC_temp
  
  select * from (
  
  select
  a.*,
  rank () over (partition by a.user_id order by a.time, rand()) as rank
  from dc_na_cut a
  inner join dc_icm b
  on a.user_id=b.user_id

  ${hiveconf:jointype} join hierarchy h
  on a.page_id=h.page_id
  
  
  ) a
  where rank=1
  ;
  
  
  
  
  DROP TABLE IF EXISTS dc_icm_conv_temp;
  CREATE EXTERNAL TABLE dc_icm_conv_temp
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT,
  initial_revenue BIGINT,
  all_revenue BIGINT

  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv_temp/'
  ;
  
  INSERT OVERWRITE TABLE dc_icm_conv_temp
  
  select
  a.*, b.revenue, tot_rev.tot_revenue
  from dc_icm_merged a
  
  inner join dc_na_filt_LC_temp b 
  on a.user_id=b.user_id

  left join (select user_id, sum(revenue) as tot_revenue from dc_na_cut group by user_id)  as tot_rev
  on a.user_id=tot_rev.user_id

  where a.time<b.time
  ;


  
  DROP TABLE IF EXISTS dc_icm_conv_temp_order;
  CREATE EXTERNAL TABLE dc_icm_conv_temp_order
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT,
   initial_revenue BIGINT,
  all_revenue BIGINT,
  dp_order INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv_temp_order/'
  ;

  INSERT OVERWRITE TABLE dc_icm_conv_temp_order
  select
  *,
  rank () over (partition by user_id order by time desc) as dp_order
  from dc_icm_conv_temp;



  DROP TABLE IF EXISTS dc_icm_conv;
  CREATE EXTERNAL TABLE dc_icm_conv
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT,
  initial_revenue BIGINT,
  all_revenue BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv/'
  ;
  
  INSERT OVERWRITE TABLE dc_icm_conv


  select
  a.user_id, a.time,  a.city_id,  a.os_id,  a.page_id,  a.buy_id,  a.browser_id,  a.site_id,  
  a.advertiser_id,  a.record_month,  a.record_week,    a.record_year,  a.keyword_id,

  --if the sub_channel is in the list and the event is the last in the pathway adn its not the only event in the pathway force it to an impression
  case when a.dp_order=1 and b.max_dp_order>1 and a.sub_channel in ${hiveconf:searchgateway}  then 2 else eventind end as eventind,
  
  channel,  sub_channel,  rank,  initial_revenue, all_revenue
  
  from dc_icm_conv_temp_order a
  left join (select user_id, max(dp_order) as max_dp_order from dc_icm_conv_temp_order group by user_id) b
  on a.user_id=b.user_id

  ;





  
  
  
  DROP TABLE IF EXISTS dc_icm_merged2;
  CREATE EXTERNAL TABLE dc_icm_merged2
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
  record_month	BIGINT,
  record_week	BIGINT,  
  record_year	BIGINT,
  keyword_id STRING,
  eventind INT,
  channel STRING,
  sub_channel STRING,
  rank INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_merged2/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged2
  
  select * from (
  
  select a.* 
  from dc_icm_merged a
  left join dc_icm_conv b
  on a.user_id=b.user_id
  where b.user_id is NULL
  
  UNION ALL
  
  select user_id, time, city_id, os_id, page_id, buy_id, browser_id, site_id, advertiser_id, 
  record_month, record_week, record_year, keyword_id, eventind, channel, sub_channel, rank
  from dc_icm_conv
  
  )u; 
  
  
  
  DROP TABLE IF EXISTS dc_unq_non_cons;
  CREATE EXTERNAL TABLE dc_unq_non_cons
  (  user_id	STRING,
  rand	DOUBLE
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_unq_non_cons/'
  ;	
  
  
  INSERT OVERWRITE TABLE dc_unq_non_cons
  select a.user_id, rand() as rand
  from dc_icm_merged a
  left join dc_icm_conv b
  on a.user_id=b.user_id
  where b.user_id is NULL
  group by a.user_id;
  
  "
  
  
  
  save(hiveMainCodeSEOGateway, file="./data/hiveMainCodeSEOGateway.RData")















