


################################################
####### HIVE REPORTING AGGREGATIONS CODE #######
################################################






hiveReportingCode<-"

  
--conversions by browser

  DROP TABLE IF EXISTS na_filt_browser;
  CREATE EXTERNAL TABLE na_filt_browser
  (
  user_id STRING,
  time string,
  browser_id int
  ) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/na_filt_browser/'
  ; 
  
  
  INSERT OVERWRITE TABLE na_filt_browser
  select a.*
  from 
      (select s.user_id, s.time, u.browser_id
      FROM dc_na_filt_LC AS s
      left JOIN dc_na_cut AS u
      ON s.user_id = u.user_id 
      and s.time = u.time) a

  inner join (select user_id from dc_icm_conv group by user_id) b
  on a.user_id=b.user_id

;



--conversions by os

DROP TABLE IF EXISTS na_filt_os;
CREATE EXTERNAL TABLE na_filt_os
(
  user_id STRING,
  time string,
  os_id int
) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/na_filt_os/'
  ; 
  
  
  INSERT OVERWRITE TABLE na_filt_os
  select a.*
  from 
  (select s.user_id, s.time, u.os_id
  FROM dc_na_filt_LC AS s
  left JOIN dc_na_cut AS u
  ON s.user_id = u.user_id 
  and s.time = u.time) a
  
  inner join (select user_id from dc_icm_conv group by user_id) b
  on a.user_id=b.user_id
  
  ;




 
--Audience

  DROP TABLE IF EXISTS audience;
  CREATE EXTERNAL TABLE audience
  (
  user_id STRING
  ) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/audience/'
  ; 
  
  
  INSERT OVERWRITE TABLE audience
  select distinct user_id
  from dc_icm_merged2
;


  DROP TABLE IF EXISTS audience_count;
  CREATE EXTERNAL TABLE audience_count
  (
  count BIGINT
  ) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/audience_count/'
  ; 
  
  
  INSERT OVERWRITE TABLE audience_count
  select count(1) from audience
;


  DROP TABLE IF EXISTS icm_count;
  CREATE EXTERNAL TABLE icm_count
  (
  count BIGINT
  ) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/icm_count/'
  ; 
  
  
  INSERT OVERWRITE TABLE icm_count
  select count(1) from dc_icm_merged2
;


--Uniques
  
  
  DROP TABLE IF EXISTS one_chan_users;
  CREATE EXTERNAL TABLE one_chan_users
  (user_id STRING) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/one_chan_users/'
  ; 
  
  
  INSERT OVERWRITE TABLE one_chan_users
  select user_id 
  from (select user_id, count(distinct channel) as channels from dc_icm_merged2 group by user_id) a
  where channels=1
  ;
  
  
  
  DROP TABLE IF EXISTS one_subchan_users;
  CREATE EXTERNAL TABLE one_subchan_users
  (user_id STRING) 
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/one_subchan_users/'
  ;

  INSERT OVERWRITE TABLE one_subchan_users
  select user_id 
  from (select user_id, count(distinct sub_channel) as sub_channels from dc_icm_merged2 group by user_id) a
  where sub_channels=1
  ;



--Uniques Distribution

  DROP TABLE IF EXISTS dc_icm_merged_unq_chan;
  CREATE EXTERNAL TABLE dc_icm_merged_unq_chan
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_unq_chan/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_unq_chan
  SELECT
  a.*
  from one_chan_users b
  inner join dc_icm_merged2 a
  on a.user_id=b.user_id
;


  DROP TABLE IF EXISTS dc_icm_merged_unq_sub;
  CREATE EXTERNAL TABLE dc_icm_merged_unq_sub
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_unq_sub/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_unq_sub
  SELECT
  a.*
  from one_subchan_users b
  inner join dc_icm_merged2 a
  on a.user_id=b.user_id
  ;











  DROP TABLE IF EXISTS unq_distr_sub;
  CREATE EXTERNAL TABLE unq_distr_sub
  (
  channel string,
  sub_channel STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_sub/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_sub
  select channel, sub_channel, count(distinct user_id)
  from dc_icm_merged_unq_sub
  group by channel, sub_channel
  ;
  
  
  DROP TABLE IF EXISTS unq_distr_chan;
  CREATE EXTERNAL TABLE unq_distr_chan
  (
  channel STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_chan/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_chan
  select channel, count(distinct user_id)
  from dc_icm_merged_unq_chan
  group by channel
  ;
  
  
  
  DROP TABLE IF EXISTS unq_distr_sub_month;
  CREATE EXTERNAL TABLE unq_distr_sub_month
  (
  channel string,
  sub_channel STRING,
  record_year STRING,
  record_month STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_sub_month/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_sub_month
  select channel, sub_channel, record_year, record_month, count(distinct user_id)
  from dc_icm_merged_unq_sub
  group by channel, sub_channel, record_year, record_month
  ;
  
  
  
  DROP TABLE IF EXISTS unq_distr_chan_month;
  CREATE EXTERNAL TABLE unq_distr_chan_month
  (
  channel STRING,
  record_year STRING,
  record_month STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_chan_month/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_chan_month
  select channel, record_year, record_month, count(distinct user_id)
  from dc_icm_merged_unq_chan
  group by channel, record_year, record_month
  ;
  
  
  DROP TABLE IF EXISTS unq_distr_sub_week;
  CREATE EXTERNAL TABLE unq_distr_sub_week
  (
  channel string,
  sub_channel STRING,
  record_year STRING,
  record_week STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_sub_week/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_sub_week
  select channel, sub_channel, record_year, record_week, count(distinct user_id)
  from dc_icm_merged_unq_sub
  group by channel, sub_channel, record_year, record_week
  ;
  
  
  
  DROP TABLE IF EXISTS unq_distr_chan_week;
  CREATE EXTERNAL TABLE unq_distr_chan_week
  (
  channel STRING,
  record_year STRING,
  record_week STRING,
  uniques BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_chan_week/'
  ; 
  
  
  INSERT OVERWRITE TABLE unq_distr_chan_week
  select channel, record_year, record_week, count(distinct user_id)
  from dc_icm_merged_unq_chan
  group by channel, record_year, record_week
  ;
  
  
  
  DROP TABLE IF EXISTS unq_distr_final;
  CREATE EXTERNAL TABLE unq_distr_final
  (
  channel STRING,
  sub_channel STRING,
  record_month STRING,
  record_week STRING,
  uniques BIGINT,
  level STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/unq_distr_final/'
  ; 
  
  INSERT OVERWRITE TABLE unq_distr_final
  
  select * from (
  
  select channel, '' as sub_channel, '' as record_month, '' as record_week,  uniques, '1_c' as level from unq_distr_chan 
  
  UNION ALL      
  
  select channel, sub_channel, '' as record_month, '' as record_week,  uniques, '1_s' as level from unq_distr_sub 
  
  UNION ALL      
  
  select channel, '' as sub_channel, '' as record_month, 
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end as record_week, 
  uniques, '3_c' as level from unq_distr_chan_week 
  
  UNION ALL      
  
  select channel, sub_channel, '' as record_month, 
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end as record_week, 
  uniques, '3_s' as level from unq_distr_sub_week 
  
  UNION ALL      
  
  select channel, '' as sub_channel, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end as record_month, 
  '' as record_week, uniques, '2_c' as level from unq_distr_chan_month 
  
  UNION ALL      
  
  select channel, sub_channel, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end as record_month, 
  '' as record_week, uniques, '2_s' as level from unq_distr_sub_month 
  
  
  ) a
  ;
  


--Audience  Distribution




  DROP TABLE IF EXISTS aud_distr_sub;
  CREATE EXTERNAL TABLE aud_distr_sub
  (
  channel string,
  sub_channel STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_sub/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_sub
  select channel, sub_channel, count(distinct user_id)
  from dc_icm_merged2
  group by channel, sub_channel
;


  DROP TABLE IF EXISTS aud_distr_chan;
  CREATE EXTERNAL TABLE aud_distr_chan
  (
  channel STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_chan/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_chan
  select channel, count(distinct user_id)
  from dc_icm_merged2
  group by channel
;



  DROP TABLE IF EXISTS aud_distr_sub_month;
  CREATE EXTERNAL TABLE aud_distr_sub_month
  (
  channel string,
  sub_channel STRING,
  record_year STRING,
  record_month STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_sub_month/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_sub_month
  select channel, sub_channel, record_year, record_month, count(distinct user_id)
  from dc_icm_merged2
  group by channel, sub_channel, record_year, record_month
;



  DROP TABLE IF EXISTS aud_distr_chan_month;
  CREATE EXTERNAL TABLE aud_distr_chan_month
  (
  channel STRING,
  record_year STRING,
  record_month STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_chan_month/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_chan_month
  select channel, record_year, record_month, count(distinct user_id)
  from dc_icm_merged2
  group by channel, record_year, record_month
;


  DROP TABLE IF EXISTS aud_distr_sub_week;
  CREATE EXTERNAL TABLE aud_distr_sub_week
  (
  channel string,
  sub_channel STRING,
  record_year STRING,
  record_week STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_sub_week/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_sub_week
  select channel, sub_channel, record_year, record_week, count(distinct user_id)
  from dc_icm_merged2
  group by channel, sub_channel, record_year, record_week
;



  DROP TABLE IF EXISTS aud_distr_chan_week;
  CREATE EXTERNAL TABLE aud_distr_chan_week
  (
  channel STRING,
  record_year STRING,
  record_week STRING,
  audience  BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_chan_week/'
  ; 
  
  
  INSERT OVERWRITE TABLE aud_distr_chan_week
  select channel, record_year, record_week, count(distinct user_id)
  from dc_icm_merged2
  group by channel, record_year, record_week
;



  DROP TABLE IF EXISTS aud_distr_final;
  CREATE EXTERNAL TABLE aud_distr_final
  (
  channel STRING,
  sub_channel STRING,
  record_month STRING,
  record_week STRING,
  audience  BIGINT,
  level STRING
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/aud_distr_final/'
  ; 

  INSERT OVERWRITE TABLE aud_distr_final

  select * from (

  select channel, '' as sub_channel, '' as record_month, '' as record_week, audience,  '1_c' as level from aud_distr_chan 

  UNION ALL      
  
  select channel, sub_channel, '' as record_month, '' as record_week, audience,  '1_s' as level from aud_distr_sub 
 
  UNION ALL      
  
  select channel, '' as sub_channel, '' as record_month, 
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end as record_week, 
  audience,  '3_c' as level from aud_distr_chan_week 

  UNION ALL      
  
  select channel, sub_channel, '' as record_month, 
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end as record_week, 
  audience,  '3_s' as level from aud_distr_sub_week 

  UNION ALL      
  
  select channel, '' as sub_channel, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end as record_month, 
  '' as record_week, audience,  '2_c' as level from aud_distr_chan_month 

  UNION ALL      
  
  select channel, sub_channel, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end as record_month, 
  '' as record_week, audience,  '2_s' as level from aud_distr_sub_month 


  ) a
;





--Operating System Distribution


  DROP TABLE IF EXISTS os_distr;
  CREATE EXTERNAL TABLE os_distr
  (
   os_id BIGINT,
   channel string,
   sub_channel string,
   users bigint,
   conv bigint,
   os string
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/os_distr/'
  ; 
  
  
  INSERT OVERWRITE TABLE os_distr

  select a.*, b.os

  from (select 
  a.os_id, a.channel, a.sub_channel,
  a.users, coalesce(b.conv,0)

  from (select os_id, channel, sub_channel, count(distinct user_id) as users from dc_icm_merged2 group by os_id, channel, sub_channel) a
  left join (select os_id, channel, sub_channel, count(distinct user_id) as conv from dc_icm_conv group by os_id, channel, sub_channel) b
  on a.os_id=b.os_id
  and a.channel=b.channel
  and a.sub_channel=b.sub_channel) a

  left join dc_match_operating_systems b
  on a.os_id=b.os_id
;



--City distribution

  DROP TABLE IF EXISTS city_distr;
  CREATE EXTERNAL TABLE city_distr
  (
   city_id BIGINT,
   channel string,
   sub_channel string,
   users bigint,
   conv bigint,
   city string
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/city_distr/'
  ; 
  
  
  INSERT OVERWRITE TABLE city_distr

  select a.*, b.city

  from (

  select 
  a.city_id, a.channel, a.sub_channel,
  a.users, coalesce(b.conv,0)

  from (select city_id, channel, sub_channel, count(distinct user_id) as users from dc_icm_merged2 group by city_id, channel, sub_channel) a
  left join (select city_id, channel, sub_channel, count(distinct user_id) as conv from dc_icm_conv group by city_id, channel, sub_channel) b
  on a.city_id=b.city_id
  and a.channel=b.channel
  and a.sub_channel=b.sub_channel) a

  left join dc_match_cities b
  on a.city_id=b.city_id
;





--click and imp time distr


  DROP TABLE IF EXISTS click_imp_time_distr;
  CREATE EXTERNAL TABLE click_imp_time_distr
  (
  channel string,
  sub_channel string,
  date_label string,
  day_of_week int,
  hour_of_day int,
  conv_path string,
  os_id bigint,
  impressions bigint,
  clicks bigint,
  record_week string,
  record_month string

  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/click_imp_time_distr/'
  ; 

  INSERT OVERWRITE TABLE click_imp_time_distr
  select
  channel,
  sub_channel,
  to_date(time) as date_label,
  case when from_unixtime(unix_timestamp(time),'u')=7 then 0 else from_unixtime(unix_timestamp(time),'u') end as day_of_week,
  hour(time) as hour_of_day,
  case when b.user_id is not null then 'C' else 'NC' end as conv_path,
  os_id, 
  sum(case when a.eventind=2 then 1 else 0 end) as impressions,
  sum(case when a.eventind=1 then 1 else 0 end) as clicks,
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end as record_week, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end as record_month 

  from dc_icm_merged2 a
  left join (select user_id from dc_icm_conv group by user_id) b 
  on a.user_id=b.user_id
 
  group by 
  channel,
  sub_channel,
  to_date(time),
  case when from_unixtime(unix_timestamp(time),'u')=7 then 0 else from_unixtime(unix_timestamp(time),'u') end,
  hour(time),
  case when b.user_id is not null then 'C' else 'NC' end,
  os_id, 
  case when length(record_week)=1 then concat(record_year, concat('0', record_week)) else concat(record_year, record_week) end, 
  case when length(record_month)=1 then concat(record_year, concat('0', record_month)) else concat(record_year, record_month) end 

  ;



--Impression Distribution


DROP TABLE IF EXISTS imp_distr;
CREATE EXTERNAL TABLE imp_distr
(
  sub_channel string,
  imps bigint
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/imp_distr/'
  ; 
  
  INSERT OVERWRITE TABLE imp_distr
  select sub_channel, sum(impressions)
  from click_imp_time_distr
  group by sub_channel
  ;
  
  
  --Click Distribution
  
  
  DROP TABLE IF EXISTS click_distr;
  CREATE EXTERNAL TABLE click_distr
  (
  sub_channel string,
  clicks bigint
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/click_distr/'
  ; 
  
  INSERT OVERWRITE TABLE click_distr
  select sub_channel, sum(clicks)
  from click_imp_time_distr
  group by sub_channel
  ;


--Events

  DROP TABLE IF EXISTS events;
  CREATE EXTERNAL TABLE events
  (
  channel string,
  sub_channel string,
  record_month string,
  record_week string,
  clicks bigint,
  impressions bigint,
  level string
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/events/'
  ; 
  
  INSERT OVERWRITE TABLE events

  select * from (

  select channel, '' as sub_channel, '' as record_month, '' as record_week, 
  sum(clicks), sum(impressions), '1_c' as level 
  from click_imp_time_distr
  group by channel

  UNION ALL

  select channel, sub_channel, '' as record_month, '' as record_week, 
  sum(clicks), sum(impressions), '1_s' as level 
  from click_imp_time_distr
  group by channel, sub_channel


  UNION ALL

  select channel, '' as sub_channel, record_month, '' as record_week, 
  sum(clicks), sum(impressions), '2_c' as level 
  from click_imp_time_distr
  group by channel, record_month

  UNION ALL

  select channel, sub_channel, record_month, '' as record_week, 
  sum(clicks), sum(impressions), '2_s' as level 
  from click_imp_time_distr
  group by channel, sub_channel, record_month



  UNION ALL

  select channel, '' as sub_channel, '' as record_month, record_week, 
  sum(clicks), sum(impressions), '3_c' as level 
  from click_imp_time_distr
  group by channel, record_week

  UNION ALL

  select channel, sub_channel, '' as record_month, record_week, 
  sum(clicks), sum(impressions), '3_s' as level 
  from click_imp_time_distr
  group by channel, sub_channel, record_week

  ) u



  ;




  DROP TABLE IF EXISTS costs;
  CREATE EXTERNAL TABLE costs
  (
  date_label string,  
  channel string,
  sub_channel string,
  impressions bigint,
  clicks bigint
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/costs/'
  ; 
  
  INSERT OVERWRITE TABLE costs

  select
  to_date(time), channel,  sub_channel,
  sum(case when eventind = 2 then 1 else 0 end) as impressions,
  sum(case when eventind = 1 then 1 else 0 end) as clicks
  from dc_icm_merged2
  group by to_date(time), channel,  sub_channel

;


  "
  


save(hiveReportingCode, file="./data/hiveReportingCode.RData")




