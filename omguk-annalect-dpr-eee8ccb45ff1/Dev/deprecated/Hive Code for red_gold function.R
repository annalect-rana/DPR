
######################################
#### HIVE CONVERTERS CODE ############
######################################

hiveConvCode<-"

--pull on converters in the pre period

DROP TABLE IF EXISTS dc_na_pre;
CREATE EXTERNAL TABLE dc_na_pre
( user_id STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na_pre/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_pre
  
  select user_id
  from dc_na
  where advertiser_id in ${hiveconf:advertiserIds}
  and activity_sub_type in ${hiveconf:activity_sub_type}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
  and user_id != '0'
  group by user_id;



--pull all convertors in the main period and eliminate any that also convert in the pre period

DROP TABLE IF EXISTS dc_na_cut;
CREATE EXTERNAL TABLE dc_na_cut
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
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  
  select a.user_id, time, activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, record_month, record_week, record_year
  from dc_na a
  left join dc_na_pre b
  on a.user_id=b.user_id
  where b.user_id is NULL
  and advertiser_id in ${hiveconf:advertiserIds}
  and activity_sub_type in ${hiveconf:activity_sub_type}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startDate}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd');




  
  --pull all clicks in the pre and main period
  
  DROP TABLE IF EXISTS dc_c_cut;
  CREATE EXTERNAL TABLE dc_c_cut
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
  record_month BIGINT,	
  record_week BIGINT,
  record_year BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_c/'
  ;
  
  
  INSERT OVERWRITE TABLE dc_c_cut
  
  SELECT user_id, time, os_id, city_id, page_id, buy_id, site_id, browser_id, keyword_id,
  advertiser_id, record_month, record_week, record_year
  FROM dc_c 
  where advertiser_id in ${hiveconf:advertiserIds}
  and buy_id in ${hiveconf:campaignIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and user_id != '0'
  ;
  
  --pull all impressions in the pre and main period
  
  DROP TABLE IF EXISTS dc_i_cut;
  CREATE EXTERNAL TABLE dc_i_cut
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
  record_year	BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_i/'
  ;
  
  INSERT OVERWRITE TABLE dc_i_cut
  
  SELECT user_id, time, city_id, os_id, page_id, buy_id, browser_id,
  site_id, advertiser_id, record_month, record_week, record_year
  FROM dc_i
  where advertiser_id in ${hiveconf:advertiserIds}
  and buy_id in ${hiveconf:campaignIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and user_id != '0'
  ;
  

  --union clicks and impressions and eliminate those of convertors in the pre period
  
  DROP TABLE IF EXISTS dc_icm;
  CREATE EXTERNAL TABLE dc_icm
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
  eventind INT
  
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm

  select a.* from (
  
  select user_id, time, city_id, os_id, page_id, buy_id, browser_id,
  site_id, advertiser_id, record_month, record_week, record_year,
  0 as keyword_id, 2 as eventind
  from dc_i_cut 

  UNION ALL

  select user_id, time, city_id, os_id, page_id, buy_id, browser_id, 
  site_id, advertiser_id, record_month, record_week, record_year,
  keyword_id, 1 as eventind
  from dc_c_cut 

  ) a
 left join dc_na_pre b
 on a.user_id=b.user_id
 where b.user_id is NULL;
  
  
  
  "
  
  

save(hiveConvCode, file="P:/GiT Repos/dpr/data/hiveConvCode.RData")








######################################
#### HIVE NEW VISITORS CODE ##########
######################################










hiveNVCode<-"

--pull together a list of users who have any clicks or network activity prior to the campaign

DROP TABLE IF EXISTS prior_visitors;
CREATE EXTERNAL TABLE prior_visitors
(user_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/prior_visitors/'
;

INSERT OVERWRITE TABLE prior_visitors

select user_id from (


select user_id
from dc_c
where advertiser_id in ${hiveconf:advertiserIds}
and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPrior}, 'yyyy-MM-dd')
and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
and month_p in (${hiveconf:priormonths}, ${hiveconf:premonths})

union all  

select user_id
from dc_na
where advertiser_id in ${hiveconf:advertiserIds}
and event_id in (1,2)
and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPrior}, 'yyyy-MM-dd')
and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
and month_p in (${hiveconf:priormonths}, ${hiveconf:premonths})

) a
group by user_id;



--pull together any clicks from that campaign or network activity during the campaign period and exclude any prior period users. this is your new visitors.


DROP TABLE IF EXISTS dc_na_cut;
CREATE EXTERNAL TABLE dc_na_cut
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
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  select a.* from (
  
  select user_id, time, activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, record_month, record_week, record_year
  from dc_na
  where advertiser_id in ${hiveconf:advertiserIds}
  and event_id in (1,2)
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startDate}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:months})
  
  UNION ALL
  
  SELECT user_id, time, 'click' as activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, record_month, record_week, record_year
  FROM dc_c 
  where advertiser_id in ${hiveconf:advertiserIds}
  and buy_id in ${hiveconf:campaignIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startDate}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:months})
  
  ) a
  left join prior_visitors b
  on a.user_id=b.user_id
  where b.user_id is NULL;
  




 DROP TABLE IF EXISTS dc_i_cut;
  CREATE EXTERNAL TABLE dc_i_cut
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
  eventind INT
  
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_i/'
  ;  
  
  INSERT OVERWRITE TABLE dc_i_cut
  
  
  SELECT i.user_id, time, city_id, os_id, page_id, buy_id, browser_id,
  site_id, advertiser_id, record_month, record_week, record_year, 
  0 as keyword_id, 2 as eventind
  FROM dc_i i
  where advertiser_id in ${hiveconf:advertiserIds}
  and buy_id in ${hiveconf:campaignIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and i.user_id!= '0'
  and month_p in (${hiveconf:premonths}, ${hiveconf:months})
  ;

  DROP TABLE IF EXISTS pre_visitors;
  CREATE EXTERNAL TABLE pre_visitors
  (user_id STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/pre_visitors/'
  ;
  
  INSERT OVERWRITE TABLE pre_visitors
  
  select user_id from (
  
  
  select user_id
  from dc_c
  where advertiser_id in ${hiveconf:advertiserIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:premonths})
  
  union all  
  
  select user_id
  from dc_na
  where advertiser_id in ${hiveconf:advertiserIds}
  and event_id in (1,2)
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:premonths})
  
  ) a
  group by user_id;


  
  DROP TABLE IF EXISTS dc_icm;
  CREATE EXTERNAL TABLE dc_icm
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
  eventind INT
  
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm
  
  
  SELECT i.*
  FROM dc_i_cut i
  left join pre_visitors p
  on i.user_id=p.user_id
  where p.user_id is NULL;


  
"
save(hiveNVCode, file="P:/GiT Repos/dpr/data/hiveNVCode.RData")











######################################
## HIVE NEW VISITOR CONVERTORS CODE ##
######################################










hiveNVCCode<-"

--pull together a list of users who have any clicks or network activity prior to the campaign

DROP TABLE IF EXISTS prior_visitors;
CREATE EXTERNAL TABLE prior_visitors
(user_id STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/prior_visitors/'
;

INSERT OVERWRITE TABLE prior_visitors

select user_id from (


select user_id
from dc_c
where advertiser_id in ${hiveconf:advertiserIds}
and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPrior}, 'yyyy-MM-dd')
and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')

union all  

select user_id
from dc_na
where advertiser_id in ${hiveconf:advertiserIds}
and event_id in (1,2)
and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPrior}, 'yyyy-MM-dd')
and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')


) a
group by user_id;


--pull network activity during the campaign period and exclude any prior period users. this is your new visitor convertors.

DROP TABLE IF EXISTS dc_na_cut;
CREATE EXTERNAL TABLE dc_na_cut
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
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  
  select a.user_id, time, activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, record_month, record_week, record_year
  from dc_na a
  left join prior_visitors b
  on a.user_id=b.user_id
  where advertiser_id in ${hiveconf:advertiserIds}
  and activity_sub_type in ${hiveconf:activity_sub_type}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startDate}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:months})
  and b.user_id is NULL;
  
  --pull impressions in the pre period and main period

  DROP TABLE IF EXISTS dc_i_cut;
  CREATE EXTERNAL TABLE dc_i_cut
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
  eventind INT
  
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_i/'
  ;  
  
  INSERT OVERWRITE TABLE dc_i_cut
  
  SELECT i.user_id, i.time, i.city_id, i.os_id, i.page_id, i.buy_id, i.browser_id,
  i.site_id, i.advertiser_id, i.record_month, i.record_week, i.record_year, 
  0 as keyword_id, 2 as eventind
  from dc_i i
  where advertiser_id in ${hiveconf:advertiserIds}
  and buy_id in ${hiveconf:campaignIds}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:months})
  and i.user_id!= '0'
  
  ;

  --eliminate impressions of anyone who had visited the site in the prior or pre period
  
  DROP TABLE IF EXISTS dc_icm;
  CREATE EXTERNAL TABLE dc_icm
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
  eventind INT
  
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm
  
  SELECT i.*
  FROM dc_i_cut i
  left join prior_visitors p
  on i.user_id=p.user_id
  where p.user_id is NULL;


  
  "


save(hiveNVCCode, file="P:/GiT Repos/dpr/data/hiveNVCCode.RData")








######################################
####### HIVE MAIN PROCESS CODE #######
######################################






hiveMainCode<-"

DROP TABLE IF EXISTS hierarchy;
CREATE EXTERNAL TABLE hierarchy
(Page_ID BIGINT,
Buy_ID BIGINT,
Channel STRING,
Sub_Channel STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/hierarchy_file/'
tblproperties (\"skip.header.line.count\"=\"1\")
;

DROP TABLE IF EXISTS dc_icm_merged;
CREATE EXTERNAL TABLE dc_icm_merged
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged
  select * from (	
  
  select
  i.*, coalesce(channel, 'Other'), coalesce(sub_channel, 'Other'), 
  rank () over (partition by user_id, sub_channel, time order by rand()) as rank
  
  from dc_icm i
  left join hierarchy h
  on i.page_id=h.page_id
  
  ) a where rank=1;
  
  
  
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
  rank INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv/'
  ;
  
  INSERT OVERWRITE TABLE dc_icm_conv
  
  select
  a.*
  from dc_icm_merged a
  inner join dc_na_filt_LC_temp b 
  on a.user_id=b.user_id	
  where a.time<b.time
  ;
  

  
  
  
  DROP TABLE IF EXISTS dc_na_filt_LC;
  CREATE EXTERNAL TABLE dc_na_filt_LC
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
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT,
  rank INT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na_filt_LC/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_filt_LC
  
  
  select
  a.*
  from dc_na_filt_LC_temp a
  inner join (select user_id from dc_icm_conv group by user_id) b
  on a.user_id=b.user_id;
  
  





 DROP TABLE IF EXISTS LC_merged;
  CREATE EXTERNAL TABLE LC_merged
  (
  user_id STRING,
  time STRING,
  channel STRING,
  sub_channel STRING,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/LC_merged/'
  ;
  
  INSERT OVERWRITE TABLE LC_merged
  
  
  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
  record_month,record_week,record_year
  FROM dc_na_filt_LC lc
  left join hierarchy cat
  ON lc.page_id = cat.page_id
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
  
  select * from dc_icm_conv
  
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
  
  
  
  
  
  DROP TABLE IF EXISTS dc_unq_non_cons_sample;
  CREATE EXTERNAL TABLE dc_unq_non_cons_sample
  (user_id	STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_unq_non_cons_sample/'
  ;	 
  
  INSERT OVERWRITE TABLE dc_unq_non_cons_sample 
  
  select
  user_id
  from dc_unq_non_cons a, 
  ( 
  select 
  a.convs,
  b.non_convs,
  convs*${hiveconf:sampleratio}/non_convs as cutoff
  from 
  (select count(distinct user_id) as convs from dc_icm_conv) a,
  (select count(1) as non_convs from dc_unq_non_cons) b
  
  ) b
  where a.rand<b.cutoff
  ;
  
  
  
  DROP TABLE IF EXISTS dc_icm_sample;
  CREATE EXTERNAL TABLE dc_icm_sample
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
  LOCATION '${hiveconf:s3location}/dc_icm_sample/'
  ;  
  
  
  INSERT OVERWRITE TABLE dc_icm_sample
  
  select * from (
  select
  b.*
  from dc_unq_non_cons_sample a
  inner join dc_icm_merged b
  on a.user_id=b.user_id) a
  
  union all
  
  select * from dc_icm_conv;"

save(hiveMainCode, file="P:/GiT Repos/dpr/data/hiveMainCode.RData")




