
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
  
  select a.user_id
  from dc_na a
  left join dc_match_activity_cats b
  on a.DT2_Activity_ID = b.activity_id
  where advertiser_id in ${hiveconf:advertiserIds}
  ${hiveconf:activity_sub_type}
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
  and a.user_id != '0'
  group by a.user_id;



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
  revenue BIGINT,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  
  select a.user_id, time, a.activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, revenue, record_month, record_week, record_year
  from dc_na a
  left join dc_match_activity_cats b
  on a.DT2_Activity_ID = b.activity_id
  left join dc_na_pre c
  on a.user_id=c.user_id
  where c.user_id is NULL
  and advertiser_id in ${hiveconf:advertiserIds}
  ${hiveconf:activity_sub_type}
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
  
  

save(hiveConvCode, file="./data/hiveConvCode.RData")








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
  revenue BIGINT,
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
  page_id, buy_id, site_id, advertiser_id, revenue, record_month, record_week, record_year
  from dc_na
  where advertiser_id in ${hiveconf:advertiserIds}
  and event_id in (1,2)
  and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startDate}, 'yyyy-MM-dd')
  and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endDate}, 'yyyy-MM-dd')
  and month_p in (${hiveconf:months})
  
  UNION ALL
  
  SELECT user_id, time, 'click' as activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, 0 as revenue, record_month, record_week, record_year
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
save(hiveNVCode, file="./data/hiveNVCode.RData")











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
  revenue BIGINT,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  
  select a.user_id, time, activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, revenue, record_month, record_week, record_year
  from dc_na a
  left join prior_visitors b
  on a.user_id=b.user_id
  where advertiser_id in ${hiveconf:advertiserIds}
  ${hiveconf:activity_sub_type}
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


save(hiveNVCCode, file="./data/hiveNVCCode.RData")








######################################
#### HIVE CONVERTERS SAMPLING CODE ###
######################################

hiveConvSampleCode<-"

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
from dc_na a
left join dc_match_activity_cats b
on a.DT2_Activity_ID = b.activity_id
where advertiser_id in ${hiveconf:advertiserIds}
${hiveconf:activity_sub_type}
and unix_timestamp(record_date, 'yyyy-MM-dd') > unix_timestamp(${hiveconf:startPre}, 'yyyy-MM-dd')
and unix_timestamp(record_date, 'yyyy-MM-dd') < unix_timestamp(${hiveconf:endPre}, 'yyyy-MM-dd')
and user_id != '0'
group by user_id;



--pull all convertors in the main period and eliminate any that also convert in the pre period

DROP TABLE IF EXISTS dc_na_all;
CREATE EXTERNAL TABLE dc_na_all
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
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na_all/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_all
  
  select a.user_id, time, activity_sub_type, os_id, browser_id, keyword_id,
  page_id, buy_id, site_id, advertiser_id, revenue, record_month, record_week, record_year
  from dc_na a
  left join dc_match_activity_cats b
  on a.DT2_Activity_ID = b.activity_id
  left join dc_na_pre c
  on a.user_id=b.user_id
  where c.user_id is NULL
  and advertiser_id in ${hiveconf:advertiserIds}
  ${hiveconf:activity_sub_type}
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
  
  DROP TABLE IF EXISTS dc_icm_all;
  CREATE EXTERNAL TABLE dc_icm_all
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
  LOCATION '${hiveconf:s3location}/dc_icm_all/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_all
  
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
  
  
DROP TABLE IF EXISTS dc_sample;
CREATE EXTERNAL TABLE dc_sample
(user_id	STRING, rand	DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/dc_sample/'
;	 

INSERT OVERWRITE TABLE dc_sample 

select * from 
    (select user_id , rand() as rand from dc_icm_all group by user_id) a
where rand>${hiveconf:sample}
;

--order by rand
--limit ${hiveconf:conv_limit}



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
  select b.*
  from dc_sample a
  inner join dc_icm_all b
  on a.user_id=b.user_id
;



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
  revenue BIGINT,
  record_month BIGINT,
  record_week BIGINT,
  record_year BIGINT
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_na/'
  ;
  
  INSERT OVERWRITE TABLE dc_na_cut
  select a.*
  from dc_na_all a
  inner join dc_sample b
  on a.user_id=b.user_id
;




  
  "
  
  

save(hiveConvSampleCode, file="./data/hiveConvSampleCode.RData")


