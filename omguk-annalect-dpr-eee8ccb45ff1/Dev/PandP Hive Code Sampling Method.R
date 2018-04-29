hiveSampleRatio<-"

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
  inner join dc_icm_merged2 b
  on a.user_id=b.user_id) a

union all

  select user_id, time, city_id, os_id, page_id, buy_id, browser_id, site_id, advertiser_id, 
  record_month, record_week, record_year, keyword_id, eventind, channel, sub_channel, rank
  from dc_icm_conv
  ;


"

save(hiveSampleRatio, file="./data/hiveSampleRatio.RData")





hiveSampleLimit<-"

DROP TABLE IF EXISTS dc_unq_non_cons_sample;
CREATE EXTERNAL TABLE dc_unq_non_cons_sample
(user_id	STRING, rand	DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/dc_unq_non_cons_sample/'
;	 

INSERT OVERWRITE TABLE dc_unq_non_cons_sample 

select
*
from dc_unq_non_cons 
distribute by rand
sort by rand
limit ${hiveconf:non_conv_limit}
;



DROP TABLE IF EXISTS dc_unq_cons_sample;
CREATE EXTERNAL TABLE dc_unq_cons_sample
(user_id	STRING, rand	DOUBLE)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/dc_unq_cons_sample/'
;	 

INSERT OVERWRITE TABLE dc_unq_cons_sample 

select * from 
    (select user_id , rand() as rand from dc_icm_conv group by user_id) a
distribute by rand
sort by rand
limit ${hiveconf:conv_limit}
;





DROP TABLE IF EXISTS dc_icm_conv_sample;
CREATE EXTERNAL TABLE dc_icm_conv_sample
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
  LOCATION '${hiveconf:s3location}/dc_icm_conv_sample/'
  ; 

  INSERT OVERWRITE TABLE dc_icm_conv_sample
  select b.* 
  from dc_unq_cons_sample a
  inner join dc_icm_conv b
  on a.user_id=b.user_id
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
                  select b.*
                  from dc_unq_non_cons_sample a
                  inner join dc_icm_merged2 b
                  on a.user_id=b.user_id
                ) a
  
  union all
  
  select user_id, time, city_id, os_id, page_id, buy_id, browser_id, site_id, advertiser_id, 
  record_month, record_week, record_year, keyword_id, eventind, channel, sub_channel, rank
  from dc_icm_conv_sample
  
  ;


--count convs and sampled convs to know how to up-weight

DROP TABLE IF EXISTS dc_icm_conv_counts;
CREATE EXTERNAL TABLE dc_icm_conv_counts
  (
  label	STRING, count	BIGINT
  ) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/dc_icm_conv_counts/'
  ; 
  

INSERT OVERWRITE TABLE dc_icm_conv_counts

select * from (

  select 'sampled_converters' as label, count(distinct user_id) from dc_icm_conv_sample

  union all

  select 'total_converters' as label, count(distinct user_id) from dc_icm_conv
)a
;



--reassign the sample to the label dc_icm_conv for use in the last click table
  
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
  LOCATION '${hiveconf:s3location}/dc_icm_conv_sample/'
  ; 


 
  
  "

save(hiveSampleLimit, file="./data/hiveSampleLimit.RData")















