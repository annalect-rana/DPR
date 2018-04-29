

###########################################
######### standard match ##################
###########################################


hiveMergeStd1<-"

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
  ${hiveconf:jointype} join hierarchy h
  on i.page_id=h.page_id
  
  ) a 
  where rank=1
  ;
  "
  
  save(hiveMergeStd1, file="./data/hiveMergeStd1.RData")
  
  
  
  
  
  hiveMergeStd2<-"
  
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
  revenue BIGINT,
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
  ${hiveconf:jointype} join hierarchy cat
  ON lc.page_id = cat.page_id
  ;
  
  "
save(hiveMergeStd2, file="./data/hiveMergeStd2.RData")

















###########################################
######### keyword matching ################
###########################################



hiveMergeKeyword1<-"

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


DROP TABLE IF EXISTS ppc_hierarchy;
CREATE EXTERNAL TABLE ppc_hierarchy
(
ppc_match_col STRING,
channel STRING,
sub_Channel STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '${hiveconf:s3location}/ppc_hierarchy_file/'
tblproperties (\"skip.header.line.count\"=\"1\")
;


DROP TABLE IF EXISTS dc_icm_merged_ppc;

CREATE EXTERNAL TABLE dc_icm_merged_ppc
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_ppc/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_ppc
  select * from (	
  
  select
  i.*, coalesce(channel, 'Other'), coalesce(sub_channel, 'Other'), 
  rank () over (partition by user_id, sub_channel, time order by rand()) as rank
  
  from dc_icm i
  left join (select paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col} from dc_match_paid_search group by paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col}) ds
  on i.keyword_id=ds.paid_search_legacy_keyword_id
  ${hiveconf:jointype} join ppc_hierarchy h
  on ds.${hiveconf:ppc_match_col}=h.ppc_match_col
  where i.keyword_id<>0
  and i.keyword_id<>''
  ) a 
  where rank=1
  ;

DROP TABLE IF EXISTS dc_icm_merged_non_ppc;

  CREATE EXTERNAL TABLE dc_icm_merged_non_ppc
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_non_ppc/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_non_ppc
  select * from (	
  
  select
  i.*, coalesce(channel, 'Other'), coalesce(sub_channel, 'Other'), 
  rank () over (partition by user_id, sub_channel, time order by rand()) as rank
  
  from dc_icm i
  ${hiveconf:jointype} join hierarchy h
  on i.page_id=h.page_id
  where i.keyword_id=0
  or i.keyword_id=''
  
  ) a 
  where rank=1
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
  
  select * from dc_icm_merged_ppc

  UNION ALL
  
  select * from dc_icm_merged_non_ppc
  
  )a; 



  "
  
  save(hiveMergeKeyword1, file="./data/hiveMergeKeyword1.RData")
  
  
  
  
  
  hiveMergeKeyword2<-"
  
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
  revenue BIGINT,
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
  
  
  DROP TABLE IF EXISTS LC_merged_ppc;
  CREATE EXTERNAL TABLE LC_merged_ppc
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
  LOCATION '${hiveconf:s3location}/LC_merged_ppc/'
  ;
  
  INSERT OVERWRITE TABLE LC_merged_ppc
  
  
  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
  record_month,record_week,record_year
  FROM dc_na_filt_LC lc
  left join (select paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col} from dc_match_paid_search group by paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col}) ds
  on lc.keyword_id=ds.paid_search_legacy_keyword_id
  ${hiveconf:jointype} join ppc_hierarchy h
  on ds.${hiveconf:ppc_match_col}=h.ppc_match_col
  where lc.keyword_id<>0
  and lc.keyword_id<>''
  ;


  DROP TABLE IF EXISTS LC_merged_non_ppc;
  CREATE EXTERNAL TABLE LC_merged_non_ppc
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
  LOCATION '${hiveconf:s3location}/LC_merged_non_ppc/'
  ;
  
  INSERT OVERWRITE TABLE LC_merged_non_ppc
  
  
  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
  record_month,record_week,record_year
  FROM dc_na_filt_LC lc
  ${hiveconf:jointype} join hierarchy cat
  ON lc.page_id = cat.page_id
  where lc.keyword_id in (0,'')
  ;
  




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
  
  select * from (
  
  select * from LC_merged_ppc

  UNION ALL
  
  select * from LC_merged_non_ppc
  
  )a; 
  

  ;
  
  "
save(hiveMergeKeyword2, file="./data/hiveMergeKeyword2.RData")
  
  
  
  
  ###########################################
  ###### keyword and page matching ##########
  ###########################################  
  
  
  

  
  
  hiveMergeKeywordAndPage1<-"
  
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
  
  
  DROP TABLE IF EXISTS ppc_hierarchy;
  CREATE EXTERNAL TABLE ppc_hierarchy
  (
  Page_ID BIGINT,
  ppc_match_col STRING,
  channel STRING,
  sub_channel STRING)
  ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
  STORED AS TEXTFILE
  LOCATION '${hiveconf:s3location}/ppc_hierarchy_file/'
  tblproperties (\"skip.header.line.count\"=\"1\")
  ;
  
  
  DROP TABLE IF EXISTS dc_icm_merged_ppc;
  
  CREATE EXTERNAL TABLE dc_icm_merged_ppc
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_ppc/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_ppc
  select * from (	
  
  select
  i.*, coalesce(channel, 'Other'), coalesce(sub_channel, 'Other'), 
  rank () over (partition by user_id, sub_channel, time order by rand()) as rank
  
  from dc_icm i
  left join (select paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col} from dc_match_paid_search group by paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col}) ds
  on i.keyword_id=ds.paid_search_legacy_keyword_id
  ${hiveconf:jointype} join ppc_hierarchy h
  on ds.${hiveconf:ppc_match_col}=h.ppc_match_col
  and i.page_id=h.page_id
  where i.keyword_id<>0
  and i.keyword_id<>''
  ) a 
  where rank=1
  ;
  
  DROP TABLE IF EXISTS dc_icm_merged_non_ppc;
  
  CREATE EXTERNAL TABLE dc_icm_merged_non_ppc
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
  LOCATION '${hiveconf:s3location}/dc_icm_merged_non_ppc/'
  ;  
  
  INSERT OVERWRITE TABLE dc_icm_merged_non_ppc
  select * from (	
  
  select
  i.*, coalesce(channel, 'Other'), coalesce(sub_channel, 'Other'), 
  rank () over (partition by user_id, sub_channel, time order by rand()) as rank
  
  from dc_icm i
  ${hiveconf:jointype} join hierarchy h
  on i.page_id=h.page_id
  where i.keyword_id=0
  or keyword_id=''
  
  ) a 
  where rank=1
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
  
  select * from dc_icm_merged_ppc
  
  UNION ALL
  
  select * from dc_icm_merged_non_ppc
  
  )a; 
  
  
  
  "
  
  save(hiveMergeKeywordAndPage1, file="./data/hiveMergeKeywordAndPage1.RData")
  
  
  
  
  
  hiveMergeKeywordAndPage2<-"
  
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
  revenue BIGINT,
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
  
  
  DROP TABLE IF EXISTS LC_merged_ppc;
  CREATE EXTERNAL TABLE LC_merged_ppc
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
  LOCATION '${hiveconf:s3location}/LC_merged_ppc/'
  ;
  
  INSERT OVERWRITE TABLE LC_merged_ppc
  
  
  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
  record_month,record_week,record_year
  FROM dc_na_filt_LC lc
  left join (select paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col} from dc_match_paid_search group by paid_search_legacy_keyword_id, ${hiveconf:ppc_match_col}) ds
  on lc.keyword_id=ds.paid_search_legacy_keyword_id
  ${hiveconf:jointype} join ppc_hierarchy h
  on ds.${hiveconf:ppc_match_col}=h.ppc_match_col
  and lc.page_id=h.page_id
  where lc.keyword_id<>0
  and lc.keyword_id<>''
  ;
  
  
  DROP TABLE IF EXISTS LC_merged_non_ppc;
  CREATE EXTERNAL TABLE LC_merged_non_ppc
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
  LOCATION '${hiveconf:s3location}/LC_merged_non_ppc/'
  ;
  
  INSERT OVERWRITE TABLE LC_merged_non_ppc
  
  
  SELECT user_id, lc.time, coalesce(channel,'Other') as channel, coalesce(sub_channel,'Other') as sub_channel,
  record_month,record_week,record_year
  FROM dc_na_filt_LC lc
  ${hiveconf:jointype} join hierarchy cat
  ON lc.page_id = cat.page_id
  where lc.keyword_id in (0,'')
  ;
  
  
  
  
  
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
  
  select * from (
  
  select * from LC_merged_ppc
  
  UNION ALL
  
  select * from LC_merged_non_ppc
  
  )a; 
  
  
  ;
  
  "
  save(hiveMergeKeywordAndPage2, file="./data/hiveMergeKeywordAndPage2.RData")  
  
  
  
  
  
  




