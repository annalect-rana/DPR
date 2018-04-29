hivePSADEIT<-"

drop table if exists dc_na;

CREATE EXTERNAL TABLE IF NOT EXISTS dc_na
(
  Time	STRING,
  User_ID	STRING,
  Advertiser_ID	BIGINT,
  Buy_ID	BIGINT,
  Ad_ID	BIGINT,
  Creative_ID	BIGINT,
  Creative_Version	BIGINT,
  Creative_Size_ID	STRING,
  Site_ID	BIGINT,
  Page_ID	BIGINT,
  Keyword	STRING,
  Country_ID	BIGINT,
  Areacode	BIGINT,
  Browser_ID	BIGINT,
  Browser_Ver	BIGINT,
  OS_ID	BIGINT,
  Activity_Type	STRING,
  Activity_Sub_Type	STRING,
  Quantity	BIGINT,
  Revenue	BIGINT,
  Transaction_ID	STRING,
  Other_Data	STRING,
  Ordinal	STRING,
  Click_Time	STRING,
  Event_ID	BIGINT,
  keyword_id	STRING,
  Batch_Date	STRING,
  
  record_date STRING,
  record_week BIGINT,
  record_month BIGINT,
  record_year BIGINT,
  month_p BIGINT
  
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS textfile
LOCATION '/tmp/data/dc_na/'

;


INSERT OVERWRITE TABLE dc_na
select

from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'),
User_ID,
Advertiser_ID,
Buy_ID,
Ad_ID,
Creative_ID,
Creative_Version,
Creative_Size_ID,
Site_ID,
Page_ID,
Keyword,
Country_ID,
Areacode,
Browser_ID,
Browser_Ver,
OS_ID,
Activity_Type,
Activity_Sub_Type,
Quantity,
Revenue,
Transaction_ID,
Other_Data,
Ordinal,
Click_Time,
Event_ID,
keyword_id,
Batch_Date,

to_date(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))),
cast(weekofyear(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))) as BIGINT),
month(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
concat(year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'MM'))

from dc_na_raw
where User_ID<>'0'
and Advertiser_ID is not NULL
;   




drop table if exists dc_c;

CREATE EXTERNAL TABLE IF NOT EXISTS dc_c
(
  Time	STRING,
  User_ID	STRING,
  Advertiser_ID	BIGINT,
  Buy_ID	BIGINT,
  Ad_ID	BIGINT,
  Creative_ID	BIGINT,
  Creative_Version	BIGINT,
  Creative_Size_ID	BIGINT,
  Site_ID	BIGINT,
  Page_ID	BIGINT,
  Keyword	BIGINT,
  Country_ID	BIGINT,
  Areacode	BIGINT,
  Browser_ID	BIGINT,
  Browser_Ver	BIGINT,
  OS_ID	BIGINT,
  City_ID	BIGINT,
  Zip_Code	STRING,
  keyword_id	STRING,
  Batch_Date	STRING,
  
  record_date STRING,
  record_week BIGINT,
  record_month BIGINT,
  record_year BIGINT,
  month_p BIGINT
  
  
)

ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS textfile
LOCATION '/tmp/data/dc_c/'
;

INSERT OVERWRITE TABLE dc_c 
select

from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'),
User_ID,
Advertiser_ID,
Buy_ID,
Ad_ID,
Creative_ID,
Creative_Version,
Creative_Size_ID,
Site_ID,
Page_ID,
Keyword,
Country_ID,
Areacode,
Browser_ID,
Browser_Ver,
OS_ID,
City_ID,
Zip_Code,
keyword_id,
Batch_Date,

to_date(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))),
cast(weekofyear(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))) as BIGINT),
month(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
concat(year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'MM'))

from dc_c_raw
where user_id<>'0'
and Advertiser_ID is not NULL;   



drop table if exists dc_i;

CREATE EXTERNAL TABLE IF NOT EXISTS dc_i
(
  Time	STRING,
  User_ID	STRING,
  Advertiser_ID	BIGINT,
  Buy_ID	BIGINT,
  Ad_ID	BIGINT,
  Creative_ID	BIGINT,
  Creative_Version	BIGINT,
  Creative_Size_ID	BIGINT,
  Site_ID	BIGINT,
  Page_ID	BIGINT,
  Keyword	BIGINT,
  Country_ID	BIGINT,
  Areacode	BIGINT,
  Browser_ID	BIGINT,
  Browser_Ver	BIGINT,
  OS_ID	BIGINT,
  City_ID	BIGINT,
  ZIP_Code	STRING,
  Batch_Date	STRING,
  
  record_date STRING,
  record_week BIGINT,
  record_month BIGINT,
  record_year BIGINT,
  month_p BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\\t'
STORED AS textfile
LOCATION '/tmp/data/dc_i/'

;


INSERT OVERWRITE TABLE dc_i
select

from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'yyyy-MM-dd HH:mm:ss'),
User_ID,
Advertiser_ID,
Buy_ID,
Ad_ID,
Creative_ID,
Creative_Version,
Creative_Size_ID,
Site_ID,
Page_ID,
Keyword,
Country_ID,
Areacode,
Browser_ID,
Browser_Ver,
OS_ID,
City_ID,
ZIP_Code,
Batch_Date,

to_date(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))),
cast(weekofyear(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))) as BIGINT),
month(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), 
concat(year(from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'))), from_unixtime(unix_timestamp(time, 'MM-dd-yyyy-HH:mm:ss'), 'MM'))

from dc_i_raw
where User_ID<>'0'
and Advertiser_ID is not NULL;       




"


save(hivePSADEIT, file="./data/hivePSADEIT.RData")