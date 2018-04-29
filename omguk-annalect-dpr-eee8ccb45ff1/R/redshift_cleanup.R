
#' Deletes tables on Redshift created from the red_function() and red_gold_function()
#'
#' 
#'
#' @param username The username for redshift
#' @param pw The password for that username
#' @param dbname Database name, defaults to "annalectuk"
#' @param dbHost Database host, defaults to "54.77.118.78"
#' @param DPID Unique identifier for the DP run you wish to delete tables for
#' @param agency What agency are you? eg "m2m", "omd", "mg", "phd"
#' @param networkID The network ID
#' @examples #redshift_cleanup(username="tim_scholtes", pw="", DPID="testrun", agency="m2m")


date_code <- "170813"
analyst_code <- 1

username="menadbadmin"
pw="P0rt@Lm3na"
dbname="ds_annalect"
dbHost="menaportal-emea-dev.ctr41hpvtdsv.eu-west-1.rds.amazonaws.com"
agency="annalect"
port <-3306
library(RMySQL)
library(DBI)
drv <- dbDriver("MySQL")
redshift_con <- dbConnect(drv, dbname, user=username, password = pw,
                     host = dbHost, port = port)

redshift_cleanup<-function(username, pw, dbname="annalectuk", dbHost="54.77.118.78", DPID, agency){
  
  port <-"5439"
  drv <- dbDriver("PostgreSQL")
  redshift_con <- dbConnect(drv, dbname = dbname, user = username, password = pw, 
                            host = dbHost, port = port)
  
  dbSendQuery(redshift_con,paste0(
    "drop table if exists ds_",agency,".",username,"_",DPID,"_audience;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_c;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_category;",                                 
    "drop table if exists ds_",agency,".",username,"_",DPID,"_channel_lookup;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_city_distr;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_click_time_distr;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_event_by_date;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_i;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_icm;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_icm_20;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_icm_conv;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_icm_merged;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_icm_merged2;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_imp_time_distr;",    
    "drop table if exists ds_",agency,".",username,"_",DPID,"_lc_merged;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_na;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_na_filt;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_na_filt_browser;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_na_filt_lc;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_os_distr;",
    "drop table if exists ds_",agency,".",username,"_",DPID,"_unq_non_cons;",
	"drop table if exists ds_",agency,".",username,"_",DPID,"_pre_na_uids;",
	"drop table if exists ds_",agency,".",username,"_",DPID,"_icm_transpose;",
  
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_audience;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_c;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_category;",                                 
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_channel_lookup;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_city_distr;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_click_time_distr;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_event_by_date;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_i;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm_20;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm_conv;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm_merged;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm_merged2;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_imp_time_distr;",    
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_lc_merged;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_na;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_na_filt;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_na_filt_browser;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_na_filt_lc;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_os_distr;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_unq_non_cons;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_pre_na_uids;",
	"drop table if exists ds_",agency,".dc_",username,"_",DPID,"_icm_transpose;"
	))
  
}