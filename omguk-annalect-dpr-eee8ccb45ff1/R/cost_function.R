#' Creates the cost file
#' 
#' Automates the process to get the media_cost by day, channel and subchannel
#' 
#' @param username The username for redshift
#' @param pw The password for that username
#' @param dbname Database name, defaults to "reportinguk"
#' @param dbHost Database host, defaults to "54.171.118.113"
#' @param advertiserIds A character vector of advertiser ids to select
#' @param campaignIds A character vector of campaign ids to select
#' @param hierarchy Hierarchy dataframe 
#' @param startDate the start date of the DP in "YYYY-MM-DD" format
#' @param endDate the end date of the DP in "YYYY-MM-DD" format
#' 
#' @return Cost file containing media_cost, clicks and impressions by date, channel and subchannel 
#' 
#' @examples
#' 
#' cost_function(username="patricia_martin",pw="",networkId="5851",
#' advertiserIds= c(
#' "2326173",    "2326266",    "4304358",    "4705211",
#' "4705212",    "4705613",    "4705617",    "4705805",
#' "4705811",    "4705815",    "4705826"),
#' campaignIds=c(
#' "7941825",  "8574112",  "8561809",
#' "8515806",    "8590693",    "8551379",    "8684699",
#' "8612335",    "8516389",    "8522817",    "8591270",
#' "8565152",    "8514189",    "8590689",    "8462957"),
#' hierarchy = hierarchy_dataframe,
#' startDate="2015-04-01",endDate="2015-04-30") #not run
#' 

cost_function <- function(username, pw,dbname="reportinguk",dbHost="54.171.118.113", networkId,advertiserIds, campaignIds, hierarchy,startDate, endDate, allcosts = FALSE){ 
  
  
  # Check if missing inputs
  if(missing(username)) {
    stop("argument username is missing")}
  if(missing(pw)) {
    stop("argument pw is missing")}
  if(missing(networkId)) {
    stop("argument networkId is missing")}
  if(missing(advertiserIds)) {
    stop("argument advertiserIds is missing")}
  if(missing(campaignIds)) {
    stop("argument campaignIds is missing")}
  if(missing(startDate)) {
    stop("argument startDate is missing")}
  if(missing(endDate)) {
    stop("argument endDate is missing")}
  
  
  # DB control --------------------------------------------------------------
  #dbname <- "reportinguk"
  #dbHost <- "54.171.118.113"
  
  port = "5439"
  drv <- dbDriver("PostgreSQL")
  
  redshift_con <- dbConnect(drv, dbname = dbname, user = username, password = pw, 
                            host = dbHost, port = port)
  
  advertiserIds     <- paste0(advertiserIds,collapse=", ")
  campaignIds       <- paste0(campaignIds,collapse=", ")
  
  startDate <- as.Date(startDate)-1
  endDate   <- as.Date(endDate)+1
  
  cost_delivered_q <-  dbSendQuery(redshift_con, paste0("
                                                        SELECT 
                                                        date,
                                                        advertiser_id,
                                                        campaign_id,
                                                        placement_id,
                                                        costmethod,
                                                        rate,
                                                        sum(total_conversions) as conversions,
                                                        sum(clicks) as clicks,
                                                        sum(impressions) as impressions,
                                                        sum(prisma_planned_spend) as planned_spend,  
                                                        sum(prisma_reconciled_spend) as reconciled_spend,
                                                        CASE WHEN costmethod = 'CPA' THEN sum(rate*total_conversions)
                                                        WHEN costmethod = 'CPC' THEN sum(rate*clicks)
                                                        WHEN costmethod = 'CPM' THEN sum(rate*impressions/1000) 
                                                        ELSE sum(prisma_reconciled_spend)
                                                        END AS delivered_spend
                                                        FROM reporting_apollo.apollo_display
                                                        WHERE advertiser_id in (",advertiserIds,") 
                                                        AND network_id =",networkId," 
                                                        AND campaign_id in (",campaignIds,")
                                                        AND date > '", startDate,"'
                                                        AND date < '", endDate, "'
                                                        AND prisma_planned_spend IS NOT NULL 
                                                        AND costmethod NOT IN ('Flat', 'Free') 
                                                        AND costmethod IS NOT NULL
                                                        GROUP BY date, advertiser_id, campaign_id, placement_id, costmethod,rate
                                                        "))
  
  
  cost_delivered <- fetch(cost_delivered_q, n=-1)
  dbDisconnect(redshift_con)
  
  if(nrow(cost_delivered)==0){
    stop("You haven't pushed data to Prisma. No costs available")
    
  } else {
    
    data <- cost_delivered[, c("date", "campaign_id", "placement_id", "clicks", "impressions", "reconciled_spend", "delivered_spend")]
    data$media_cost <- ifelse(data$reconciled_spend==0,data$delivered_spend,data$reconciled_spend)
    
    # Subsetting 
    cost_data <- data[, c("date", "campaign_id", "placement_id", "clicks", "impressions", "media_cost")]
    
    # Merging both data frames
    media_cost_table <- merge(cost_data,hierarchy,all=F)
    
    # Subsetting
    media_cost_table <- media_cost_table[,c("date", "channel", "sub_channel", "clicks", "impressions", "media_cost")]
    media_cost_table[order(media_cost_table$date, decreasing = FALSE),]
    
    if(allcosts){
      allcosts_file <- cost_delivered[, c("date", "campaign_id", "placement_id", "clicks", "impressions", "planned_spend", "delivered_spend", "reconciled_spend")]
      return(list(mc = media_cost_table, ac = allcosts_file))
      
    } else {
      
      return(media_cost_table)
    }
    
  }
}
