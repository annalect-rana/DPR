
#' Takes the output from the blue function (recently, not old outputs!!) and reforms them into the new data required for the shiny dashboard
#'
#' Takes the output from the blue function (recently, not old outputs!!) and reforms them into the new data required for the shiny dashboard
#' Takes the output from the blue function (recently, not old outputs!!) and reforms them into the new data required for the shiny dashboard
#' 
#' @param input_data_dir This is where the blue_function outputs are stored. You MUST have had "extra_csv_outputs=TRUE" selected when you ran the blue function, or this will fail. You can check this by seeing if icm_conv is a dataset in this folder location
#' @param output_dir This is where you would like the outputs to be saved.
#' @param new_visitor_location If you have run new visitors analysis, this is where that dataset is saved (the full .csv location).
#' 
#' @return Returns nothing, but saves the data down to the specified location

#' @examples
#' 
#' #blue2dash(input_data_dir="Z:/OMD/DISNEY_DP/05.May2016/OUTPUT/",
#' #           output_dir = "Z:/GROUP/TIM/shiny/blue2dash_testing/")



blue2dash <- function(input_data_dir,output_dir,new_visitor_location=NULL) {

# tab1 has the totals
tab1 <- read.csv(paste0(input_data_dir,"/tab_1.csv"))

tab1_vars <- colnames(tab1)[-c(1:5,18,19)]

# tab2 is the time series
tab2 <- read.csv(paste0(input_data_dir,"/tab_2.csv"))
ts_data <- aggregate(cbind(weight,impressions,clicks,media_cost,revenue)~date,
  data=subset(tab2,level=="c"),FUN=sum)
ts_data$date <- as.Date(ts_data$date)
ts_data <- xts(ts_data[,-1],ts_data$date)

##

tod_dow_heatmap_data <- read.csv(paste0(input_data_dir,"/tab_7.csv"))
tod_dow_heatmap_data <- subset(tod_dow_heatmap_data,hour_flag==1)
tod_dow_heatmap_data <- tod_dow_heatmap_data[,c("channel","conv_path","conversions","impressions","clicks","day_of_week","hour_of_day")]
print("here")
# we have to rbind on a data frame with all the combinations of day, hour, channel, conversion path,
# with 0 impressions and conversions. this is so that the data frames all have full span after dcast.
unique_chans <- unique(na.omit(tod_dow_heatmap_data$channel))
input_df <- data.frame(channel=rep(unique_chans,each=7*24*2),
                       conv_path=rep(c("NC","C"),
                                     7*length(unique_chans)*24),
                       conversions=0,
                       impressions=0,
                       clicks=0,
                       day_of_week=rep(rep(weekdays(as.Date("1990-01-01")+0:6,FALSE),each=24*2),
                                length(unique_chans)),
                       hour_of_day=rep(rep(0:23,each=2),
                                 7*length(unique_chans)))

tod_dow_heatmap_data <- rbind(tod_dow_heatmap_data,input_df)

tod_dow_heatmap_data$events <- tod_dow_heatmap_data$impressions+tod_dow_heatmap_data$clicks
events_by_chan <- aggregate(events~channel,FUN=sum,tod_dow_heatmap_data)
imps_by_chan <- aggregate(impressions~channel,FUN=sum,tod_dow_heatmap_data)
allowed_hmap_chans <- imps_by_chan$channel[which(imps_by_chan$impressions!=0)]


tod_dow_heatmap_data_convs <- dcast(tod_dow_heatmap_data,
      hour_of_day~day_of_week,
      fun.aggregate = sum,
      value.var = "conversions")

#assign hours to rownames
rownames(tod_dow_heatmap_data_convs)<-paste0(ifelse(nchar(as.character(tod_dow_heatmap_data_convs[,1]))==1,paste0("0", as.character(tod_dow_heatmap_data_convs[,1])),as.character(tod_dow_heatmap_data_convs[,1])), "00")
#reorder columns to days of week not alphabetical
tod_dow_heatmap_data_convs<-tod_dow_heatmap_data_convs[c(3,7,8,6,2,4,5)]




pathway <- read.csv(paste0(input_data_dir,"/tab_4.csv"))
pathway$conv_rate <- pathway$conversions/pathway$uniques
pathway$channel <- as.character(pathway$channel)
pathway$channel[is.na(pathway$channel)] <- "ALL"
pathway <- split(pathway,pathway$channel)

pathway <- lapply(pathway,function(x) {
  x$cuml_conv <- cumsum(x$conversions)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
})

#sunburstData <- read.csv("www/visit-sequences.csv",
#  header = F,stringsAsFactors=F)

channel_pathways <- read.csv(paste0(input_data_dir,"/Channel_Pathways.csv"))
subchannel_pathways <- read.csv(paste0(input_data_dir,"/Sub_Channel_Pathways.csv"))

icm_conv <- read.csv(paste0(input_data_dir,"/icM_conv.csv"))
chan_subchan_lkup <- unique(icm_conv[c("channel","sub_channel")])
sub_channels <- unique(as.character(icm_conv$sub_channel))

lc_ra_matrix <- dcast(data=icm_conv,
           formula=channel~lc_channel,
           fun.aggregate = sum,
           value.var = "weight")

rownames(lc_ra_matrix) <- lc_ra_matrix$channel
lc_ra_matrix <- lc_ra_matrix[,-1]

#lc_ra_matrix <- sweep(lc_ra_matrix,2,
#  colSums(lc_ra_matrix),"/")


lc_ra_matrix_sub <- dcast(data=icm_conv,
           formula=sub_channel~lc_sub_channel,
           fun.aggregate = sum,
           value.var = "weight")

rownames(lc_ra_matrix_sub) <- lc_ra_matrix_sub$sub_channel
lc_ra_matrix_sub <- lc_ra_matrix_sub[,-1]

###
###



tab1_sub <- subset(tab1,level=="1_s")
tab1$channel <- as.character(tab1$channel)
tab1_channels <- unique(tab1$channel)


ra_lc_figures <- aggregate(cbind(weight,lc)~channel+sub_channel,tab1_sub,FUN=sum)

ra_lc_figures$RA_gain_loss <- ra_lc_figures$weight/ra_lc_figures$lc-1


icM_20 <- read.csv(paste0(input_data_dir,"/icM_20.csv"))
channels <- unique(icM_20$channel)
icM_20dt <- as.data.table(icM_20)
#visit_sequences1 <- icM_20dt[,list(path=paste(paste_dash_dedupe(channel),
#                                              "end",sep="-")),by=user_id]
#visit_sequences <- as.data.frame(visit_sequences1[,list(count=.N),by=path])

### do transpose of icm_20 at channel level:

icM_20dt_imp <- icM_20dt
icM_20dt_imp$date  <- as.Date(icM_20dt_imp$time)
icM_20dt_imp$week  <- ISOweek(icM_20dt_imp$date)
icM_20dt_imp$month <- month(icM_20dt_imp$date)

setkey(icM_20dt_imp,user_id,channel)
count_by_chan <- icM_20dt_imp[,list(channel_length=.N),by=key(icM_20dt_imp)]
setkey(icM_20dt_imp,user_id,channel,sub_channel)
count_by_sub <- icM_20dt_imp[,list(sub_channel_length=.N),by=key(icM_20dt_imp)]


setkey(icM_20dt_imp,user_id,channel,month)
count_by_chan_month <- icM_20dt_imp[,list(channel_length=.N),
                                by=key(icM_20dt_imp)]
setkey(count_by_chan_month,user_id,channel)
count_by_chan_month_max <- count_by_chan_month[,list(channel_length=max(channel_length)),
                                           by=key(count_by_chan_month)]

setkey(icM_20dt_imp,user_id,channel,week)
count_by_chan_week <- icM_20dt_imp[,list(channel_length=.N),
                               by=key(icM_20dt_imp)]
setkey(count_by_chan_week,user_id,channel)
count_by_chan_week_max <- count_by_chan_week[,list(channel_length=max(channel_length)),
                                         by=key(count_by_chan_week)]

setkey(icM_20dt_imp,user_id,channel,date)
count_by_chan_day <- icM_20dt_imp[,list(channel_length=.N),
                               by=key(icM_20dt_imp)]
setkey(count_by_chan_day,user_id,channel)
count_by_chan_day_max <- count_by_chan_day[,list(channel_length=max(channel_length)),
                                         by=key(count_by_chan_day)]


stored_files <- list.files(input_data_dir)
#load(paste0(input_data_dir,"/",stored_files[grep("convers.RData",
#                                                 stored_files)]))
convers <- icm_conv$user_id


# overall
count_by_chan$conv <- count_by_chan$user_id %in% convers
  
unique_count <- aggregate(user_id~channel+channel_length,
               count_by_chan,
          FUN=length)
conversion_sum <- aggregate(conv~channel+channel_length,
               count_by_chan,
               FUN=sum)
pathway_inchannel <- merge(unique_count,conversion_sum)
colnames(pathway_inchannel) <- c("channel","channel_length","uniques","conv")

pathway_inchannel <- pathway_inchannel[order(pathway_inchannel$channel,
                                             pathway_inchannel$channel_length),]
pathway_inchannel$conv_rate <- pathway_inchannel$conv/pathway_inchannel$uniques
pathway_inchannel <- split(pathway_inchannel,pathway_inchannel$channel)

pathway_inchannel <- do.call(rbind,lapply(pathway_inchannel,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

CPI <- subset(tab1,level=="1_c")
CPI$GrossCPI <- CPI$media_cost/(CPI$impressions+CPI$clicks)
CPI <- CPI[,c("channel","GrossCPI")]

pathway_inchannel <- merge(pathway_inchannel,CPI,all.x=T)
pathway_inchannel$Cost <- pathway_inchannel$uniques*
                          pathway_inchannel$channel_length*
                          pathway_inchannel$GrossCPI


### monthly
# overall
count_by_chan_month$conv <- count_by_chan_month$user_id %in% convers
count_by_chan_month_max$conv <- count_by_chan_month_max$user_id %in% convers

unique_count <- aggregate(user_id~channel+channel_length,
               count_by_chan_month,
               FUN=length)
conversion_sum <- aggregate(conv~channel+channel_length,
               count_by_chan_month_max,
               FUN=sum)
pathway_inchannel_month <- merge(unique_count,conversion_sum)
colnames(pathway_inchannel_month) <- c("channel","channel_length","uniques","conv")

pathway_inchannel_month <- pathway_inchannel_month[order(pathway_inchannel_month$channel,
                                             pathway_inchannel_month$channel_length),]
pathway_inchannel_month$conv_rate <- pathway_inchannel_month$conv/pathway_inchannel_month$uniques
pathway_inchannel_month <- split(pathway_inchannel_month,pathway_inchannel_month$channel)

pathway_inchannel_month <- do.call(rbind,lapply(pathway_inchannel_month,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_inchannel_month <- merge(pathway_inchannel_month,CPI,all.x=T)
pathway_inchannel_month$Cost <- pathway_inchannel_month$uniques*
                          pathway_inchannel_month$channel_length*
                          pathway_inchannel_month$GrossCPI

### weekly
# overall
count_by_chan_week$conv <- count_by_chan_week$user_id %in% convers
count_by_chan_week_max$conv <- count_by_chan_week_max$user_id %in% convers

unique_count <- aggregate(user_id~channel+channel_length,
               count_by_chan_week,
          FUN=length)
conversion_sum <- aggregate(conv~channel+channel_length,
               count_by_chan_week_max,
               FUN=sum)
pathway_inchannel_week <- merge(unique_count,conversion_sum)
colnames(pathway_inchannel_week) <- c("channel","channel_length","uniques","conv")

pathway_inchannel_week <- pathway_inchannel_week[order(pathway_inchannel_week$channel,
                                             pathway_inchannel_week$channel_length),]
pathway_inchannel_week$conv_rate <- pathway_inchannel_week$conv/pathway_inchannel_week$uniques
pathway_inchannel_week <- split(pathway_inchannel_week,pathway_inchannel_week$channel)

pathway_inchannel_week <- do.call(rbind,lapply(pathway_inchannel_week,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_inchannel_week <- merge(pathway_inchannel_week,CPI,all.x=T)
pathway_inchannel_week$Cost <- pathway_inchannel_week$uniques*
                          pathway_inchannel_week$channel_length*
                          pathway_inchannel_week$GrossCPI


### daily
# overall
count_by_chan_day$conv <- count_by_chan_day$user_id %in% convers
count_by_chan_day_max$conv <- count_by_chan_day_max$user_id %in% convers

unique_count <- aggregate(user_id~channel+channel_length,
               count_by_chan_day,
          FUN=length)
conversion_sum <- aggregate(conv~channel+channel_length,
               count_by_chan_day_max,
               FUN=sum)
pathway_inchannel_day <- merge(unique_count,conversion_sum)
colnames(pathway_inchannel_day) <- c("channel","channel_length","uniques","conv")

pathway_inchannel_day <- pathway_inchannel_day[order(pathway_inchannel_day$channel,
                                             pathway_inchannel_day$channel_length),]
pathway_inchannel_day$conv_rate <- pathway_inchannel_day$conv/pathway_inchannel_day$uniques
pathway_inchannel_day <- split(pathway_inchannel_day,pathway_inchannel_day$channel)

pathway_inchannel_day <- do.call(rbind,lapply(pathway_inchannel_day,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_inchannel_day <- merge(pathway_inchannel_day,CPI,all.x=T)
pathway_inchannel_day$Cost <- pathway_inchannel_day$uniques*
                          pathway_inchannel_day$channel_length*
                          pathway_inchannel_day$GrossCPI

##################################################################
# AS ABOVE, BUT FOR SUB CHANNELS #
##################################################################


setkey(icM_20dt_imp,user_id,channel,sub_channel)
count_by_sub <- icM_20dt_imp[,list(sub_channel_length=.N),by=key(icM_20dt_imp)]


setkey(icM_20dt_imp,user_id,sub_channel,month)
count_by_sub_month <- icM_20dt_imp[,list(sub_channel_length=.N),
                                    by=key(icM_20dt_imp)]
setkey(count_by_sub_month,user_id,sub_channel)
count_by_sub_month <- count_by_sub_month[,list(sub_channel_length=max(sub_channel_length)),
                                           by=key(count_by_sub_month)]

setkey(icM_20dt_imp,user_id,sub_channel,week)
count_by_sub_week <- icM_20dt_imp[,list(sub_channel_length=.N),
                                   by=key(icM_20dt_imp)]
setkey(count_by_sub_week,user_id,sub_channel)
count_by_sub_week <- count_by_sub_week[,list(sub_channel_length=max(sub_channel_length)),
                                         by=key(count_by_sub_week)]

setkey(icM_20dt_imp,user_id,sub_channel,date)
count_by_sub_day <- icM_20dt_imp[,list(sub_channel_length=.N),
                                  by=key(icM_20dt_imp)]
setkey(count_by_sub_day,user_id,sub_channel)
count_by_sub_day <- count_by_sub_day[,list(sub_channel_length=max(sub_channel_length)),
                                       by=key(count_by_sub_day)]



# overall
count_by_sub$conv <- count_by_sub$user_id %in% convers

unique_count <- aggregate(user_id~sub_channel+sub_channel_length,
                          count_by_sub,
                          FUN=length)
conversion_sum <- aggregate(conv~sub_channel+sub_channel_length,
                            count_by_sub,
                            FUN=sum)
pathway_in_subchan <- merge(unique_count,conversion_sum)
colnames(pathway_in_subchan) <- c("sub_channel","sub_channel_length","uniques","conv")

pathway_in_subchan <- pathway_in_subchan[order(pathway_in_subchan$sub_channel,
                                             pathway_in_subchan$sub_channel_length),]
pathway_in_subchan$conv_rate <- pathway_in_subchan$conv/pathway_in_subchan$uniques
pathway_in_subchan <- split(pathway_in_subchan,pathway_in_subchan$sub_channel)

pathway_in_subchan <- do.call(rbind,lapply(pathway_in_subchan,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

CPI <- subset(tab1,level=="1_s")
CPI$GrossCPI <- CPI$media_cost/(CPI$impressions+CPI$clicks)
CPI <- CPI[,c("sub_channel","GrossCPI")]

pathway_in_subchan <- merge(pathway_in_subchan,CPI,all.x=T)
pathway_in_subchan$Cost <- pathway_in_subchan$uniques*
  pathway_in_subchan$sub_channel_length*
  pathway_in_subchan$GrossCPI


### monthly
# overall
count_by_sub_month$conv <- count_by_sub_month$user_id %in% convers

unique_count <- aggregate(user_id~sub_channel+sub_channel_length,
                          count_by_sub_month,
                          FUN=length)
conversion_sum <- aggregate(conv~sub_channel+sub_channel_length,
                            count_by_sub_month,
                            FUN=sum)
pathway_in_subchan_month <- merge(unique_count,conversion_sum)
colnames(pathway_in_subchan_month) <- c("sub_channel","sub_channel_length","uniques","conv")

pathway_in_subchan_month <- pathway_in_subchan_month[order(pathway_in_subchan_month$sub_channel,
                                                         pathway_in_subchan_month$sub_channel_length),]
pathway_in_subchan_month$conv_rate <- pathway_in_subchan_month$conv/pathway_in_subchan_month$uniques
pathway_in_subchan_month <- split(pathway_in_subchan_month,pathway_in_subchan_month$sub_channel)

pathway_in_subchan_month <- do.call(rbind,lapply(pathway_in_subchan_month,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_in_subchan_month <- merge(pathway_in_subchan_month,CPI,all.x=T)
pathway_in_subchan_month$Cost <- pathway_in_subchan_month$uniques*
  pathway_in_subchan_month$sub_channel_length*
  pathway_in_subchan_month$GrossCPI

### weekly
# overall
count_by_sub_week$conv <- count_by_sub_week$user_id %in% convers

unique_count <- aggregate(user_id~sub_channel+sub_channel_length,
                          count_by_sub_week,
                          FUN=length)
conversion_sum <- aggregate(conv~sub_channel+sub_channel_length,
                            count_by_sub_week,
                            FUN=sum)
pathway_in_subchan_week <- merge(unique_count,conversion_sum)
colnames(pathway_in_subchan_week) <- c("sub_channel","sub_channel_length","uniques","conv")

pathway_in_subchan_week <- pathway_in_subchan_week[order(pathway_in_subchan_week$sub_channel,
                                                       pathway_in_subchan_week$sub_channel_length),]
pathway_in_subchan_week$conv_rate <- pathway_in_subchan_week$conv/pathway_in_subchan_week$uniques
pathway_in_subchan_week <- split(pathway_in_subchan_week,pathway_in_subchan_week$sub_channel)

pathway_in_subchan_week <- do.call(rbind,lapply(pathway_in_subchan_week,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_in_subchan_week <- merge(pathway_in_subchan_week,CPI,all.x=T)
pathway_in_subchan_week$Cost <- pathway_in_subchan_week$uniques*
  pathway_in_subchan_week$sub_channel_length*
  pathway_in_subchan_week$GrossCPI


### daily
# overall
count_by_sub_day$conv <- count_by_sub_day$user_id %in% convers

unique_count <- aggregate(user_id~sub_channel+sub_channel_length,
                          count_by_sub_day,
                          FUN=length)
conversion_sum <- aggregate(conv~sub_channel+sub_channel_length,
                            count_by_sub_day,
                            FUN=sum)
pathway_in_subchan_day <- merge(unique_count,conversion_sum)
colnames(pathway_in_subchan_day) <- c("sub_channel","sub_channel_length","uniques","conv")

pathway_in_subchan_day <- pathway_in_subchan_day[order(pathway_in_subchan_day$sub_channel,
                                                     pathway_in_subchan_day$sub_channel_length),]
pathway_in_subchan_day$conv_rate <- pathway_in_subchan_day$conv/pathway_in_subchan_day$uniques
pathway_in_subchan_day <- split(pathway_in_subchan_day,pathway_in_subchan_day$sub_channel)

pathway_in_subchan_day <- do.call(rbind,lapply(pathway_in_subchan_day,function(x) {
  x$cuml_conv <- cumsum(x$conv)
  x$cuml_uniques <- cumsum(x$uniques)
  x$cuml_conv_rate <- x$cuml_conv/x$cuml_uniques
  x
}))

pathway_in_subchan_day <- merge(pathway_in_subchan_day,CPI,all.x=T)
pathway_in_subchan_day$Cost <- pathway_in_subchan_day$uniques*
  pathway_in_subchan_day$sub_channel_length*
  pathway_in_subchan_day$GrossCPI

###################################################################
###################################################################


#### FUNNEL path presence

funnel <- icM_20[c("user_id", "channel", "sub_channel", "time")]

# Number the touchpoints sequentially in order of descending time by user_id
funnel <- data.table(funnel)
setkey(funnel,user_id,time)
setkey(funnel,user_id)
funnel <- funnel[, id := seq_len(.N), by = user_id]

# Add total_touchpoints by user_id
funnel <- funnel[,total_touchpoints := max(id),by=user_id]

# Identify first event, last event, mid pathway
funnel$funnel_position<-ifelse(funnel$id==funnel$total_touchpoints,funnel$funnel_position<-"3. Last event",
                              ifelse(funnel$id==1,funnel$funnel_position <- "1. First event",
                                     funnel$funnel_position<-"2. Mid pathway"))

# Count funnel position by channel & sub-channel

funnel_chans <- as.data.frame(dcast(funnel,channel~funnel_position,length))
funnel_subs <- as.data.frame(dcast(funnel,sub_channel~funnel_position,length))
funnel_subs$Channel <- chan_subchan_lkup$channel[match(funnel_subs$sub_channel,chan_subchan_lkup$sub_channel)]

funnel_chans$Total <- rowSums(funnel_chans[,-1])
funnel_subs$Total <- rowSums(funnel_subs[,-c(1,5)])


funnel_chans[,-c(1,5)]     <- sweep(funnel_chans[,-c(1,5)],
  1,rowMeans(funnel_chans[,-c(1,5)]),"/")*100
funnel_subs[,-c(1,5,6)] <- sweep(funnel_subs[,-c(1,5,6)],
  1,rowMeans(funnel_subs[,-c(1,5,6)]),"/")*100

#### now get a version of funnel chans/subs but by time, for some boxplots.
# 
 if(!"T2C" %in% colnames(icm_conv)) {
   icm_conv$time <- as.POSIXct(icm_conv$time)
   icm_conv$conv_time <- as.POSIXct(icm_conv$conv_time)
   icm_conv$T2C <- as.numeric(icm_conv$conv_time-icm_conv$time)/(24*3600)
 }
T2C_quantiles_chan <- do.call(rbind,lapply(split(icm_conv$T2C,icm_conv$channel),function(x) {
  data.frame(
    y0 = min(x),
    y25 = quantile(x, 0.25),
    y50 = median(x),
    y75 = quantile(x, 0.75),
    y100 = max(x)
  )
}))

T2C_quantiles_chan$channel <- rownames(T2C_quantiles_chan)

T2C_quantiles_subchan <- do.call(rbind,lapply(split(icm_conv$T2C,icm_conv$sub_channel),function(x) {
  data.frame(
    y0 = min(x),
    y25 = quantile(x, 0.25),
    y50 = median(x),
    y75 = quantile(x, 0.75),
    y100 = max(x)
  )
}))

T2C_quantiles_subchan$subchannel <- rownames(T2C_quantiles_subchan)


### unused right now:
# use this SQL code to generate a sales funnel
#SELECT * FROM annalectuk.ds_grp.dc_na_1707_mg 
#where advertiser_id in (1663727)LIMIT 100;
#select count(1),activity_sub_type,report_name from ( 
#SELECT a.activity_sub_type, b.report_name 
#FROM annalectuk.ds_grp.dc_na_1707_mg as a
#left join ds_grp.dc_match_activity_cat as b
#on a.activity_sub_type = b.activity_sub_type
#where advertiser_id in (1663727)
#) group by activity_sub_type, report_name
#;
###

### add in frequency data to tab 1

setkey(icM_20dt,user_id,channel)
count_by_chan2 <- icM_20dt[,list(channel_length=.N),by=key(icM_20dt)]
setkey(icM_20dt,user_id,channel,sub_channel)
count_by_sub2 <- icM_20dt[,list(sub_channel_length=.N),by=key(icM_20dt)]

count_by_chan2 <- setkey(count_by_chan2,channel)
count_by_sub2 <- setkey(count_by_sub2,channel,sub_channel)

count_by_chan2 <- count_by_chan2[,list(frequency=mean(channel_length)),by=key(count_by_chan2)]
count_by_sub2 <- count_by_sub2[,list(frequency=mean(sub_channel_length)),by=key(count_by_sub2)]

count_by_chan2$level <- "1_c"
count_by_sub2$level <- "1_s"
frequency_data <- rbind.fill(count_by_chan2,count_by_sub2)

tab1 <- merge(tab1,frequency_data,all.x=TRUE,by=c("channel","sub_channel","level"))

##### get the historical heatmaps of activity for each of the 24*7 combos of tod~dow, before for conversion events and after for impression events

#### FIRST GET back looking from conversion ("history_for_converters")
dow <- c("Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday")
hod <- 0:23

history_for_converters <- lapply(dow,function(x) {lapply(hod, function(x) {})})
names(history_for_converters) <- dow

for(d in dow) {
  for(h in hod) {
    print(d)
    print(h)
    X <- icm_conv[icm_conv$conv_day_name==d&icm_conv$conv_hour_of_day==h,]
    if(nrow(X)==0) {} else {
    # get rid of events more than 2 weeks prior
    X <- X[X$T2C<=7,]
    X$T2C <- floor(X$T2C*24)
    
    X <- X[,c("channel","T2C","user_id")]
    X_pad <- data.frame(channel=rep(as.character(unique(icm_conv$channel)),each=24*7),
                        T2C = 0:(24*7-1),
                        user_id=NA)
    X <- rbind(X,X_pad)
    X$T2C <- floor(X$T2C/4)*4
    X$T2C <- X$T2C/24

    selected_time <- as.POSIXct(paste0(as.character(as.Date("2000-01-03")+match(d,dow)-1)," ",
                           str_pad(h,width=2,side="left",pad="0"),":00"))
    back_time <- selected_time - seq(0,(7*24-4),4)*3600
    back_wday <- weekdays(back_time)
    back_hours <- hour(back_time)
    
    label <- paste(substr(back_wday,1,3),str_pad(back_hours,2,"left",pad="0"),":00")

    
    Y <- dcast(X,T2C~channel,fun.aggregate=length)
    Y[,-1] <- Y[,-1]-4
    Y$Total <- rowSums(Y[,-1])
    Y$label <- label
    Y <- Y[,-1]
    levs <- rev(unique(as.character(Y$label)))
    Y$label <- factor(Y$label,level=levs)
    
    history_for_converters[[d]][[h+1]] <- Y}
  }
}

### NOW get looking forward to conversion from an event by channel


future_for_events_conv <- lapply(dow,function(x) {lapply(hod, function(x) {})})
names(future_for_events_conv) <- dow

# identify the first event for each

#icm_conv$time <- as.POSIXct(strptime(as.character(icm_conv$time),"%d/%m/%Y %H:%M"))
icm_convdt <- as.data.table(icm_conv)

setkey(icm_convdt,user_id,time)
setkey(icm_convdt,user_id)

icm_convdt[,TFF := as.numeric(time-min(time))/3600,
                     by=key(icm_convdt)]
icm_convdt[,First_indicator := TFF==0,by=key(icm_convdt)]
icm_convdt[,c("hod","dow"):= list(hour(time),weekdays(time)),]
icm_convdf <- as.data.frame(icm_convdt)

for(d in dow) {
  for(h in hod) {
    print(d)
    print(h)
    #users <- unique(icM_convdf[icM_convdf$First_indicator&icM_convdf$hod==h&icM_convdf$dow==d,"user_id"])
    
    X <- icm_convdf[icm_convdf$hod==h&icm_convdf$dow==d,]
    X$T2C <- X$T2C *24
    # get rid of events more than 2 weeks prior
    X <- X[X$T2C<=7*24,]
    if(nrow(X)==0) {} else {

      X$T2C <- floor(X$T2C)
      X <- X[,c("channel","T2C","user_id")]
      X_pad <- data.frame(channel=rep(as.character(unique(icm_conv$channel)),each=24*7),
                          T2C = 0:(24*7-1),
                          user_id=NA)
      X <- rbind(X,X_pad)
      X$T2C <- floor(X$T2C/4)*4
      X$T2C <- X$T2C/24
      
      
      selected_time <- as.POSIXct(paste0(as.character(as.Date("2000-01-03")+match(d,dow)-1)," ",
                                         str_pad(h,width=2,side="left",pad="0"),":00"))
      back_time <- selected_time + seq(0,(7*24-4),4)*3600
      back_wday <- weekdays(back_time)
      back_hours <- hour(back_time)
      
      label <- paste(substr(back_wday,1,3),str_pad(back_hours,2,"left",pad="0"),":00")
      
      
      Y <- dcast(X,T2C~channel,fun.aggregate=length)
      Y[,-1] <- Y[,-1]-4
      Y$Total <- rowSums(Y[,-1])
      Y$label <- label
      Y <- Y[,-1]
      levs <- unique(as.character(Y$label))
      Y$label <- factor(Y$label,level=levs)
      
      future_for_events_conv[[d]][[h+1]] <- Y}
  }
}


##### overlaps
# for converters, sub channel
    Y <- apply(na.omit(subchannel_pathways[,4:25]),1,function(x) {
      a <- unique(x[-c(1:3)])
      if(any(a=="_")) {
        a <- a[-which(a=="_")]
      }
      if(length(a)==1) {
        b <- data.frame(chan1=a,chan2=a)
      }
      if(length(a)==2) {
        b <- data.frame(chan1=a,chan2=rev(a))
      }
      if(length(a)>2) {
        b <- as.data.frame(rbind(t(combn(a,m=2)),
                                 t(combn(rev(a),m=2))))
        names(b) <- c("chan1","chan2")
      }
        b$value <- as.numeric(as.character(x[1]))
      return(b)
    })
    Y <- do.call(rbind,Y)
overlap_matrix_sub1_conv <- Y


# for nonconverters, sub channel
    Y <- apply(na.omit(subchannel_pathways[,4:25]),1,function(x) {
      a <- unique(x[-c(1:3)])
      if(any(a=="_")) {
        a <- a[-which(a=="_")]
      }
      if(length(a)==1) {
        b <- data.frame(chan1=a,chan2=a)
      }
      if(length(a)==2) {
        b <- data.frame(chan1=a,chan2=rev(a))
      }
      if(length(a)>2) {
        b <- as.data.frame(rbind(t(combn(a,m=2)),
                                 t(combn(rev(a),m=2))))
        names(b) <- c("chan1","chan2")
      }
        b$value <- as.numeric(as.character(x[3]))
      return(b)
    })
    Y <- do.call(rbind,Y)
overlap_matrix_sub1_nonconv <- Y

if(is.null(new_visitor_location)) {
  print("writing data")
  save(tab1,
    tab1_sub,
    tab1_channels,
    ra_lc_figures,
    lc_ra_matrix_sub,
    lc_ra_matrix,
    channel_pathways,
    subchannel_pathways,
    channels,
    sub_channels,
    funnel_subs,
    funnel_chans,
    pathway,
    tod_dow_heatmap_data_convs,
    ts_data,
    tod_dow_heatmap_data,
    #visit_sequences,
    pathway_inchannel,
    pathway_inchannel_month,
    pathway_inchannel_week,
    pathway_inchannel_day,
    pathway_in_subchan,
    pathway_in_subchan_month,
    pathway_in_subchan_week,
    pathway_in_subchan_day,
    allowed_hmap_chans,
    future_for_events_conv,
    history_for_converters,
    T2C_quantiles_subchan,
    T2C_quantiles_chan,
    overlap_matrix_sub1_conv,
    overlap_matrix_sub1_nonconv,
    file=paste0(output_dir,"/start_data.RData")
    ) } else {

    tab1_new_visitors <- read.csv(new_visitor_location)
    print("writing data incl new visitors")
    save(tab1,
      tab1_sub,
      tab1_channels,
      tab1_new_visitors,
      ra_lc_figures,
      lc_ra_matrix_sub,
      lc_ra_matrix,
      channel_pathways,
      subchannel_pathways,
      channels,
      sub_channels,
      funnel_subs,
      funnel_chans,
      pathway,
      tod_dow_heatmap_data_convs,
      ts_data,
      tod_dow_heatmap_data,
      #visit_sequences,
      pathway_inchannel,
      pathway_inchannel_month,
      pathway_inchannel_week,
      pathway_inchannel_day,
      pathway_in_subchan,
      pathway_in_subchan_month,
      pathway_in_subchan_week,
      pathway_in_subchan_day,
      allowed_hmap_chans,
      future_for_events_conv,
      history_for_converters,
      T2C_quantiles_subchan,
      T2C_quantiles_chan,
      overlap_matrix_sub1_conv,
      overlap_matrix_sub1_nonconv,
      file=paste0(output_dir,"/start_data_new.RData"))
  }
}
