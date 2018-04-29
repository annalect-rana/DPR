#' Runs the blue process
#'

#' Runs the blue process locally on R
#'

#' @param username The username for redshift

#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param cost_save_location This is where the cost file is located, which must have columns channel, sub_channel,date,impressions,clicks,media_cost
#' @param output_dir This is where you would like the csv tabs for the dashboard to be output. If this is the same location as the tableau currently points then you will only need to refresh the dashboard to update. Do not have any of these files open while running.
#' @param click_cutoff The number of clicks that is counted as too few for a sub channel to be left on its own. Defaults to 5
#' @param imp_cutoff The number of imps that is counted as too few for a sub channel to be left on its own. Defaults to 250
#' @param revenue_per_conv The revenue for each conversion.
#' @param extra_csv_outputs Defaults to TRUE, asks if you want to write out as csv's some important intermediate files. For very large DP runs this may be undesirable as it can be slow for large files.
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' @examples
#' #blue_function(username="tim_scholtes",file_save_location="Z:/GROUP/DPR/TIM_test",
#' cost_file_location="Z:/GROUP/DPR/TIM_test",
#' output_dir="Z:/GROUP/DPR/TIM_test",
#' click_cutoff = 5,imp_cutoff=250,revenue_per_conv=10) # not run

blue_function_OMD <- function(username,DPID,file_save_location,cost_file_location,output_dir,
                          click_cutoff = 5,imp_cutoff=250,revenue_per_conv,extra_csv_outputs=TRUE) {
  start.time <- Sys.time()    
  analyst <- username
  message("Loading Files")

  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_aud_distr.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_uni_distr.RData")))
  #load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_click_distr.RData")))
  #load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_imp_distr.RData")))
  
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_event_dow_distr.RData")))
  
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_convers.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_icM_20.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_lc.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_lc_na.RData")))
  
  #load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_imp_distr.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_trans1M_20.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_na.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_chanLkup.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_aud_length.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_events.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_na_browser.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_os_distr.RData")))
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_city_distr.RData")))

  
  message("Deduping")
  #____Replace missing values with 0s___________________
  for(i in 1:length(trans1M_20)){
  idx <- which(is.na(trans1M_20[[i]]))
  if(!is.null(idx)){
      trans1M_20[[i]][idx] <- rep(0, length=length(idx))
      }
  };rm(idx) # use na.zero?
  
  #_____Remove duplicates_______________________________
  
  # melt and recast the final table to remove duplicates - now it's aggregated should be fast
  dup_ids <- unique(trans1M_20$user_id[which(duplicated(trans1M_20$user_id))])
  
  trans_melt <- melt(subset(trans1M_20,user_id %in% dup_ids),id.vars = "user_id") # this is still a bit slow - use data.table
  trans1M_20_clean <- subset(trans1M_20,!user_id %in% dup_ids)
  trans1M_20_deduped <- dcast(trans_melt,user_id~variable,fun.aggregate = sum)
  trans1M_20 <- rbind(trans1M_20_clean,trans1M_20_deduped)
  
  #Add conversion column
  trans1M_20$conv <- as.integer(trans1M_20$user_id %in% convers)
  
  #___Remove__single__value__columns____________NEEDS_IMPROVEMENT____________ 
  ## is this necessary??? # also use .subset2(x,i)
  tEmptyColInd <- NULL
  for(i in 2:(length(trans1M_20)-1) ){
  if( luna(trans1M_20[[i]]) == 1 ){
    tEmptyColInd = c(tEmptyColInd, i)
    print(paste('Column ', i, 'contains only ', unique(trans1M_20[[i]])))
  }
  }
  
  rm(tEmptyColInd)
  message("Prepping for small data")
  #__PREP_FOR_SMALL_DATA_______start____
  message('Plan is to split the (non-combo) trans1M_20 to two DFs, one for impressions and one for clicks...')
  message('...and then apply the below unaltered to each')
  
  M_red_1 <- trans1M_20[c(1,
                          which(substr(names(trans1M_20),
                                       nchar(names(trans1M_20))-1,
                                       nchar(names(trans1M_20))) == '_1'),
                          length(trans1M_20))]
  
  M_red_2 <- trans1M_20[c(1,
                          which(substr(names(trans1M_20),
                                       nchar(names(trans1M_20))-1,
                                       nchar(names(trans1M_20))) == '_2'),
                          length(trans1M_20))]
  
  ### check for converting channels:
  M_red_1_conv <- subset(M_red_1,conv==1) ## watchout for environment clashes
  M_red_2_conv <- subset(M_red_2,conv==1)
  ### redo to reference column name?
  keepers_1 <- names(which(colSums(M_red_1_conv[,-c(1,ncol(M_red_1_conv))])!=0))
  keepers_2 <- names(which(colSums(M_red_2_conv[,-c(1,ncol(M_red_2_conv))])!=0))
  
  
  #___________________DEAL_WITH_SMALL_DATA__________start_____________________________
  #_1._Clicks______________
  
  cutoff <- click_cutoff
  
  # arbitrary 'small data' limit. 
  #NOTE: It may be worth for cutoff to be a 
  #function of the number of converters or the
  #total uniques within M_red_1.
  if(length(keepers_1)>0) {
  subChInd_small_1 <- names(which(
    colSums(M_red_1[,keepers_1]>0) < cutoff))
  } else subChInd_small_1 <- NULL
  # index of subChannels within M_red_1 that have less than 'cutoff'
  #non-zero entries. The names of the entries are the subchannel names.
  
  if(length(subChInd_small_1) > 0){
    message(paste('There are ', length(subChInd_small_1), ' small click sub-channels'))
    subChAggr_1 <- rowSums(M_red_1[, subChInd_small_1,drop=F]) 
    M_red_1 <- M_red_1[which(!names(M_red_1) %in% subChInd_small_1)] 
    M_red_1$subChAggr_1 <- subChAggr_1
  
    # Adds the merged subCh ('s)
    # if(length(which(M_red_1$subChAggr_1 > 0)) < cutoff){
    #   M_red_1 <- M_red_1[which(!names(M_red_1) == 'subChAggr_1')]
    #   message('Sub_Channel aggregator too frail to survive')
    # }
  }
  
  M_red_1 <- M_red_1[,c(which(!(names(M_red_1) %in% 'conv')), 
                       which(names(M_red_1) == 'conv'))] 
  #_2.__________________________Imps__________________________________________
  
  cutoff <- imp_cutoff
  
  if(length(keepers_2)>0) {
    subChInd_small_2 <- names(which(
      colSums(M_red_2[,keepers_2]>0) < cutoff))
  } else subChInd_small_2 <- NULL
  # index of subChannels within M_red_2 that have less than 'cutoff' 
  #non-zero entries. The names of the entries are the subchannel names.
  
  if(length(subChInd_small_2) > 0){
      message(paste('There are ', length(subChInd_small_2), ' small imp sub-channels'))
    subChAggr_2 <- rowSums(M_red_2[, subChInd_small_2,drop=F])
    M_red_2 <- M_red_2[which(!names(M_red_2) %in% subChInd_small_2)]
    M_red_2$subChAggr_2 <- subChAggr_2 
    
    # names(M_red_2)[length(M_red_2)] <- 'subChAggr_2'
    #   if(length(which(M_red_2$subChAggr_2 > 0)) < cutoff){
    #     M_red_2 <- M_red_2[which(!names(M_red_2) == 'subChAggr_2')]
    #     message('Sub_Channel aggregator too frail to survive')
    # }
  }
  
  M_red_2 <- M_red_2[,c(which(!(names(M_red_2) %in% 'conv')), 
                       which(names(M_red_2) == 'conv'))] 
  
  #__Stitch_the_matrices_back:
  M_red <- cbind(M_red_1, M_red_2[2:(length(M_red_2)-1)])
  M_red <- M_red[,c(which(!(names(M_red) %in% 'conv')), 
                   which(names(M_red) == 'conv'))]
  
  names(M_red) = gsub('\\-', '_',
                      gsub('\\+', '', 
                           gsub(' ', '_', names(M_red))))
  ## limit M_red to be 100 events max
  
  M_red_num <- as.matrix(M_red[,-1])
  M_red_num[M_red_num>100] <- 100
  
  M_red[,-1] <- M_red_num # check what this is used for
  
  message("Running the model")
  
  # _____________  MODEL _________________________________________
  message('_______MODEL KICK-OFF_________') # all data (raw)
  form <- as.formula(paste("conv ~", paste(names(M_red)[!(names(M_red) %in% c("user_id", "conv"))], collapse = " + ")))
  
  model <- mle_logreg_constrained(fmla = form,
                                  data = M_red)
  
  coef <- model$beta
  
  
  #rm(form, modM_st)
  
  
  #__2._Linearisation_________________
  trans1M_20_conv <- M_red[which(M_red$conv == 1), ] # do on convs or non convs
  f = function(x){exp(x)/(1+exp(x))}
  
  #coef[-1] <- pmax(coef[-1],0.00000000000001)
  
  M_redCoef <- sweep(trans1M_20_conv[,-which(colnames(trans1M_20_conv)
                                                 %in% c("user_id","conv"))],
                         2,coef[-1],"*")
  
  # sum(f(rowSums(M_redCoef)+coef[1])) this is what you need for including non converters
  
  
  #########
  total_vec <- rowSums(M_redCoef)
  total_vec[which(total_vec > 100 )] = 100 # check this
  total_vec <- total_vec + coef[1]
  prob_vec <- f(total_vec) 
  s_tmp <- NULL
  e_tmp <- NULL
  d_tmp <- NULL
  
  for(i in 1:length(M_redCoef)){
    p_tmp <- M_redCoef[, i]
    s_tmp <- as.data.frame(cbind(s_tmp, total_vec - p_tmp))
    e_tmp <- as.data.frame(cbind(e_tmp, f(s_tmp[, i])))
    d_tmp <- as.data.frame(cbind(d_tmp, prob_vec - e_tmp[, i] ))
  }
  
  ###### start here again
  
  # thinka bout this one again to explain it better
  zeroInd <- which(rowSums(d_tmp) == 0)
  if(length(zeroInd) > 0){
    d_tmp[zeroInd, ] <- M_redCoef[zeroInd, ]
  # precision loss: replaces probability difference with coefficient difference for a few lines
  }
  
  
  w_df          <- na.zero(sweep(d_tmp,1,rowSums(d_tmp),"/"))
  names(w_df)   <- names(M_redCoef)
  weights_group <- colSums(w_df)
  names(weights_group) <- names(M_redCoef)
  
  
  #__Merge_click_and_imp_channels
  namesCoef <- names(weights_group)
  namesCoef <- substr(namesCoef, 1, nchar(namesCoef)-2) # removes the _1, _2 from tail of names.
  weights_combo_group <- tapply(weights_group, namesCoef, sum) # The crucial vector
  
  
  message("Aligning output with events")
  #___DATA_aggregation_____________________________________start___________________
  #__________________________________________________________________________________________
  
  w_df$user_id <- trans1M_20_conv$user_id
  w_df$user_id <- as.character(w_df$user_id)
  w_df <- w_df[c(which(names(w_df) == 'user_id'), 
                 which(!names(w_df) == 'user_id'))] # re-arrange columns
  icM_20   <- icM_20[, -which(names(icM_20) == 'row_rank')]
  icM_conv <- icM_20[which(icM_20$user_id %in% convers), ]
  
  icM_conv$sub_channel_group <- icM_conv$sub_channel
  icM_conv$sub_channel_group[which(paste(icM_conv$sub_channel_group,
                                         icM_conv$eventind, sep = '_')
                                   %in% (subChInd_small_1))] <- 'subChAggr' 
  # replace small subCh's with the agregate - clicks
  
  icM_conv$sub_channel_group[which(paste(icM_conv$sub_channel_group,
                                         icM_conv$eventind, sep = '_')
                                   %in% (subChInd_small_2))] <- 'subChAggr' 
  # replace small subCh's with the agreegate - imps
  
  
  icM_conv$text <- paste(icM_conv$sub_channel_group,
                                     icM_conv$eventind,sep="_")
  
  
  # do melting and merging of w_df and trans - this is to reallocate for subChAggr
  
  w_df_melt              <- melt(w_df)
  colnames(w_df_melt)[3] <- "w"
  
  events_melt  <- melt(trans1M_20_conv[,
                  -which(colnames(trans1M_20_conv)=="conv")])
  events_melt  <- events_melt[events_melt$value!=0,]
  events_melt  <- merge(events_melt,w_df_melt,all.x=T)
  colnames(events_melt)[2] <- "text"
  
  events_melt$weight <- events_melt$w/events_melt$value
  events_melt  <- events_melt[,c("user_id","text","weight")]
  
  # merge weight onto icM
  icM_conv <- merge(icM_conv,events_melt,by=c("user_id","text"),all.x=T)
  icM_conv <- icM_conv[, -which(names(icM_conv) %in% 'text')]
  icM_conv <- icM_conv[, -which(names(icM_conv) %in% 'sub_channel_group')]
  
  ##  look at conversion times
  names(na)[which(names(na) == 'time')] <- 'conv_time'
  
  icM_conv                  <- merge(icM_conv, na, by = 'user_id', all.x = T) # adds conversion time column
  icM_conv$date             <- as.Date(format(icM_conv$time, "%Y-%m-%d"))
  icM_conv$day_name         <- weekdays(icM_conv$time)
  icM_conv$hour_of_day      <- as.numeric(format(icM_conv$time, "%H"))
  icM_conv$conv_date        <- as.Date(format(icM_conv$conv_time, "%Y-%m-%d"))
  icM_conv$conv_day_name    <- weekdays(icM_conv$conv_time)
  icM_conv$conv_hour_of_day <- as.numeric(format(icM_conv$conv_time, "%H"))
  icM_conv$week             <- paste0(icM_conv$record_year,str_pad(icM_conv$record_week,width = 2,pad = "0"))
  icM_conv$month            <- paste0(icM_conv$record_year,str_pad(icM_conv$record_month,width = 2,pad = "0"))
  
  
  dow_lookup <- data.frame(day_name = c('Sunday',
                                        'Monday',
                                        'Tuesday',
                                        'Wednesday',
                                        'Thursday',
                                        'Friday',
                                        'Saturday'),
                           day_of_week = 0:6)
  
  event_dow_distr <- merge(event_dow_distr, dow_lookup, by = 'day_of_week', all.x = T)
  event_dow_distr <- event_dow_distr[, -which(names(event_dow_distr) == 'day_of_week')]
  
  
  
  #___DATA_aggregation_____________________________________end___________________
  #______________________________________________________________________________
  
  #__________PATHS_________________________________________start_________________
  
  message("Pathway analysis")
  # add channel and sub channel codes to icM_20 # check if this is old code
  {
  
    icM_backup <- icM_20
    
    icM_20 <- subset(icM_20,select=c(user_id,
                                     channel,
                                     sub_channel,
                                     time,
                                     eventind,
                                     browser_id,
                                     city_id))
    
    
    icMSubCh <- unique(chanLkup$sub_channel)
    subChIndexDf <- data.frame(sub_channel = icMSubCh,
                               sub_channel_ind = 1:length(icMSubCh)) # old line
    
    icM_20       <- merge(icM_20, subChIndexDf,
                          by = 'sub_channel',
                          all.x = T) 
    
    # rearranges column order
    icM_20 <- icM_20[,c(which(!(names(icM_20) %in% 'sub_channel')),
                       which(names(icM_20) == 'sub_channel'))] 
    
    
    icMCh <- unique(chanLkup$channel)
    
    channelIndexDf <- data.frame(channel = icMCh,
                                 channel_ind = 1:length(icMCh))
    
    icM_20 <- merge(icM_20,
                    channelIndexDf,
                    by = 'channel',
                    all.x = T)
    
    # rearranges columns
    icM_20 <- icM_20[,c(which(!(names(icM_20) %in% 'channel')), 
                        which(names(icM_20) == 'channel'))]
  }
  
  ## Develop and tab1 paths
  {
  icM_20dt <- as.data.table(icM_20)
  setkey(icM_20dt,user_id,time)
  setkey(icM_20dt,user_id)
  
  # by user, get paths
  pathDf <- as.data.frame(icM_20dt[,list(
    channel_path = paste_bar_dedupe(channel_ind),
    sub_path = paste_bar_dedupe(sub_channel_ind),
    path_length = length(channel_ind),
    time = max(time)),
    by=key(icM_20dt)])
  
  pathDf <- merge(pathDf, M_red[, c('user_id', 'conv')], by = 'user_id') # add conv column
  
  # by subchannel then by channel, get paths / uniques

  pathDt <- as.data.table(pathDf)
  setkey(pathDt,sub_path,channel_path)
  
  # path length gets calculated as the mean as the paths are deduped,
  # so theres a different length each time.
  pathSubsDf <- as.data.frame(pathDt[,list(conversions=sum(conv),
                                          path_length=mean(path_length), 
                                          uniques=length(conv)),by=key(pathDt)])
  
  setkey(pathDt,channel_path)
  pathChanDf <- as.data.frame(pathDt[,list(conversions=sum(conv),
                                           path_length=mean(path_length),
                                           uniques=length(conv)),by=key(pathDt)])
  
  pathSubsDf <- pathSubsDf[with(pathSubsDf, order(uniques, decreasing = T)), ]
  pathChanDf <- pathChanDf[with(pathChanDf, order(uniques, decreasing = T)), ]
  
  pathChanDf$uniques <- round((pathChanDf$uniques - pathChanDf$conversions) * 
    (aud_length - length(which(M_red$conv == 1)))/
    (nrow(M_red) - length(which(M_red$conv == 1))) + pathChanDf$conversions) # scale up
  
  pathSubsDf$uniques <- round((pathSubsDf$uniques - pathSubsDf$conversions) * 
    (aud_length - length(which(M_red$conv == 1)))/
    (nrow(M_red) - length(which(M_red$conv == 1))) + pathSubsDf$conversions) # scale up
  
  
  subs_expanded <- do.call(rbind,lapply(strsplit(pathSubsDf$sub_path,split = "|",fixed = T),function(x) {
    y <- head(x,20)
    y <- as.character(
      subChIndexDf$sub_channel[match(y,subChIndexDf$sub_channel_ind)])
    
    if(length(y)<20) {
      y <- c(y,rep("_",20-length(y)))
    }
    y <- as.data.frame(t(data.frame(y)))
    colnames(y) <- paste0("subchan",1:20)
    return(y)
  }))
  
  
  chans_expanded <- do.call(rbind,lapply(strsplit(pathChanDf$channel_path,split = "|",fixed = T),function(x) {
    y <- head(x,20)
    y <- as.character(
      channelIndexDf$channel[match(y,channelIndexDf$channel_ind)])
    
    if(length(y)<20) {
      y <- c(y,rep("_",20-length(y)))
    }
    y <- as.data.frame(t(data.frame(y)))
    colnames(y) <- paste0("chan",1:20)
    return(y)
  }))
  
  pathChanDf <- cbind(pathChanDf,chans_expanded)
  pathSubsDf <- cbind(pathSubsDf,subs_expanded)
  
  pathChanDf$CR <- pathChanDf$conversions/pathChanDf$uniques
  pathSubsDf$CR <- pathSubsDf$conversions/pathSubsDf$uniques
  
  write.csv(pathChanDf,file= file.path(output_dir,"Channel_Pathways.csv"))
  write.csv(pathSubsDf,file= file.path(output_dir,"Sub_Channel_Pathways.csv"))
  }
  #######
  message("Uniques and weights analysis (Pivoting)")
  ### calc exclusive conversions from the icM_conv - allows for weekly, monthly and topline tab1
  {
    ## top_level - channel
    unique_converters_channel <- subset(aggregate(channel~user_id,
                                          icM_conv,FUN=luna),
                                channel==1,select=user_id)
    
    unique_converters_sub_channel <- subset(aggregate(sub_channel~user_id,
                                          icM_conv,FUN=luna),
                                sub_channel==1,select=user_id)
    
    icM_conv$unique_conv_channel_flag <- icM_conv$user_id %in%
      unique_converters_channel$user_id
    icM_conv$unique_conv_sub_channel_flag <- icM_conv$user_id %in%
      unique_converters_sub_channel$user_id
    
    icM_conv$record_month <- icM_conv$month
    icM_conv$record_week  <- icM_conv$week
    
    icM_conv$conv_month <- paste0(year(icM_conv$conv_date),
                                  str_pad(month(icM_conv$conv_date),2,pad="0"))
    icM_conv$conv_week <- paste0(year(icM_conv$conv_date),
                                  substr(ISOweek(icM_conv$conv_date),7,8))
    
    
    weights <- list(total_sub    = aggregate(weight~sub_channel+channel,
                                             icM_conv,FUN=sum),
                    total_chan   = aggregate(weight~channel,
                                             icM_conv,FUN=sum),
                    monthly_sub  = aggregate(weight~sub_channel+channel+conv_month,
                                             icM_conv,FUN=sum),
                    monthly_chan = aggregate(weight~channel+conv_month,
                                             icM_conv,FUN=sum),
                    weekly_sub   = aggregate(weight~channel+sub_channel+conv_week,
                                             icM_conv,FUN=sum),
                    weekly_chan  = aggregate(weight~channel+conv_week,
                                             icM_conv,FUN=sum))
    
    
    weights$total_sub$level    <- "1_s"
    weights$total_chan$level   <- "1_c"
    weights$monthly_sub$level  <- "2_s"
    weights$monthly_chan$level <- "2_c"
    weights$weekly_sub$level   <- "3_s"
    weights$weekly_chan$level  <- "3_c"
    
    weights <- do.call(rbind.fill,weights)
    
    #########
    
    uniq_conv <- list(total_sub  = aggregate(user_id~sub_channel+channel,
                                             icM_conv,FUN=luna,
                                             subset=icM_conv$unique_conv_sub_channel_flag),
                      total_chan = aggregate(user_id~channel,
                                             icM_conv,FUN=luna,
                                             subset=icM_conv$unique_conv_channel_flag),
                      monthly_sub  = aggregate(user_id~sub_channel+channel+conv_month,
                                               icM_conv,FUN=luna,
                                               subset=icM_conv$unique_conv_sub_channel_flag),
                      monthly_chan = aggregate(user_id~channel+conv_month,
                                               icM_conv,FUN=luna,
                                               subset=icM_conv$unique_conv_channel_flag),
                      weekly_sub  = aggregate(user_id~sub_channel+channel+conv_week,
                                              icM_conv,FUN=luna,
                                              subset=icM_conv$unique_conv_sub_channel_flag),
                      weekly_chan = aggregate(user_id~channel+conv_week,
                                              icM_conv,FUN=luna,
                                              subset=icM_conv$unique_conv_channel_flag))
    
    uniq_conv$total_sub$level    <- "1_s"
    uniq_conv$total_chan$level   <- "1_c"
    uniq_conv$monthly_sub$level  <- "2_s"
    uniq_conv$monthly_chan$level <- "2_c"
    uniq_conv$weekly_sub$level   <- "3_s"
    uniq_conv$weekly_chan$level  <- "3_c"
    
    uniq_conv <- do.call(rbind.fill,uniq_conv)
    colnames(uniq_conv)[which(colnames(uniq_conv)=="user_id")] <- "uniq_conv"
    
    colnames(uniq_conv)[which(colnames(uniq_conv)=="conv_month")] <- "record_month"
    colnames(uniq_conv)[which(colnames(uniq_conv)=="conv_week")] <- "record_week"
    
    colnames(weights)[which(colnames(weights)=="conv_month")] <- "record_month"
    colnames(weights)[which(colnames(weights)=="conv_week")] <- "record_week"
  }
  
  
  message("Days to conversion analysis")
  ## get the days for each conversion - this is the date it is assigned to
  {
    
    icM_conv_dt <- as.data.table(icM_conv)
    setkey(icM_conv_dt,user_id,time)
    setkey(icM_conv_dt,user_id)
    
    days_func <- function(x,y) {
      as.numeric(difftime(x[1],y[1],units = "days"))
    }
    
    days <- as.data.frame(icM_conv_dt[,list(channel = channel[1],
                                            sub_channel=sub_channel[1],
                                            record_month=record_month[1],
                                            record_week=record_week[1],
                                            days=days_func(x=conv_time,y=time)),by=key(icM_conv_dt)])
    
    
    
    days <- list(total_chan=aggregate(days~channel,days,FUN=mean),
                 total_sub=aggregate(days~channel+sub_channel,days,FUN=mean),
                 monthly_chan=aggregate(days~channel+record_month,days,FUN=mean),
                 monthly_sub=aggregate(days~channel+sub_channel+record_month,days,FUN=mean),
                 weekly_chan=aggregate(days~channel+record_week,days,FUN=mean),
                 weekly_sub=aggregate(days~channel+sub_channel+record_week,days,FUN=mean))
    
    
    days$total_sub$level    <- "1_s"
    days$total_chan$level   <- "1_c"
    days$monthly_sub$level  <- "2_s"
    days$monthly_chan$level <- "2_c"
    days$weekly_sub$level   <- "3_s"
    days$weekly_chan$level  <- "3_c"
    
    
    
    days <- do.call(rbind.fill,days)
    
  }
  
  # work on costs
  
  costs <- read.csv(cost_file_location)
  costs$date <- as.Date(costs$date,"%d/%m/%Y")
  costs$record_year <- year(costs$date)
  costs$record_month <- paste0(costs$record_year,
                               str_pad(month(costs$date),
                                       width = 2,pad = "0"))
  costs$record_week <- paste0(costs$record_year,
                              substr(ISOweek(costs$date),7,8))
  
  costs$channel <- gsub('\\-', '_',
                            gsub('\\+', '', 
                                 gsub(' ', '_',
                                      gsub("'","", costs$channel))))
  costs$sub_channel <- gsub('\\-', '_',
                                gsub('\\+', '', 
                                     gsub(' ', '_',
                                          gsub("'","",  costs$sub_channel))))
  
  ## check for revenue
  
  check <- "revenue" %in% colnames(costs)
  if(!check) {
    costs$revenue <- 0
  }
  
  #__________PATHS_________________________________________end____________________________
  message("Outputs")
  #___tab1_______________________________________________start______________
  
  ### put on a level marker to lc and aud
  
  
  ## get tab1 going
  tab1 <- merge(lc,aud_distr,all=T)
  tab1 <- merge(tab1,events,all=T)
  tab1 <- merge(tab1,weights,all=T)
  tab1 <- merge(tab1,uniq_conv,all=T)
  tab1 <- merge(tab1,uni_distr,all=T)
  tab1 <- merge(tab1,days,all=T)
  
  costs2 <- list(total_sub  = aggregate(cbind(clicks,impressions,media_cost,revenue)~sub_channel+channel,
                                       costs,FUN=sum),
                total_chan = aggregate(cbind(clicks,impressions,media_cost,revenue)~channel,
                                       costs,FUN=sum),
                monthly_sub = aggregate(cbind(clicks,impressions,media_cost,revenue)~sub_channel+channel+record_month,
                                        costs,FUN=sum),
                monthly_chan = aggregate(cbind(clicks,impressions,media_cost,revenue)~channel+record_month,
                                         costs,FUN=sum),
                weekly_sub  = aggregate(cbind(clicks,impressions,media_cost,revenue)~sub_channel+channel+record_week,
                                        costs,FUN=sum),
                weekly_chan = aggregate(cbind(clicks,impressions,media_cost,revenue)~channel+record_week,
                                        costs,FUN=sum))
  ###
  
  costs2$total_sub$level    <- "1_s"
  costs2$total_chan$level   <- "1_c"
  costs2$monthly_sub$level  <- "2_s"
  costs2$monthly_chan$level <- "2_c"
  costs2$weekly_sub$level   <- "3_s"
  costs2$weekly_chan$level  <- "3_c"
  
  
  costs2 <- do.call(rbind.fill,costs2)
  costs2  <- costs2[,c("channel","sub_channel","record_month",
                       "record_week","clicks","impressions","media_cost","revenue","level")]
  ###
  colnames(costs2) <- c("channel","sub_channel","record_month",
                       "record_week","cost_clicks","cost_impressions","media_cost","revenue","level")
  
  tab1 <- merge(tab1,costs2,all=T)
  
  if(!check) {
    tab1$revenue <- tab1$weight*revenue_per_conv
  }
  
  tab1$lc           <- na.zero(tab1$lc)
  tab1$weight       <- na.zero(tab1$weight)
  tab1$uniq_conv    <- na.zero(tab1$uniq_conv)
  tab1$uniques      <- na.zero(tab1$uniques)
  tab1$days         <- na.zero(tab1$days)
  tab1$revenue      <- na.zero(tab1$revenue)
  tab1$audience     <- na.zero(tab1$audience)
  tab1$clicks       <- na.zero(tab1$clicks)
  tab1$impressions  <- na.zero(tab1$impressions)
  tab1$cost_clicks       <- na.zero(tab1$cost_clicks)
  tab1$cost_impressions  <- na.zero(tab1$cost_impressions)
  tab1$media_cost  <- na.zero(tab1$media_cost)
  
  
  tab1$level <- factor(x = tab1$level,
                       levels = c("1_c","1_s","2_c",
                                  "2_s","3_c","3_s"))
  tab1$top <- substr(tab1$level,1,1)
  tab1$ch_sub_flag <- substr(tab1$level,3,3)
  
  tab1 <- tab1[order(tab1$level,tab1$channel,tab1$sub_channel,
                     tab1$record_month,tab1$record_week),]
  
  ### here merge on the costs:
  setnames(tab1,"record_month","month")
  setnames(tab1,"record_week","week")
  
  
  #########################
  
  ##########
  
  #__Tab2:_________________________
  
  tab2_sub <- aggregate(weight~channel+sub_channel+conv_date,
                        icM_conv,FUN=sum)
  tab2_chan <- aggregate(weight~channel+conv_date,
                        icM_conv,FUN=sum)
  tab2_sub$level  <- "s"
  tab2_chan$level <- "c"
  
  tab2_events_sub <- aggregate(cbind(impressions,clicks)~
                                 channel+sub_channel+date,
                               event_dow_distr,FUN=sum)
  tab2_events_chan <- aggregate(cbind(impressions,clicks)~
                                 channel+date,
                                event_dow_distr,FUN=sum)
  tab2_events_sub$level  <- "s"
  tab2_events_chan$level <- "c"
  
  
  tab2_weights <- rbind.fill(tab2_sub,tab2_chan)
  tab2_events  <- rbind.fill(tab2_events_sub,tab2_events_chan)
  
  colnames(tab2_weights)[which(colnames(tab2_weights)=="conv_date")] <- "date"
  
  tab2 <- merge(tab2_weights,tab2_events,all=T,
                by=c("channel","sub_channel","date","level"))
  tab2$weight      <- na.zero(tab2$weight)
  tab2$impressions <- na.zero(tab2$impressions)
  tab2$clicks      <- na.zero(tab2$clicks)
  

  tab2 <- merge(tab2,subset(costs,select=c("date","sub_channel","channel","media_cost","revenue")),
                by=c("date","sub_channel","channel"),all.x=T)
  
  if(!check) {
    tab2$revenue <- tab2$weight*revenue_per_conv
  }
  
  ######################################
  
  ###############
  
  #______Tab4:_____________
  
  #need to make pathChanDf again, not deduping this time
{
  icM_20dt <- as.data.table(icM_20)
  setkey(icM_20dt,user_id,time)
  setkey(icM_20dt,user_id)
  
  # by user, get paths
  pathDf2 <- as.data.frame(icM_20dt[,list(
    channel_path = paste_bar(channel_ind),
    sub_channel_path = paste_bar(sub_channel_ind),
    path_length = length(channel_ind),
    time = max(time),
    month=month(max(time))),
    by=key(icM_20dt)])
  
  pathDf2 <- merge(pathDf2, M_red[, c('user_id', 'conv')],
                   by = 'user_id') # add conv column
  
  # by channel, sub_channel, get paths / uniques
  pathDt <- as.data.table(pathDf2)
  
  ## Channel
  setkey(pathDt,channel_path)
  
  pathChanDf <- as.data.frame(pathDt[,list(conversions=sum(conv),
                                           path_length=unique(path_length),
                                           uniques=length(conv)),by=key(pathDt)])
  
  pathChanDf <- pathChanDf[with(pathChanDf, order(uniques, decreasing = T)), ]
  
  pathChanDf$uniques <- round((pathChanDf$uniques - pathChanDf$conversions) * 
                                (aud_length - length(which(M_red$conv == 1)))/
                                (nrow(M_red) - length(which(M_red$conv == 1))) +
                                pathChanDf$conversions)
  
  
  ## Sub Channel
  setkey(pathDt,channel_path,sub_channel_path,month)
  
  pathSubsDf <- as.data.frame(pathDt[,list(conversions=sum(conv),
                                           path_length=unique(path_length),
                                           uniques=length(conv)),by=key(pathDt)])
  
  pathSubsDf <- pathSubsDf[with(pathSubsDf, order(uniques, decreasing = T)), ]
  
  pathSubsDf$uniques <- round((pathSubsDf$uniques - pathSubsDf$conversions) * 
                                (aud_length - length(which(M_red$conv == 1)))/
                                (nrow(M_red) - length(which(M_red$conv == 1))) +
                                pathSubsDf$conversions)
}

  ##### channels
  conv_rows_by_len <- aggregate(cbind(uniques,conversions)~path_length,
                                pathChanDf,FUN=sum)
  
  path_split <- lapply(strsplit(pathChanDf$channel_path,split = "|",fixed = T),unique)
  
  by_channel_conv_rows_by_len <- lapply(unique(unlist(lapply(path_split,unique))),function(x) {
    X <- unlist(lapply(path_split,function(y) x %in% y))
    Y <- pathChanDf[which(X),1:4]
    Z <- aggregate(cbind(uniques,conversions)~path_length,
              Y,FUN=sum)
    Z$chan <- x
    return(Z)
  })
  
  by_channel_conv_rows_by_len <- do.call(rbind,by_channel_conv_rows_by_len)
  
  by_channel_conv_rows_by_len$chan <- channelIndexDf$channel[
    match(by_channel_conv_rows_by_len$chan,
          channelIndexDf$channel_ind)]
  
  tab4a <- rbind.fill(conv_rows_by_len,by_channel_conv_rows_by_len)
  colnames(tab4a)[which(colnames(tab4a)=="chan")] <- "channel"
  
  
  ##### sub channels
  conv_rows_by_len <- aggregate(cbind(uniques,conversions)~path_length+month,
                                pathSubsDf,FUN=sum)
  
  path_split <- lapply(strsplit(pathSubsDf$sub_channel_path,split = "|",fixed = T),unique)
  
  by_subchannel_conv_rows_by_len <- lapply(unique(unlist(lapply(path_split,unique))),function(x) {
    X <- unlist(lapply(path_split,function(y) x %in% y))
    Y <- pathSubsDf[which(X),1:6]
    Z <- aggregate(cbind(uniques,conversions)~path_length+month,
                   Y,FUN=sum)
    Z$subchan <- x
    return(Z)
  })
  
  by_subchannel_conv_rows_by_len <- do.call(rbind,by_subchannel_conv_rows_by_len)
  
  by_subchannel_conv_rows_by_len$subchan <- subChIndexDf$sub_channel[
    match(by_subchannel_conv_rows_by_len$subchan,
          subChIndexDf$sub_channel_ind)]

  by_subchannel_conv_rows_by_len$channel <- chanLkup$channel[
    match(by_subchannel_conv_rows_by_len$subchan,
          chanLkup$sub_channel)]

  tab4b <- rbind.fill(conv_rows_by_len,by_subchannel_conv_rows_by_len)
  colnames(tab4b)[which(colnames(tab4b)=="subchan")] <- "sub_channel"
  
  ######################
  
  #________Tab5:______________
  
  ## assign last click details onto icM_conv
  icM_conv <- icM_conv[order(icM_conv$user_id,icM_conv$time,decreasing=T),]
  
## cannot use lc_na from red code, as this doesn;t 
# include the time of the event which is registered as being for lc.
# so we must calculate lc again.

setnames(lc_na,"time","conv_time")

  icM_conv <- do.call(rbind,lapply(split(icM_conv, icM_conv$user_id),function(x){
    y <- rep(0,nrow(x))
    if(x$eventind[1] == 2 & any(x$eventind == 1) ){
      y[which(x$eventind == 1)[1]] <- 1
      lc_channel     <- x$channel[which(x$eventind == 1)[1]]
      lc_sub_channel <- x$sub_channel[which(x$eventind == 1)[1]]
    } else{
      y[1] <- 1 # if no clicks on pathway, choose last impression
      lc_channel     <- x$channel[1]
      lc_sub_channel <- x$sub_channel[1]
    }
    x$lc <- y
    x$lc_channel     <- lc_channel
    x$lc_sub_channel <- lc_sub_channel
    return(x)}))
  
  icM_conv <- icM_conv[order(icM_conv$user_id,icM_conv$time,decreasing=T),]
  rownames(icM_conv) <- NULL
  
  tab5a <- melt(dcast(icM_conv,channel~lc_channel,
             fun.aggregate = sum,value.var = "weight"),
       variable_name = "channel_lc",value_name="weight")
  
  tab5a <- tab5a[order(tab5a$channel),]
  colnames(tab5a) <- c("channel_id","Channel_LC","Weights")
  
  tab5b <- melt(dcast(icM_conv,sub_channel~lc_sub_channel,
                     fun.aggregate = sum,value.var = "weight"),
               variable_name = "sub_channel_lc",value_name="weight")
  
  tab5b <- tab5b[order(tab5b$sub_channel),]
  colnames(tab5b) <- c("sub_channel_id","Sub_Channel_LC","Weights")
  
  #________Tab6:_________________
  
  X <- do.call(rbind,lapply(split(icM_conv, icM_conv$user_id), function(x) {
    
    x <- x[order(x$time),]
    
    if(luna(x$eventind)<2) { return(NULL)} else {
      
      first_imp   <- head(subset(x,eventind==2),1)
      first_click <- head(subset(x,eventind==1),1)
      
      diff1 <- as.numeric(difftime(first_click$time,first_imp$time,units="mins"))
      diff2 <- as.numeric(difftime(x$conv_time[1],first_click$time,units="mins"))
      diff3 <- as.numeric(difftime(x$conv_time[1],first_imp$time,units="mins"))
      
      out <- data.frame(firstimp_channel=first_imp$channel,
                        firstimp_sub_channel=first_imp$sub_channel,
                        firstclick_channel=first_click$channel,
                        firstclick_sub_channel=first_click$sub_channel,
                        diff1=diff1,
                        diff2=diff2,
                        diff3=diff3)
      
      return(out)
    }
    
  }))
  if(!is.null(X)) {
    tab6_ok <- TRUE
    X$firstimp_channel <- as.character(X$firstimp_channel)
    X$firstimp_sub_channel <- as.character(X$firstimp_sub_channel)
    X$firstclick_channel <- as.character(X$firstclick_channel)
    X$firstclick_sub_channel <- as.character(X$firstclick_sub_channel)
    
    X_chan <- X[,c("firstimp_channel","firstclick_channel",
                 "diff1","diff2","diff3")]
    
    X_sub_chan <- X[,c("firstimp_sub_channel","firstclick_sub_channel",
                 "diff1","diff2","diff3")]
    
    tab6a <- aggregate(formula=cbind(diff1,diff2,diff3)~
                        firstimp_channel+firstclick_channel,
                      data=X_chan,FUN=mean)
    
    tab6b <- aggregate(formula=cbind(diff1,diff2,diff3)~
                        firstimp_sub_channel+firstclick_sub_channel,
                      data=X_sub_chan,FUN=mean)

  } else {tab6_ok <- FALSE}
  # tab7 --------------------------------------------------------------------
  
  timeSplitFunc <- function(x) {
    mins <- minute(x) + 60*hour(x)
    
    y <- as.character(cut(mins,breaks=c(30,6*60,9*60+25,12*60,16*60,17*60+25,20*60,23*60),
             labels=c("Late_Night",
                      "Breakfast",
                      "Coffee Time",
                      "Daytime",
                      "Pre Peak",
                      "Early Peak",
                      "Late Peak")))
    
    y[is.na(y)] <- "Post Peak"
  
  return(y)
  }
  
  daypart_match <- data.frame(hour=0:23,daypart=NA)
  daypart_match$daypart[daypart_match$hour==0]            <- "Late_Night"
  daypart_match$daypart[daypart_match$hour %in% c(1:5)]   <- "Late_Night"
  daypart_match$daypart[daypart_match$hour %in% c(6:9)]   <- "Breakfast"
  daypart_match$daypart[daypart_match$hour %in% c(10:11)] <- "Coffee"
  daypart_match$daypart[daypart_match$hour %in% c(12:15)] <- "DayTime"
  daypart_match$daypart[daypart_match$hour %in% c(16:17)] <- "Pre_Peak"
  daypart_match$daypart[daypart_match$hour %in% c(18:19)] <- "Early_Peak"
  daypart_match$daypart[daypart_match$hour %in% c(20:22)] <- "Late_Peak"
  daypart_match$daypart[daypart_match$hour %in% c(23)]    <- "Post_Peak"
  colnames(daypart_match) <- c("hour_of_day","daypart")
  
  event_dow_distr$daypart <- daypart_match$daypart[match(event_dow_distr$hour_of_day,
                                                         daypart_match$hour_of_day)]
  event_dow_distr$day_of_week <- wday(event_dow_distr$date)-1
  # now for the conversions
  
  na_browser$hour_of_day <- hour(na_browser$time)
  na_browser$day_of_week <- wday(na_browser$time)-1
  na_browser$conversions <- "C"
  na_browser$daypart <- daypart_match$daypart[match(na_browser$hour_of_day,
                                                         daypart_match$hour_of_day)]
  
  tab7_list <- list(
    ic_hod  = aggregate(cbind(impressions,clicks)~
                      channel+
                      sub_channel+
                      day_of_week+
                      hour_of_day+
                      conv_path+
                      browser_id,
                    data=event_dow_distr,FUN=sum),
    ic_dayp = aggregate(cbind(impressions,clicks)~
                           channel+
                           sub_channel+
                           day_of_week+
                           daypart+
                           conv_path+
                           browser_id,
                         data=event_dow_distr,FUN=sum),
    conv_hod  = aggregate(conversions~browser_id+day_of_week+hour_of_day,
                          data=na_browser,FUN=length),
    conv_dayp = aggregate(conversions~browser_id+day_of_week+daypart,
                          data=na_browser,FUN=length))
  
  tab7 <- do.call(rbind.fill,tab7_list)
                    
  
  tab7$conversions <- na.zero(tab7$conversions)
  tab7$hour_flag <- 1
  tab7$hour_flag[is.na(tab7$hour_of_day)] <- 0

  tab7$hour_of_day[is.na(tab7$hour_of_day)] <- ""
  tab7$daypart[is.na(tab7$daypart)] <- ""

day_match <- data.frame(day_of_week = 0:6,dow=c("Sunday","Monday","Tuesday",
                                                "Wednesday","Thursday","Friday",
                                                "Saturday"))
tab7$day_of_week <- day_match$dow[match(tab7$day_of_week,day_match$day_of_week)] 

  # tab8 --------------------------------------------------------------------
  # order of events in paths
  
  X <- lapply(split(icM_conv,icM_conv$user_id),function(x) {
    x <- x[order(x$time,decreasing = T),]
    df <- data.frame(sub_channel=head(x$sub_channel,10),
                     position=(1:10)[1:min(10,nrow(x))])
    return(df)
  })
  
  X <- do.call(rbind,X)
  rownames(X) <- NULL
  
  tab8 <- melt(table(X$sub_channel,X$position))
  colnames(tab8) <- c("sub_channel","c","_FREQ_")
  tab8 <- tab8[order(tab8$sub_channel,tab8$c),]
  ##########
  
  
  # tab9 --------------------------------------------------------------------
  
  # calc time to conversion by user from first touchpoint
  
  X <- lapply(split(icM_conv,icM_conv$user_id),function(x) {
    x <- x[order(x$time,decreasing=F),]
    t2c <- as.numeric(difftime(x$conv_time[1],x$time[1],units = "hours"))
    df <- data.frame(channel=x$channel[1],t2c=t2c)
    return(df)
  })
  X <- do.call(rbind,X)
  rownames(X) <- NULL
  
  X$diff2 <- cut(X$t2c,
                 breaks=c(0,1,2,6,12,24,2*24,
                          3*24,7*24,14*24,21*24,
                          28*24,10000),
                 labels=c("<1 hour",
                          "1-2 hours",
                          "2-6 hours",
                          "6-12 hours",
                          "12-24 hours",
                          "1-2 days",
                          "2-3 days",
                          "3-7 days",
                          "7-14 days",
                          "14-21 days",
                          "21-28 days",
                          "28+ days"))
  X$conv <- 1
  
  tab9 <- aggregate(conv~diff2+channel,data=X,FUN=sum)
  
  Y <- aggregate(conv~channel,tab9,FUN=sum)
  colnames(Y) <- c("channel","tot")
  
  tab9 <- merge(tab9,Y)
  tab9 <- tab9[order(tab9$channel,tab9$diff2),]
  
  Z <- do.call(rbind,
               lapply(split(tab9,tab9$channel),
                      function(x) {
                        df <- data.frame(channel=x$channel,
                                         cum=cumsum(x$conv))
                        return(df)
                      }))
  rownames(Z) <- NULL
  
  Z    <- Z[order(Z$channel),]
  tab9 <- tab9[order(tab9$channel),]
  
  tab9 <- cbind(tab9,Z$cum)
  colnames(tab9) <- c("channel","diff2","conv","tot","cum")
  
  tab9$percent <- tab9$conv/tab9$tot
  tab9$cumpc   <- tab9$cum/tab9$tot
  
  
  # tab10/11 -------------------------------------------------------------------
  
  # already done in red code
  tab10 <- os_distr
  tab11 <- city_distr
  
  # Write Outputs -----------------------------------------------------------
  
  write.csv(tab1,   file.path(output_dir,'tab_1.csv'),  row.names = F)
  write.csv(tab2,   file.path(output_dir,'tab_2.csv'),  row.names = F)
  write.csv(tab4a,   file.path(output_dir,'tab_4a.csv'),  row.names = F)
  write.csv(tab4b,   file.path(output_dir,'tab_4b.csv'),  row.names = F)
  write.csv(tab5a,  file.path(output_dir,'tab_5a.csv'), row.names = F)
  write.csv(tab5b,  file.path(output_dir,'tab_5b.csv'), row.names = F)
  if(tab6_ok) {  
    write.csv(tab6a,  file.path(output_dir,'tab_6a.csv'), row.names = F)
    write.csv(tab6b,  file.path(output_dir,'tab_6b.csv'), row.names = F)}
  write.csv(tab7,   file.path(output_dir,'tab_7.csv'),  row.names = F)
  write.csv(tab8,   file.path(output_dir,'tab_8.csv'),  row.names = F)
  write.csv(tab9,   file.path(output_dir,'tab_9.csv'),  row.names = F)
  write.csv(tab10,  file.path(output_dir,'tab_10.csv'), row.names = F)
  write.csv(tab11,  file.path(output_dir,'tab_11.csv'), row.names = F)
 
  # these outputs might be extra.

if(extra_csv_outputs) {
write.csv(icM_20,file=file.path(output_dir,'icM_20.csv'))
#write.csv(pathSubsDf,file=file.path(output_dir,'pathSubsDf.csv'))
#write.csv(pathChanDf,file=file.path(output_dir,'pathChanDf.csv'))
write.csv(icM_conv,file=file.path(output_dir,'icM_conv.csv'))
write.csv(trans1M_20,file=file.path(output_dir,'trans1M_20.csv'))
write.csv(M_red,file=file.path(output_dir,'M_red.csv'))
}




end.time <- Sys.time()
  function_summary <- cat(paste0("Blue funtion Runtime Stats:"),
                          paste0("Start Time: ",start.time),
                          paste0("Finish Time: ",end.time),
                          paste0("Run Time: ",end.time-start.time),sep="\n")
  return(function_summary)
}