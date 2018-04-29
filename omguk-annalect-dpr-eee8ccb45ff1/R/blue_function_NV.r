#' Runs the blue process
#'

#' Runs a simplified blue process specifically for the New Visitors analysis
#'

#' @param username The username for redshift

#' @param file_save_location This is where you would like the intermediate files (between red and blue processes) to be saved.
#' @param output_dir This is where you would like the csv tabs for the dashboard to be output. If this is the same location as the tableau currently points then you will only need to refresh the dashboard to update. Do not have any of these files open while running.
#' @param click_cutoff The number of clicks that is counted as too few for a sub channel to be left on its own. Defaults to 5
#' @param imp_cutoff The number of imps that is counted as too few for a sub channel to be left on its own. Defaults to 250
#' @param revenue_per_conv The revenue for each conversion.
#' @param extra_csv_outputs Defaults to TRUE, asks if you want to write out as csv's some important intermediate files. For very large DP runs this may be undesirable as it can be slow for large files.
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' @examples
#' #blue_function_NV(username="tim_scholtes",file_save_location="Z:/GROUP/DPR/TIM_test",
#' output_dir="Z:/GROUP/DPR/TIM_test",
#' click_cutoff = 5,imp_cutoff=250,revenue_per_conv=10) # not run

blue_function_NV <- function(username,DPID,file_save_location,output_dir,
                          click_cutoff = 5,imp_cutoff=250,extra_csv_outputs=TRUE) {
  start.time <- Sys.time()    
  analyst <- username
  message("Loading Files")

  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_aud_distr.RData")))
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
  load(file = file.path(file_save_location,paste0(analyst,"_",DPID,"_na_os.RData")))
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
  
  if (length(dup_ids>0)){
  
  trans_melt <- melt(subset(trans1M_20,user_id %in% dup_ids),id.vars = "user_id") # this is still a bit slow - use data.table
  trans1M_20_clean <- subset(trans1M_20,!user_id %in% dup_ids)
  trans1M_20_deduped <- dcast(trans_melt,user_id~variable,fun.aggregate = sum)
  trans1M_20 <- rbind(trans1M_20_clean,trans1M_20_deduped)
  
  }
  
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
  
  
  M_red_2 <- trans1M_20
  ### check for converting channels:

  M_red_2_conv <- subset(M_red_2,conv==1)
  ### redo to reference column name?

  keepers_2 <- names(which(colSums(M_red_2_conv[,-c(1,ncol(M_red_2_conv))])!=0))
  
  #_2.__________________________Imps__________________________________________
  
  cutoff <- imp_cutoff
  
  if(length(keepers_2)>0) {
    subChInd_small_2 <- names(which(
      colSums(M_red_2[,keepers_2,drop=FALSE]>0) < cutoff))
  } else subChInd_small_2 <- NULL
  # index of subChannels within M_red_2 that have less than 'cutoff' 
  #non-zero entries. The names of the entries are the subchannel names.
  
  if(length(subChInd_small_2) > 0){
      message(paste('There are ', length(subChInd_small_2), ' small imp sub-channels'))
    subChAggr_2 <- rowSums(M_red_2[, subChInd_small_2,drop=F])
    M_red_2 <- M_red_2[which(!names(M_red_2) %in% subChInd_small_2)]
    M_red_2$subChAggr_2 <- subChAggr_2 
    }
  
  M_red_2 <- M_red_2[,c(which(!(names(M_red_2) %in% 'conv')), 
                       which(names(M_red_2) == 'conv'))] 
  
  #__Stitch_the_matrices_back:
  M_red <- M_red_2
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
  message(paste0("Number of users replaced by coefs is: ", length(zeroInd)))
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
  
  icM_conv$record_month <- icM_conv$month
  icM_conv$record_week  <- icM_conv$week
  
  icM_conv$conv_month <- paste0(year(icM_conv$conv_date),
                                str_pad(month(icM_conv$conv_date),2,pad="0"))
  icM_conv$conv_week <- paste0(year(icM_conv$conv_date),
                               substr(ISOweek(icM_conv$conv_date),7,8))
  
  
  ### calc exclusive conversions from the icM_conv - allows for weekly, monthly and topline tab1
    
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
  
  colnames(weights)[which(colnames(weights)=="conv_month")] <- "record_month"
  colnames(weights)[which(colnames(weights)=="conv_week")] <- "record_week"  
  
    #########
  #__________PATHS_________________________________________end____________________________
  message("Outputs")
  #___tab1_______________________________________________start______________
  
  ## get tab1 going

  tab1 <- weights
  tab1$weight       <- na.zero(tab1$weight)
 
  tab1$level <- factor(x = tab1$level,
                       levels = c("1_c","1_s","2_c",
                                  "2_s","3_c","3_s"))
  tab1$top <- substr(tab1$level,1,1)
  tab1$ch_sub_flag <- substr(tab1$level,3,3)
  
  
  tab1 <- tab1[order(tab1$level,tab1$channel,tab1$sub_channel,
                     tab1$record_month,tab1$record_week),]
  
  
  setnames(tab1,"record_month","month")
  setnames(tab1,"record_week","week")
  
  
  # Write Outputs -----------------------------------------------------------
  
  write.csv(tab1,   file.path(output_dir,'tab_1.csv'),  row.names = F)
  
end.time <- Sys.time()
  function_summary <- cat(paste0("Blue funtion Runtime Stats:"),
                          paste0("Start Time: ",start.time),
                          paste0("Finish Time: ",end.time),
                          paste0("Run Time: ",end.time-start.time),sep="\n")
  return(function_summary)
}