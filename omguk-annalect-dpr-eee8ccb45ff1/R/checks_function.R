#' Does checks in the red function for stuff that will cause errors

#'

#' Does checks in the red function for stuff that will cause errors

#'

#' @param startDate
#' @param endDate
#' @param startPre
#' @param endPre
#' @param hierarchy_location
#' @param reupload_hierarchy
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' 

checks_function1 <- function(startDate,endDate,
                            startPre,endPre,
                            reupload_hierarchy,
                            hierarchy_location) {
  
  ## Dates - check they're valid:
  failure <- 0
  
  sd <- try( as.Date( startDate, format= "%Y-%m-%d" ) )
  ed <- try( as.Date( endDate, format= "%Y-%m-%d" ) )
  sp <- try( as.Date( startPre, format= "%Y-%m-%d" ) )
  ep <- try( as.Date( endPre, format= "%Y-%m-%d" ) )
 
  if(class(sd)=="try-error" || is.na(sd)) {
    stop("WHAT WENT WRONG: startDate must be a valid date, in YYYY-MM-DD format" )
     failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
     failure <- 1}
  if(class(sp)=="try-error" || is.na(sp)) {
    stop("WHAT WENT WRONG: startPre must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
failure <- 1}
  
  if(class(ed)=="Date"&class(sd)=="Date" & ed<sd) {
    stop("WHAT WENT WRONG: startDate must be before endDate")
    failure <- 1}
  if(class(ep)=="Date"&class(sp)=="Date" & ep<sp) {
    stop("WHAT WENT WRONG: startPre must be before endPre")
    failure <- 1}
  if(class(ep)=="Date"&class(sd)=="Date" & sd<ep) {
    stop("WHAT WENT WRONG: startDate must be after endPre")
    failure <- 1
  }
  
  if(!reupload_hierarchy) {
    X <-  try({
    x <- dbSendQuery(redshift_con,paste0("
            select 1 from ds_",agency,".dc_",analyst,"_",DPID,"_category"))
    x <- fetch(x,n=-1)},
    silent=TRUE)
    
    if(class(X)=="try-error") {
      stop(
        "WHAT WENT WRONG: Hierarchy is not present in redshift, put reupload_hierarchy=T and set the hierarchy locations appropriately")
      failure <- 1
    }
  }


  if(reupload_hierarchy) {
    
    X <- is.null(hierarchy_location)
    if(X) {stop(
      "WHAT WENT WRONG: You haven't specified a hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the csv.")}
    
    hierarchy <- read.csv(hierarchy_location)
    colnames(hierarchy) <- tolower(colnames(hierarchy))
    
    x <- colnames(hierarchy)
    x <- gsub("[^a-z_]*","",x)
    X <- c("placement_id","campaign_id","channel","sub_channel")
    
    if(any(!(X %in% x))) {
      stop(
        "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'placement_id', 'campaign_id', 'channel', 'sub_channel'")
      failure <- 1
    }
    
    # check for duplicate page_ids
    
    X <- any(duplicated(hierarchy$page_id))
    
    if(X) {
      warning(
        "WARNING!!! There are duplicate page_ids in your hierarchy. This may cause duplication issues when merging onto your events."
        )
    }
    
    
  }

  
  return(failure)
}



#' Does checks in the red function for stuff that will cause errors

#'

#' Does checks in the red function for stuff that will cause errors

#'

#' @param startDate
#' @param endDate
#' @param startPre
#' @param endPre
#' @param hierarchy_location
#' @param ppc_match
#' @param ppc_hierarchy_location
#' @param reupload_hierarchy
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' 

checks_function_keyword <- function(startDate,endDate,
                             startPre,endPre,
                             reupload_hierarchy,
                             ppc_match,
                             hierarchy_location,
                             ppc_hierarchy_location,
                             ppc_match_col) {
  
  ## Dates - check they're valid:
  failure <- 0
  
  sd <- try( as.Date( startDate, format= "%Y-%m-%d" ) )
  ed <- try( as.Date( endDate, format= "%Y-%m-%d" ) )
  sp <- try( as.Date( startPre, format= "%Y-%m-%d" ) )
  ep <- try( as.Date( endPre, format= "%Y-%m-%d" ) )
  
  if(class(sd)=="try-error" || is.na(sd)) {
    stop("WHAT WENT WRONG: startDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(sp)=="try-error" || is.na(sp)) {
    stop("WHAT WENT WRONG: startPre must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  
  if(class(ed)=="Date"&class(sd)=="Date" & ed<sd) {
    stop("WHAT WENT WRONG: startDate must be before endDate")
    failure <- 1}
  if(class(ep)=="Date"&class(sp)=="Date" & ep<sp) {
    stop("WHAT WENT WRONG: startPre must be before endPre")
    failure <- 1}
  if(class(ep)=="Date"&class(sd)=="Date" & sd<ep) {
    stop("WHAT WENT WRONG: startDate must be after endPre")
    failure <- 1
  }
  
  if(!reupload_hierarchy) {
    X <-  try({
      x <- dbSendQuery(redshift_con,paste0("
            select 1 from ds_",agency,".dc_",analyst,"_",DPID,"_category"))
      x <- fetch(x,n=-1)},
      silent=TRUE)
    
    if(class(X)=="try-error") {
      stop(
        "WHAT WENT WRONG: Hierarchy is not present in redshift, put reupload_hierarchy=T and set the hierarchy locations appropriately")
      failure <- 1
    }
  }
  
  
  if(reupload_hierarchy) {
    
    X <- is.null(hierarchy_location)
    if(X) {stop(
      "WHAT WENT WRONG: You haven't specified a hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the csv.")}
    
    
    hierarchy <- read.csv(hierarchy_location)
    colnames(hierarchy) <- tolower(colnames(hierarchy))
    
    x <- colnames(hierarchy)
    x <- gsub("[^a-z_]*","",x)
    X <- c("placement_id","campaign_id","channel","sub_channel")
    
    if(any(!(X %in% x))) {
      stop(
        "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'placement_id', 'campaign_id', 'channel', 'sub_channel'")
      failure <- 1
    }
    
    
    X <- any(duplicated(hierarchy$placement_id))
    
    if(X) {
      warning(
        "WARNING!!! There are duplicate placement_id in your hierarchy. This may cause duplication issues when merging onto your events."
      )
    }
    
    ## PPC bit
    
    if(ppc_match) {
      
      X <- is.null(ppc_hierarchy_location)
      if(X) {stop(
        "WHAT WENT WRONG: You haven't specified a ppc_hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the ppc_hierarchy csv.")}
      
      
      hierarchy <- read.csv(ppc_hierarchy_location)
      colnames(hierarchy) <- tolower(colnames(hierarchy))
      
      x <- colnames(hierarchy)
      x <- gsub("[^a-z_]*","",x)
      X <- c(ppc_match_col,"channel","sub_channel")
      
      if(any(!(X %in% x))) {
        stop(
          "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'channel', 'sub_channel', and either 'paid_search_campaign' or 'paid_search_ad_group',whichever you specified ppc_match_col to be.")
        failure <- 1
      }
      
      
      X <- any(duplicated(hierarchy[,ppc_match_col]))
      
      if(X) {
        warning(
          "WARNING!!! There are duplicate entries of ppc_match_col in your hierarchy. This may cause duplication issues when merging onto your events. CHECK that there is only one channel/sub_channel per ppc_match_col or results WILL be wrong."
        )
      }
    }
    
    
    
  }
  
  
  return(failure)
}



#' Does checks in the red function for stuff that will cause errors

#'

#' Does checks in the red function for stuff that will cause errors

#'

#' @param startDate
#' @param endDate
#' @param startPre
#' @param endPre
#' @param hierarchy_location
#' @param ppc_match
#' @param ppc_hierarchy_location
#' @param reupload_hierarchy
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' 

checks_function_keyword_PHD <- function(startDate,endDate,
                                    startPre,endPre,
                                    reupload_hierarchy,
                                    ppc_match,
                                    hierarchy_location,
                                    ppc_hierarchy_location,
                                    ppc_match_col) {
  
  ## Dates - check they're valid:
  failure <- 0

  sd <- try( as.Date( startDate, format= "%Y-%m-%d" ) )
  ed <- try( as.Date( endDate, format= "%Y-%m-%d" ) )
  sp <- try( as.Date( startPre, format= "%Y-%m-%d" ) )
  ep <- try( as.Date( endPre, format= "%Y-%m-%d" ) )
  
  if(class(sd)=="try-error" || is.na(sd)) {
    stop("WHAT WENT WRONG: startDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(sp)=="try-error" || is.na(sp)) {
    stop("WHAT WENT WRONG: startPre must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  
  if(class(ed)=="Date"&class(sd)=="Date" & ed<sd) {
    stop("WHAT WENT WRONG: startDate must be before endDate")
    failure <- 1}
  if(class(ep)=="Date"&class(sp)=="Date" & ep<sp) {
    stop("WHAT WENT WRONG: startPre must be before endPre")
    failure <- 1}
  if(class(ep)=="Date"&class(sd)=="Date" & sd<ep) {
    stop("WHAT WENT WRONG: startDate must be after endPre")
    failure <- 1
  }
  
  if(!reupload_hierarchy) {
    X <-  try({
      x <- dbSendQuery(redshift_con,paste0("
                                           select 1 from ds_",agency,".dc_",analyst,"_",DPID,"_category"))
      x <- fetch(x,n=-1)},
      silent=TRUE)
    
    if(class(X)=="try-error") {
      stop(
        "WHAT WENT WRONG: Hierarchy is not present in redshift, put reupload_hierarchy=T and set the hierarchy locations appropriately")
      failure <- 1
    }
  }
  
  
  if(reupload_hierarchy) {
    
    X <- is.null(hierarchy_location)
    if(X) {stop(
      "WHAT WENT WRONG: You haven't specified a hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the csv.")}
    
    
    hierarchy <- read.csv(hierarchy_location)
    colnames(hierarchy) <- tolower(colnames(hierarchy))
    
    x <- colnames(hierarchy)
    x <- gsub("[^a-z_]*","",x)
    X <- c("page_id","buy_id","channel","sub_channel")
    
    if(any(!(X %in% x))) {
      stop(
        "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'page_id', 'buy_id', 'channel', 'sub_channel'")
      failure <- 1
    }
    
    
    X <- any(duplicated(hierarchy$page_id))
    
    if(X) {
      warning(
        "WARNING!!! There are duplicate page_ids in your hierarchy. This may cause duplication issues when merging onto your events."
      )
    }
    
    ## PPC bit
    
    if(ppc_match) {
      
      X <- is.null(ppc_hierarchy_location)
      if(X) {stop(
        "WHAT WENT WRONG: You haven't specified a ppc_hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the ppc_hierarchy csv.")}
      
      
      hierarchy <- read.csv(ppc_hierarchy_location)
      colnames(hierarchy) <- tolower(colnames(hierarchy))
      
      x <- colnames(hierarchy)
      x <- gsub("[^a-z_]*","",x)
      X <- c(ppc_match_col,"channel","sub_channel")
      
      if(any(!(X %in% x))) {
        stop(
          "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'channel', 'sub_channel', and either 'paid_search_campaign' or 'paid_search_ad_group',whichever you specified ppc_match_col to be.")
        failure <- 1
      }
      
      
      X <- any(duplicated(hierarchy[,ppc_match_col]))
      
      if(X) {
        warning(
          "WARNING!!! There are duplicate entries of ppc_match_col in your hierarchy. This may cause duplication issues when merging onto your events. CHECK that there is only one channel/sub_channel per ppc_match_col or results WILL be wrong."
        )
      }
    }
    
    
    
  }
  
  
  return(failure)
  }


#' Does checks in the red function for stuff that will cause errors

#'

#' Does checks in the red function for stuff that will cause errors

#'

#' @param startDate
#' @param endDate
#' @param startPre
#' @param endPre
#' @param hierarchy_location
#' @param ppc_match
#' @param ppc_hierarchy_location
#' @param reupload_hierarchy
#' 
#' @return Summary statistics of the run process. The data is saved down in the file_save_location rather than outputted to the console.

#' 

checks_function_keyword_OMD <- function(startDate,endDate,
                                    startPre,endPre,
                                    reupload_hierarchy,
                                    ppc_match,
                                    hierarchy_location,
                                    ppc_hierarchy_location,
                                    ppc_match_col) {
  
  ## Dates - check they're valid:
  failure <- 0
  
  sd <- try( as.Date( startDate, format= "%Y-%m-%d" ) )
  ed <- try( as.Date( endDate, format= "%Y-%m-%d" ) )
  sp <- try( as.Date( startPre, format= "%Y-%m-%d" ) )
  ep <- try( as.Date( endPre, format= "%Y-%m-%d" ) )
  
  if(class(sd)=="try-error" || is.na(sd)) {
    stop("WHAT WENT WRONG: startDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(sp)=="try-error" || is.na(sp)) {
    stop("WHAT WENT WRONG: startPre must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  if(class(ed)=="try-error" || is.na(ed)) {
    stop("WHAT WENT WRONG: endDate must be a valid date, in YYYY-MM-DD format" )
    failure <- 1}
  
  if(class(ed)=="Date"&class(sd)=="Date" & ed<sd) {
    stop("WHAT WENT WRONG: startDate must be before endDate")
    failure <- 1}
  if(class(ep)=="Date"&class(sp)=="Date" & ep<sp) {
    stop("WHAT WENT WRONG: startPre must be before endPre")
    failure <- 1}
  if(class(ep)=="Date"&class(sd)=="Date" & sd<ep) {
    stop("WHAT WENT WRONG: startDate must be after endPre")
    failure <- 1
  }
  
  if(!reupload_hierarchy) {
    X <-  try({
      x <- dbSendQuery(redshift_con,paste0("
                                           select 1 from ds_",agency,".dc_",analyst,"_",DPID,"_category"))
      x <- fetch(x,n=-1)},
      silent=TRUE)
    
    if(class(X)=="try-error") {
      stop(
        "WHAT WENT WRONG: Hierarchy is not present in redshift, put reupload_hierarchy=T and set the hierarchy locations appropriately")
      failure <- 1
    }
  }
  
  
  if(reupload_hierarchy) {
    
    X <- is.null(hierarchy_location)
    if(X) {stop(
      "WHAT WENT WRONG: You haven't specified a hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the csv.")}
    
    
    hierarchy <- read.csv(hierarchy_location)
    colnames(hierarchy) <- tolower(colnames(hierarchy))
    
    x <- colnames(hierarchy)
    x <- gsub("[^a-z_]*","",x)
    X <- c("placement_id","campaign_id","channel","sub_channel")
    
    if(any(!(X %in% x))) {
      stop(
        "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'placement_id', 'campaign_id', 'channel', 'sub_channel'")
      failure <- 1
    }
    
    
    X <- any(duplicated(hierarchy$placement_id))
    
    if(X) {
      warning(
        "WARNING!!! There are duplicate placement_ids in your hierarchy. This may cause duplication issues when merging onto your events."
      )
    }
    
    ## PPC bit
    
    if(ppc_match) {
      
      X <- is.null(ppc_hierarchy_location)
      if(X) {stop(
        "WHAT WENT WRONG: You haven't specified a ppc_hierarchy location. Either set reupload_hierarchy=F if it's already on redshift in the right format, or keep reupload_hierarchy=T and specify the location of the ppc_hierarchy csv.")}
      
      
      hierarchy <- read.csv(ppc_hierarchy_location)
      colnames(hierarchy) <- tolower(colnames(hierarchy))
      
      x <- colnames(hierarchy)
      x <- gsub("[^a-z_]*","",x)
      X <- c(ppc_match_col,"channel","sub_channel","placement_id")
      
      if(any(!(X %in% x))) {
        stop(
          "WHAT WENT WRONG: You don't have the right column names in your hierarchy! Needs to be 'channel', 'sub_channel','placement_id' and either 'paid_search_campaign' or 'paid_search_ad_group',whichever you specified ppc_match_col to be.")
        failure <- 1
      }
      
      
      X <- any(duplicated(hierarchy[,ppc_match_col]))
      
      if(X) {
        warning(
          "WARNING!!! There are duplicate entries of ppc_match_col in your hierarchy. This may cause duplication issues when merging onto your events. CHECK that there is only one channel/sub_channel per ppc_match_col or results WILL be wrong."
        )
      }
    }
    
    
    
  }
  
  
  return(failure)
  }

