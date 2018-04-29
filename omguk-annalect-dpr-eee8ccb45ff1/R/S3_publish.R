
#' Uploads prepared datasets for DP shiny dashboards to the relevant S3 bucket for the server app to find
#'
#' Uploads prepared datasets for DP shiny dashboards to the relevant S3 bucket for the server app to find
#' This function simply does an upload, the data should have been previously checked within shiny_local_test()
#' 
#' @param data_location The full path of the start_data.RData file from blue2dash()
#' @param agency which agency you are in (lowercase) - allowed values are "omd","psa","m2m","mg","roc","lgi","phd"
#' @param client_name What is the name of the client? This is important for data partitioning. If you already have a client DP up in S3, then please use the same name.
#' @param DPID Character vector of the DPID. Please use something sensible, as this is what will be used in the app to select the data and also partition the files. Please no whitespaces or slashes. eg. May2016
#' @param KeyID Your AWS KeyID - this should be your personal key and match the agency S3 bucket you're looking to upload to
#' @param SecretAccessKey Your AWS SecretAccessKey - this should be your personal SecretAccessKey and match the agency S3 bucket you're looking to upload to
#' 
#' @return Returns nothing, only has side-effect of saving down to S3

#' @examples
#' 
#' #S3_publish(data_location="Z:/GROUP/TIM/shiny/blue2dash_testing/HP/start_data.RData",
#' #           agency="m2m",
#' #		   client_name="RCI",
#' #		   DPID="SEP_DEC_15"
#' #		   KeyID = XXX
#' #		   SecretAccessKey = XXX)

S3_publish <- function(
	data_location, 			# where is the data stored
	agency, 				# what agency are you at
	client_name, 			# what client does the data relate too 
	DPID,					# what is the DPID
	KeyID,					# user keyID
	SecretAccessKey			# user secret access key
	) {

# to be used in apps.annalect form
#	user_list <- read.csv("//ukncsas01/DATASCIENCE/SHARED/DP/DPR_shinydashboard/data/user_list.csv")
	
	if(!agency %in% c("omd","psa","m2m","mg","roc","lgi","phd")) {
		stop(
			paste("Not a valid agency name. Allowed agency names are:",
				paste(c("omd","psa","m2m","mg","roc","lgi","phd"),collapse=", ")))}

#	for when we have user client lists
#	if(!client_name %in% user_list$client[user_list$agency==agency]) {
#		stop(
#			paste("Not a valid Client name. Allowed client names are:",
#				paste(user_list$client[user_list$agency==agency],collapse=", ")))}

if(agency %in% c("psa","lgi")) {
		s3Loc <- paste0(
		"s3://",agency,"-visualisation/DP/",
		client_name,"/",
		DPID,"/")
	} else {
	s3Loc <- paste0(
		"s3://",agency,"-uk-visualisation/DP/",
		client_name,"/",
		DPID,"/")
}
 
	s3UploadFile(KeyID,SecretAccessKey,s3Loc,data_location)
}
