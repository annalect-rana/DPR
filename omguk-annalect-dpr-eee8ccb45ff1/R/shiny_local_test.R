
#' Tests the shiny dashboard on preprepared datasets from blue2dash
#'
#' Tests the shiny dashboard on preprepared datasets from blue2dash
#' This function will launch the app code from a shared location (not from the server) to run on the specified data, so you can test all the outputs are A-OK. If they're not, email tim.scholtes@@annalect.com with the details.
#' 
#' @param dashboard_file_location The full path of the start_data.RData file from blue2dash()
#' @param app_location Where is the app stored? This defaults to the current location, but if there is an alternative app that has been created you can point it here
#' 
#' @return Returns nothing, but will launch the app.

#' @examples
#' 
#' #shiny_local_test(
#' #	dashboard_file_location="Z:/GROUP/TIM/shiny/blue2dash_testing/HP/start_data.RData"
#' #	)

shiny_local_test <- function(
	dashboard_file_location=NULL,
	app_location="//ukncsas01/DATASCIENCE/SHARED/DP/DPR_shinydashboard/") {
print(dashboard_file_location)

	load(file=dashboard_file_location,envir = globalenv())
	#save(dashboard_file_location,file=file.path(app_location,"data/dashboard_file_location.RData"))
	runApp(app_location,launch.browser=T)
}