% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/S3_publish.R
\name{S3_publish}
\alias{S3_publish}
\title{Uploads prepared datasets for DP shiny dashboards to the relevant S3 bucket for the server app to find}
\usage{
S3_publish(data_location, agency, client_name, DPID, KeyID, SecretAccessKey)
}
\arguments{
\item{data_location}{The full path of the start_data.RData file from blue2dash()}

\item{agency}{which agency you are in (lowercase) - allowed values are "omd","psa","m2m","mg","roc","lgi","phd"}

\item{client_name}{What is the name of the client? This is important for data partitioning. If you already have a client DP up in S3, then please use the same name.}

\item{DPID}{Character vector of the DPID. Please use something sensible, as this is what will be used in the app to select the data and also partition the files. Please no whitespaces or slashes. eg. May2016}

\item{KeyID}{Your AWS KeyID - this should be your personal key and match the agency S3 bucket you're looking to upload to}

\item{SecretAccessKey}{Your AWS SecretAccessKey - this should be your personal SecretAccessKey and match the agency S3 bucket you're looking to upload to}
}
\value{
Returns nothing, only has side-effect of saving down to S3
}
\description{
Uploads prepared datasets for DP shiny dashboards to the relevant S3 bucket for the server app to find
This function simply does an upload, the data should have been previously checked within shiny_local_test()
}
\examples{

#S3_publish(data_location="Z:/GROUP/TIM/shiny/blue2dash_testing/HP/start_data.RData",
#           agency="m2m",
#		   client_name="RCI",
#		   DPID="SEP_DEC_15"
#		   KeyID = XXX
#		   SecretAccessKey = XXX)
}
