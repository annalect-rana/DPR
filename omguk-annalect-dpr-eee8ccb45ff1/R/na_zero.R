#' Replace NA's with zeroes

#'

#' Replace NA's with zeroes

#'

#' @param x - the data with NAs

#' @return The data with zeroes instead

#' @examples

#' y <- data.frame(a=sample(c(1,2,3,NA),10,replace=T),
#'                 b = sample(c(1,2,3,4,NA),10,replace=T))

#' na.zero(y)

na.zero <- function(x) {
  x[is.na(x)] <- 0
  x
}