#' paste together with "|"

#'

#' paste together with "|"

#'

#' @param x - the data

#' @return The pasted data as a string

#' @examples

#' y <- c(sample(LETTERS,20,replace=T))

#' paste_bar(y)
#' 
paste_bar <- function(x) {
  paste(rev(x),collapse="|")}