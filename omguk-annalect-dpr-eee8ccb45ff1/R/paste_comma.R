#' pastes together objects with commas

#'

#' paste together objects with commas

#'

#' @param x - the data

#' @return The pasted data

#' @examples

#' y <- c(sample(LETTERS,20,replace=T))

#' paste_comma(y)
#' 
paste_comma       <- function(...) {paste(...,sep=", ")}
