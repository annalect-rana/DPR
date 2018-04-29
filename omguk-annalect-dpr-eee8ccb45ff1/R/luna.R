#' Find the length of the unique values in an object

#'

#' Find the length of the unique values in an object

#'

#' @param x - the data

#' @return The count of unique values

#' @examples

#' y <- c(sample(1:50,20,replace=T))

#' luna(y)
luna = function(x){length(unique(x))}
