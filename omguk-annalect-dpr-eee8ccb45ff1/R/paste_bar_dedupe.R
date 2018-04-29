#' paste deduped values with "|"

#'

#' Dedupes consecutive values and pastes together with "|"

#'

#' @param x - the data

#' @return The pasted data as a string

#' @examples

#' y <- c(sample(LETTERS[1:10],20,replace=T))

#' paste_bar_dedupe(y)
#' 
paste_bar_dedupe <- function(x) {
  
  y <- x[cumsum(rle(as.character(x))$lengths)]
  paste(y,collapse="|")
  
}