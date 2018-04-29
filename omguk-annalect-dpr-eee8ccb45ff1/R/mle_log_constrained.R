#' Fit the logistic regression

#'

#' This function will fit

#' summary statistics and transformation of the data

#'

#' @param fmla - a formula string with the model to fit
#' @param data - the data which matches the fmla
#' @param maxit - maximum number of iterations
#' @param trace - BOOLEAN. True by default. TRUE gives a trace of the iterations into the console, FALSE gives no trace.

#' @return A list containing the coefficients and the deviance

#' @examples

#' fmla <- as.formula("conv ~ sub_ch_A_1 + sub_ch_B_1 + sub_ch_A_2 + sub_ch_B_2")

#' data <- data.frame(conv=sample(c(0,1),size=1e3,replace=T),
#'                       sub_ch_A_1 = sample(0:4,size=1e3,replace=T),
#'                       sub_ch_B_1 = sample(0:4,size=1e3,replace=T),
#'                       sub_ch_A_2 = sample(0:4,size=1e3,replace=T),
#'                       sub_ch_B_2 = sample(0:4,size=1e3,replace=T))
#'                       
#' out <- mle_logreg_constrained(fmla,data)


mle_logreg_constrained = function(fmla, data,maxit=500, trace=TRUE)
{
  # Define the negative log likelihood function
  logl <- function(theta,x,y){
    y <- y
    x <- as.matrix(x)
    beta <- theta[1:ncol(x)]
    
    # Use the log-likelihood of the Bernouilli distribution, where p is
    # defined as the logistic transformation of a linear combination
    # of predictors, according to logit(p)=(x%*%beta)
    loglik <- sum(-y*log(1 + exp(-(x%*%beta))) - (1-y)*log(1 + pmin(1e9,exp(x%*%beta))))
    return(-loglik)
  }
  
  
  grfun <- function(theta,x,y) {
    y <- y
    x <- as.matrix(x)
    beta <- theta[1:ncol(x)]
    
    gr <- colSums(sweep(x,1,(y-(1/(1+exp(-(x%*%beta))))),"*"))
    return(-gr)
  }
  # Prepare the data
  outcome = rownames(attr(terms(fmla),"factors"))[1]
  dfrTmp = model.frame(data)
  x = as.matrix(model.matrix(fmla, data=dfrTmp))
  y = as.numeric(as.matrix(data[,match(outcome,colnames(data))]))
  
  # Define initial values for the parameters
  theta.start = rep(0.1,(dim(x)[2]))
  names(theta.start) = colnames(x)
  
  # Non-negative slopes constraint
  lower = c(-Inf,rep(1e-10,(length(theta.start)-1)))
  
  trace_level<-ifelse(trace==TRUE, 3, 0)
  
  # Calculate the maximum likelihood
  mle = optim(theta.start,logl,gr = grfun,x=x,y=y,lower=lower,
              method="L-BFGS-B",control=list(trace=trace_level,maxit=maxit))
  beta = mle$par
  
  
  # Return estimates
  return(out = list(beta=beta,dev=2*mle$value))
}