
# RserveConnection list ----

#' @title Connect to Rserve instances
#' @description A wrapper to `RS.connect` function using `lapply` over recycled input arguments. It also requires data.table package.
#' @param port integer vector of port numbers
#' @param host character scalar, soon will support vector.
#' @param tls logical, see `?RSclient::RS.connect` for details.
#' @param proxy.target logical, see `?RSclient::RS.connect` for details.
#' @param proxy.wait logical, see `?RSclient::RS.connect` for details.
#' @return List of active connections to Rserve nodes.
rsc = function(port = 6311L, host = NULL, tls = FALSE, proxy.target = NULL, proxy.wait = TRUE){
    # TO DO this needs to have nice recycling of attributes to easy setup set of nodes
    stopifnot(as.logical(length(port)))
    rscl = lapply(setNames(port, port), function(port) RS.connect(host = host, port = port, tls = tls, proxy.target = proxy.target, proxy.wait = proxy.wait))
    # workaround check warning for use of "require"
    qrequire = substitute(as.logical(suppressPackageStartupMessages(req.dt)),
                          list(req.dt = call("require", package="data.table", quietly=TRUE, character.only=TRUE)))
    r = sapply(rscl, RS.eval, qrequire, lazy = FALSE)
    if(!all(r)) stop("Some nodes failed to load data.table.")
    rscl
}

#' @title Check if list stores Rserve connections
#' @param x a list
#' @param silent logical default TRUE, if FALSE then error are raised if list is not Rserve connections list.
#' @return TRUE, FALSE or raise error if `silent=FALSE`.
is.rsc = function(x, silent=TRUE){
    if(silent) return(is.list(x) && length(x) && all(sapply(x, inherits, "RserveConnection")))
    if(!is.list(x)) stop(sprintf("Rserve connection list must be of type list."))
    if(!length(x)) stop(sprintf("Rserve connection list cannot have 0 length."))
    if(!all(sapply(x, inherits, "RserveConnection"))) stop(sprintf("Rserve connection list must store only 'RserveConnection' class objects."))
    return(TRUE)
}

#' @title Force collect results to unlock error
#' @param rscl list of connections to R nodes
#' @return Results from `try` `RS.collect` from each node, simplified if possible.
clean = function(rscl){
    sapply(rscl, function(rsc) try(RS.collect(rsc), silent=TRUE))
}
