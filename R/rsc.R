
# RserveConnection list ----

rsc = function(port = 6311L, host = NULL, tls = FALSE, proxy.target = NULL, proxy.wait = TRUE){
    # TO DO this needs to have nice recycling of attributes to easy setup set of nodes
    stopifnot(as.logical(length(port)))
    rscl = lapply(setNames(port, port), function(port) RS.connect(host = host, port = port, tls = tls, proxy.target = proxy.target, proxy.wait = proxy.wait))
    r = sapply(rscl, RS.eval, as.logical(suppressPackageStartupMessages(require(data.table))))
    if(!all(r)) stop("Some nodes failed to load data.table.")
    rscl
}
is.rsc = function(x, silent=TRUE){
    if(silent) return(is.list(x) && length(x) && all(sapply(x, inherits, "RserveConnection")))
    if(!is.list(x)) stop(sprintf("Rserve connection list must be of type list."))
    if(!length(x)) stop(sprintf("Rserve connection list cannot have 0 length."))
    if(!all(sapply(x, inherits, "RserveConnection"))) stop(sprintf("Rserve connection list must store only 'RserveConnection' class objects."))
    return(TRUE)
}
