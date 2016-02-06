
# rscl.* vectorized RS.* ----

#' @title Connect to Rserve instances
#' @description A wrapper to `RS.connect` function using `sapply` over recycled input arguments. It also requires data.table package.
#' @param port integer vector of port numbers
#' @param host character scalar, soon will support vector.
#' @param tls logical, see `?RSclient::RS.connect` for details.
#' @param proxy.target logical, see `?RSclient::RS.connect` for details.
#' @param proxy.wait logical, see `?RSclient::RS.connect` for details.
#' @param pkgs character vector of packages to load after connect to node.
#' @return List of active connections to Rserve nodes.
rscl.connect = function(port = Sys.getenv("RSERVE_PORT", "6311"), host = Sys.getenv("RSERVE_HOST", "127.0.0.1"), tls = FALSE, proxy.target = NULL, proxy.wait = TRUE, pkgs = character()){
    # - [x] nice recycling of attributes to easy setup set of nodes
    lp = length(port)
    lh = length(host)
    stopifnot(lp >= 1L, lh >= 1L)
    nm = NULL
    if(lh != lp){
        stopifnot(lp == 1L | lh == 1L)
        if(lp > lh){
            host = rep(host, lp)
            nm = port
        } else if(lh > lp){
            port = rep(port, lh)
            nm = host
        }
    }
    rscl = lapply(
        setNames(seq_along(host), nm),
        function(i) tryCatch(RS.connect(host = host[[i]], port = port[[i]], tls = tls, proxy.target = proxy.target, proxy.wait = proxy.wait), error = function(e) e)
    )
    err = sapply(rscl, inherits, "error")
    if(any(err)) stop(sprintf("Failed to connect to %s node%s from the list.", paste(which(err), collapse=", "), if(sum(err) > 1L) "s" else ""))
    if(length(pkgs)){
        stopifnot(is.character(pkgs))
        rscl.require(rscl, pkgs, quietly = FALSE)
    }
    rscl
}

#' @title Disconnect from Rserve instances
#' @description A wrapper to `RS.close`.
#' @param rscl lists of Rserve connections.
rscl.close = function(rscl){
    sapply(rscl, RS.close)
}

#' @title Eval on list of Rserve instances
#' @description A wrapper to `RS.eval`.
#' @param rscl lists of Rserve connections.
#' @param x expression, can be quoted then use *lazy* arg. Can be list of quoted expression to run on all nodes in by expression batches on all R nodes.
#' @param wait logical default TRUE passed to `RS.eval`. If *parallel* arg is TRUE then this argument is ignored.
#' @param lazy logical default TRUE, when FALSE then *x* is quoted or a list of quoted.
#' @param parallel logical, default FALSE, when TRUE it will auto collect results
#' @param simplify logical, default TRUE, passed to underlying `sapply`.
#' @return Logical matrix.
rscl.eval = function(rscl = getOption("bigdatatable.rscl"), x, wait = TRUE, lazy = TRUE, parallel = FALSE, simplify = TRUE){
    stopifnot(is.list(rscl), is.logical(wait), is.logical(lazy), is.logical(parallel), is.logical(simplify))
    expr = if(isTRUE(lazy)) substitute(x) else x
    # - [x] allow to inject any expression and every `.expr` symbol will be substituted with actual `x` expression, a way to push down the logR call for cleaner logs
    # returns
    if(parallel){
        invisible(sapply(rscl, RS.eval, expr, wait = FALSE, lazy = FALSE))
        rscl.collect(rscl, simplify = simplify)
    } else {
        sapply(rscl, RS.eval, expr, wait = wait, lazy = FALSE, simplify = simplify)
    }
}

#' @title `RS.assign` for list of Rserve connections
#' @description Wrapper on `RS.assign` for list of Rserve connections.
#' @param rscl, lists of Rserve connections.
#' @param name character variable of name to be used in each node for new object.
#' @param value object to be assigned to each node.
#' @param wait logical default TRUE passed to `RS.eval`.
#' @param parallel logical, default FALSE, when TRUE it will auto collect results
#' @param simplify logical, default TRUE, passed to underlying `sapply`.
#' @return Results from `RS.assign` in `sapply`.
rscl.assign = function(rscl = getOption("bigdatatable.rscl"), name, value, wait = TRUE, parallel = FALSE, simplify = TRUE){
    stopifnot(is.list(rscl), is.character(name), is.logical(wait), is.logical(parallel), is.logical(simplify))
    # returns
    if(parallel){
        invisible(sapply(rscl, RS.assign, name = name, value = value, wait = FALSE))
        rscl.collect(rscl, simplify = simplify)
    } else {
        sapply(rscl, RS.assign, name = name, value = value, wait = wait, simplify = simplify)
    }
}

#' @title Collect results from Rserve connection list
#' @param rscl list of connections to R nodes
#' @param timeout numeric passed to `RS.collect`.
#' @param detail logical passed to `RS.collect`.
#' @param try logical default TRUE, will wrap collection into `try` to finish collection from all nodes instead of aborting on error.
#' @param simplify logical, default TRUE, passed to underlying `sapply`.
#' @return Results from `try` `RS.collect` from each node, simplified if possible.
rscl.collect = function(rscl = getOption("bigdatatable.rscl"), timeout = Inf, detail = FALSE, try = TRUE, simplify = TRUE){
    if(try){
        sapply(rscl, function(rsc) base::try(RS.collect(rsc, timeout = timeout, detail = detail), silent=TRUE), simplify = simplify)
    } else {
        sapply(rscl, RS.collect, timeout = timeout, detail = detail, simplify = simplify)
    }
}

# rscl wrappers ----------------------------------------------------------------

#' @title Check if list stores Rserve connections
#' @param x a list
#' @param silent logical default TRUE, if FALSE then error are raised if list is not Rserve connections list.
#' @return TRUE, FALSE or raise error if `silent=FALSE`.
is.rscl = function(x, silent=TRUE){
    if(silent) return(is.list(x) && length(x) && all(sapply(x, inherits, "RserveConnection")))
    if(!is.list(x)) stop(sprintf("Rserve connection list must be of type list."))
    if(!length(x)) stop(sprintf("Rserve connection list cannot have 0 length."))
    if(!all(sapply(x, inherits, "RserveConnection"))) stop(sprintf("Rserve connection list must store only 'RserveConnection' class objects."))
    return(TRUE)
}

#' @title `ls` on every node
#' @param rscl list of connections to R nodes
#' @param simplify logical default TRUE passed to `sapply`.
#' @return `ls()` of `.GlobalEnv` from each node.
rscl.ls = function(rscl = getOption("bigdatatable.rscl"), simplify = TRUE){
    rscl.eval(rscl, ls(envir = .GlobalEnv), simplify = simplify)
}

#' @title `ls.str` on every node
#' @param rscl list of connections to R nodes
#' @return Prints of `ls.str` on all nodes as side effect
rscl.ls.str = function(rscl = getOption("bigdatatable.rscl")){
    prntl = rscl.eval(rscl, capture.output(print(ls.str(envir = .GlobalEnv))), simplify = FALSE)
    nm = names(prntl)
    if(is.null(nm)) nm = seq_along(prntl)
    prntl = lapply(seq_along(prntl), function(i) c(sprintf("# Rserve node %s ----", nm[i]), prntl[[i]]))
    cat(unlist(prntl), sep = "\n")
}

#' @title Require packages in list of Rserve instances
#' @description A wrapper to `RS.eval`.
#' @param rscl, lists of Rserve connections.
#' @param package character vector of packages to require on each of R node.
#' @param quietly logical defaul TRUE, no warning.
#' @return Logical matrix.
rscl.require = function(rscl = getOption("bigdatatable.rscl"), package, quietly = TRUE){
    stopifnot(is.rscl(rscl), is.character(package), length(package) > 0L, is.logical(quietly))
    # workaround for CHECK warning when use "require" directly
    require_call = function(package, quietly){
        substitute(
            as.logical(suppressPackageStartupMessages(req.call)), 
            list(req.call = call("require", package=package, quietly=quietly, character.only=TRUE))
        )
    }
    expr_list = lapply(setNames(nm = package), require_call, quietly = quietly)
    r = sapply(expr_list, function(expr) rscl.eval(rscl, expr, lazy = FALSE))
    if(!quietly && !all(r)) warning("Some nodes failed to load, check logical matrix returned.")
    r
}
