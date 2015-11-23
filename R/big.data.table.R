selfNames = function(x) setNames(x, x)
rbindlapply = function(X, FUN, ..., use.names = fill, fill = FALSE, idcol = NULL) rbindlist(lapply(X = X, FUN = FUN, ... = ...), use.names=use.names, fill=fill, idcol=idcol)

force.data.table = function(x){
    setattr(x, "class", c("data.table","data.frame"))
}

dim.big.data.table = function(x){
    nc = length(x)
    nr = sapply(attr(x,"rscl"), RS.eval, nrow(x))
    c(sum(nr), nc)
}
print.big.data.table = function(x, ...){
    print(setDT(unclass(x)))
}
str.big.data.table = function(object, ...){
    str(setDT(unclass(object)))
}

#' @title Test if big.data.table
#' @param x R object.
#' @return TRUE if *x* inherits from *big.data.table*.
is.big.data.table = function(x){
    inherits(x, "big.data.table")
}

#' @title big.data.table evaluate on nodes
#' @description Main engine for passing queries to nodes, control parallelization, rbinding or simplifing returned object. Allows to measure timing and verbose messages.
#' @param x big.data.table.
#' @param expr expression.
#' @param qexpr quoted expression.
#' @param send logical, if TRUE submit expression appended with `TRUE` to not fetch potentially big results from provided *expr*, useful for data.table *set** or `:=` functions.
#' @param simplify logical if *TRUE* (default) it will simplify list of 1 length same type objects to vector.
#' @param rbind logical if *TRUE* (default) results are data.table they will be rbinded.
#' @param try should be wrapped into tryCatch for errors and warnings? default (TRUE), means that warnings will not return the actual value.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @return Depending on *simplify, rbind* the results of evaluated expression.
bdt.eval = function(x, expr, qexpr, send = FALSE, simplify = TRUE, rbind = TRUE, try = FALSE, parallel = TRUE){
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = TRUE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    if(isTRUE(getOption("bigdatatable.verbose"))){
        nnodes = length(rscl)
        ts = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) microbenchmarkCore::get_nanotime() else proc.time()[[3L]]
        cat(sprintf("big.data.table: submitting data.table queries to %s nodes %s.\n", nnodes, ifelse(isTRUE(parallel), "in parallel", "sequentially"), sep=""))
    }
    expr = if(!missing(qexpr)) qexpr else substitute(expr)
    if(!missing(send) && isTRUE(send)) expr = call("{",expr, TRUE)
    if(!missing(try) && isTRUE(try)) expr = substitute(tryCatch(expr, error = function(e) e, warning = function(w) w), list(expr=expr))
    if(!parallel){
        x = lapply(rscl, RS.eval, expr, lazy = FALSE)
    }
    if(parallel){
        invisible(lapply(rscl, RS.eval, expr, wait = FALSE, lazy = FALSE))
        x = lapply(rscl, RS.collect)
    }
    if(isTRUE(getOption("bigdatatable.verbose"))){
        timing = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) (microbenchmarkCore::get_nanotime() - ts) * 1e-9 else proc.time()[[3L]] - ts
        cat(sprintf("big.data.table: data.table queries collected from %s nodes in %.4f seconds.\n", nnodes, timing), sep="")
    }
    if(rbind && is.data.table(x[[1L]])){
        if(isTRUE(getOption("bigdatatable.verbose"))){
            ts = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) microbenchmarkCore::get_nanotime() else proc.time()[[3L]]
            cat(sprintf("big.data.table: row bind data collected from %s nodes.\n", nnodes, sep=""))
        }
        x = rbindlist(x)
        if(isTRUE(getOption("bigdatatable.verbose"))){
            timing = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) (microbenchmarkCore::get_nanotime() - ts) * 1e-9 else proc.time()[[3L]] - ts
            cat(sprintf("big.data.table: data binded in %.4f seconds.\n", timing), sep="")
        }
    } else if(simplify && length(x[[1L]])==1 && is.atomic(x[[1L]])){
        if(all(sapply(x, length)==1L) && all(sapply(x, is.atomic)) && length(unique(sapply(x, typeof)))==1L) x = simplify2array(x)
    }
    return(x)
}

bdt.assign = function(x, name, value, parallel = TRUE){
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = FALSE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    nnodes = length(rscl)
    if(is.data.table(value) && isTRUE(getOption("bigdatatable.verbose"))){
        ts = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) microbenchmarkCore::get_nanotime() else proc.time()[[3L]]
        cat(sprintf("big.data.table: assigning data.table to %s nodes %s.\n", nnodes, ifelse(isTRUE(parallel), "in parallel", "sequentially"), sep=""))
    }
    partitions = if(is.big.data.table(x)) attr(x, "partitions") else data.table(NULL)
    if(!is.data.table(value)){
        value = rep(value, nnodes)
        rscid = seq_len(nnodes)
    }
    if(is.data.table(value)){
        rscid = seq_len(min(nnodes, nrow(value)))
        if(length(partitions)){
            value = lapply(rscid, function(i){
                value[partitions[i], nomatch = 0L, on = key(partitions)]
            })
        }
        if(!length(partitions)){
            partition.map = cut(seq_len(nrow(value)), rscid[length(rscid)], labels = FALSE)
            value = lapply(rscid, function(i){
                value[partition.map==i]
            })
        }
    }
    if(!parallel){
        x = lapply(rscid, function(i) RS.assign(rsc = rscl[[i]], name = name, value = value[[i]], wait = TRUE))
    }
    if(parallel){
        invisible(lapply(rscid, function(i) RS.assign(rsc = rscl[[i]], name = name, value = value[[i]], wait = FALSE)))
        x = lapply(rscl[rscid], RS.collect)
    }
    if(isTRUE(getOption("bigdatatable.verbose"))){
        timing = if(requireNamespace("microbenchmarkCore", quietly = TRUE)) (microbenchmarkCore::get_nanotime() - ts) * 1e-9 else proc.time()[[3L]] - ts
        cat(sprintf("big.data.table: data.table assigned to %s nodes in %.4f seconds.\n", nnodes, timing), sep="")
    }
    return(x)
}

#' @title Controls partitioning
#' @description By default function will validate if data partitioning is enforced on the nodes. It can also rearrange data between nodes to update partitioning.
#' @param x big.data.table.
#' @param partition.by character.
#' @param copy logical, default FALSE, or a data.table.
#' @param validate if TRUE (default) it will validate that data are correctly partitioned.
#' @param parallel, see `bdt.eval`.
#' @return big.data.table
bdt.partition = function(x, partition.by, copy = FALSE, validate = TRUE, parallel = TRUE){
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = FALSE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    nnodes = length(rscl)
    partitions = if(is.big.data.table(x)) attr(x, "partitions") else data.table(NULL)
    if(!missing(partition.by)){
        if(!identical(partition.by, nondotnames(partitions))) warning("Provided partition.by does not match to currently declared partitions, they will be updated, if ", immediate. = TRUE)
        partition.by = nondotnames(partitions)
        # update partitions
        qcall = substitute(unique(x, by = partition.by)[, c(partition.by), with=FALSE], list(partition.by=partition.by))
        x = big.data.table(x = force.data.table(x)[0L], rscl = rscl, partitions = unique(bdt.eval(x, qexpr = qcall, parallel=parallel), by = partition.by))
    }
    if(isTRUE(copy)){
        lapply(seq_len(nnodes), function(i){
            # copy 'x' to local session
            cat(sprintf("big.data.table: processing %s of %s nodes.\n", i, nnodes))
            # check if anything to copy
            qcall = substitute(x[!partition.key], list(partition.key=partitions[i]))
            tmp = bdt.eval(rscl[i], qexpr = qcall, parallel=FALSE)
            if(nrow(tmp)){
                # send it to potentially multiple nodes
                bdt.assign(x, name = "x", tmp, parallel = parallel)
            }
            TRUE
        })
    }
    if(is.data.table(copy)){
        bdt.assign(x, name = "x", copy, parallel = parallel)
    }
    if(validate){
        qcall = quote(nrow(unique(x, by = partition.by))==1L)
        r = bdt.eval(x, qexpr = qcall, parallel = parallel)
        if(!all(r)) stop(sprintf("big.data.table partitioning has been finished but validation of data didn't pass for %s nodes.", which(!r)))
    }
    return(x)
}

#' @title Subset from big.data.table
#' @param x big.data.table object.
#' @param ... arguments passed to each node `[.data.table` call: *i, j, by, keyby...*.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @note Results from nodes are rbinded and the same call is evalated on combined results. That means the column names cannot be renamed or simplified to vector in `...` call. Use `[[.big.data.table` for deeper flexibility.
#' @return data.table object.
"[.big.data.table" = function(x, ..., parallel = TRUE){
    dtq = match.call(expand.dots = FALSE)$`...`
    dtcall = as.call(c(list(as.symbol("["), x = as.name("x")), dtq))
    # bdt node eval
    x = bdt.eval(x, qexpr = dtcall, parallel = parallel)
    # aggregate results from nodes
    return(eval(dtcall))
}

#' @title Extract from big.data.table
#' @param x big.data.table object.
#' @param j numeric scalar, if provided then all other arguments are ignored and subset behaves the same way as for data.table, but returns 0 length column.
#' @param expr expression to be evaluated on node where data.table objects are stored as `x` variable in `.GlobalEnv`.
#' @param send logical, if TRUE submit expression appended with `TRUE` to not fetch potentially big results from provided *expr*, useful for data.table *set** or `:=` functions.
#' @param i numeric restrict expression to particular nodes
#' @param ... ignored.
#' @param simplify logical passed to `bdt.eval`, affects the type of returned object.
#' @param rbind logical passed to `bdt.eval`, affects the type of returned object.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @return When using *j* arg the 0 length variable from underlying data is returned. Otherwise the results from expression evaluated as `lapply*. When using *rbind* or *simplify* the returned list be can simplified.
"[[.big.data.table" = function(x, j, expr, send = FALSE, i, ..., simplify = TRUE, rbind = TRUE, parallel = TRUE){
    # when `j` provided it return empty column from bdt to get a class of column
    if(!missing(j) && !is.null(j) && is.numeric(j)) return(unclass(x)[[j]])
    qexpr = substitute(expr)
    rscl = attr(x, "rscl")
    if(missing(i)) i = seq_along(rscl)
    bdt.eval(rscl[i], qexpr = qexpr, send = send, simplify = simplify, rbind = rbind, parallel = parallel)
}
