#' @title big.data.table-package
#' @description Leverage data.table onto set of Rserve nodes.
#' @details
#' Use `as.big.data.table` methods to extract/load/assign/populate data.
#' Methods for `function` and `call` will be evaluated remotly, method for `data.table` will be evaluated locally and assigned to nodes.
#' @aliases bdt big.dt big.data.table
#' @docType package
#' @author Jan Gorecki
#' @name big.data.table-package
NULL

selfNames = function(x) setNames(x, x)

#' @title Row bind lapply results
#' @description Wrapper on `rbindlist(lapply(...))`.
#' @param X vector (atomic or list) passed to `lapply`.
#' @param FUN function passed to `lapply`.
#' @param \dots optional arguments to `FUN`.
#' @param use.names logical passed to `rbindlist`.
#' @param fill logical passed to `rbindlist`.
#' @param idcol logical or  character passed to `rbindlist`.
rbindlapply = function(X, FUN, ..., use.names = fill, fill = FALSE, idcol = NULL) rbindlist(lapply(X = X, FUN = FUN, ... = ...), use.names=use.names, fill=fill, idcol=idcol)

dim.big.data.table = function(x){
    nodes.ok = is.big.data.table(x, check.nodes = TRUE)
    if(!all(nodes.ok)) stop(sprintf("Variable 'x' data.table does not exist on %s nodes.", paste(which(!nodes.ok), collapse = ", ")))
    dimx = bdt.eval(x, dim(x), lazy = TRUE, simplify = TRUE, rbind = FALSE)
    nr = sapply(dimx, `[`, 1L)
    nc = sapply(dimx, `[`, 2L)
    if(uniqueN(nc)!=1L) stop("big.data.table nodes various in number of columns.")
    c(sum(nr), nc[[1L]])
}

print.big.data.table = function(x, topn = getOption("datatable.print.topn"), quote = FALSE, ...){
    dims = dim(x)
    if(dims[[1L]] == 0L) {
        if(dims[[2L]]==0L){
            cat("Null data.table (0 rows and 0 cols)\n")
        } else {
            cat("Empty data.table (0 rows) of ",dims[[2L]]," col", if(dims[[2L]] > 1L) "s",": ", paste(head(names(x), 6), collapse=","), if(dims[[2L]] > 6) "...", "\n", sep="")
        }
        return(invisible())
    }
    rscl = attr(x, "rscl")
    qh = substitute(head(x, topn), list(topn = topn))
    head.dt = RS.eval(rscl[[1L]], qh, wait = TRUE, lazy = FALSE)
    qt = substitute(tail(x, topn), list(topn = topn))
    tail.dt = RS.eval(rscl[[length(rscl)]], qt, wait = TRUE, lazy = FALSE)
    toprint = rbind(head.dt, tail.dt)
    rn = c(seq_len(nrow(head.dt)), seq.int(to = dims[[1L]], length.out = nrow(tail.dt)))
    toprint = format(toprint, ...)
    rownames(toprint) = paste(format(rn, right=TRUE, scientific=FALSE), ":", sep="")
    if(is.null(names(x))) colnames(toprint) = rep("NA", ncol(toprint))
    toprint = rbind(head(toprint, nrow(head.dt)),"---"="", tail(toprint, nrow(tail.dt)))
    rownames(toprint) = format(rownames(toprint), justify="right")
    print(toprint, right=TRUE, quote=quote)
    return(invisible())
}

str.big.data.table = function(object, unclass = FALSE, ...){
    var = attr(object, "var")
    if(unclass){
        str(core.data.table(object, var))
        return(invisible())
    }
    qdim = substitute(dim(.x), list(.x = as.name(var)))
    dims.nodes = bdt.eval(object, qdim, lazy = FALSE, parallel = FALSE)
    nrows = sapply(dims.nodes, `[[`, 1L)
    ncols = unique(sapply(dims.nodes, `[[`, 2L))
    nnodes = length(dims.nodes)
    if(length(ncols)!=1L) stop("Nodes differs in data.table structure in terms of columns number. Use: `bdt.eval(bdt, capture.output(str(x)))` to investigate.")
    core.dt = core.data.table(object, var)#as.data.table(lapply(object, function(x) x)) # 0 rows template
    dtcols = capture.output(str(core.dt, give.attr = FALSE))[-1L]
    prnt = character()
    prnt["header"] = sprintf("'big.data.table': %s obs. of %s variable%s across %s node%s%s", sum(nrows), ncols, if(ncols!=1L) "s" else "", nnodes, if(nnodes!=1L) "s" else "", if(ncols > 0L) ":" else "")
    if(ncols > 0L) prnt["columns"] = paste(dtcols, collapse="\n")
    prnt["nodes_header"] = sprintf("row count by node:")
    prnt["nodes_nrow"] = paste(capture.output(print(nrows)), collapse="\n")
    if(length(partitions <- attr(object, "partitions"))) prnt["partitions"] = sprintf("'big.data.table' partitioned by '%s'.", paste(nondotnames(partitions), collapse = ", "))
    cat(prnt, sep="\n")
    invisible()
}

#' @title Test if object is big.data.table
#' @param x R object.
#' @param check.nodes logical default FALSE, when TRUE it will validate that nodes have *x* variable data.table
#' @return For `check.nodes=FALSE` (default) a scalar logical if *x* inherits from *big.data.table*. For `check.nodes=TRUE` vector of results from expression `exists("x") && is.data.table(x)` on each node.
is.big.data.table = function(x, check.nodes = FALSE){
    if(!inherits(x, "big.data.table")) return(FALSE)
    if(!check.nodes) return(TRUE)
    var = attr(x, "var", exact = TRUE)
    if(is.null(var)) var = "x"
    is.node = substitute(exists(.var) && is.data.table(.x), list(.var = var, .x = as.name(var)))
    return(bdt.eval(x, is.node, lazy = FALSE, simplify = TRUE, rbind = FALSE, parallel = FALSE))
}

#' @title big.data.table evaluate on nodes
#' @description Main engine for passing queries to nodes, control parallelization, rbinding or simplifing returned object. Allows to measure timing and verbose messages.
#' @param x big.data.table.
#' @param expr expression.
#' @param lazy logical if TRUE then *expr* is substituted.
#' @param send logical, if TRUE submit expression appended with `TRUE` to not fetch potentially big results from provided *expr*, useful for data.table *set** or `:=` functions.
#' @param simplify logical if *TRUE* (default) it will simplify list of 1 length same type objects to vector.
#' @param rbind logical if *TRUE* (default) results are data.table they will be rbinded.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param silent should be silently catched by `try` or `logR` if enabled.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @return Depending on *simplify, rbind* the results of evaluated expression.
bdt.eval = function(x, expr, lazy = TRUE, send = FALSE, simplify = TRUE, rbind = TRUE, parallel = TRUE, silent = TRUE, .log = getOption("bigdatatable.log",FALSE)){
    stopifnot(is.big.data.table(x) || is.rsc(x, silent = TRUE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    if(isTRUE(lazy)) expr = substitute(expr)
    # logging and error catching
    if(!.log){
        if(isTRUE(silent)) expr = substitute(tryCatch(.expr, error = function(e) e, warning = function(w) w), list(.expr=expr))
    }
    if(.log){
        expr = substitute(
            logR(.expr, alert = .alert, silent = .silent, .log = ..log),
            list(.expr=expr, .alert=!silent, .silent=silent, ..log=.log)
        )
    }
    if(!missing(send) && isTRUE(send)) expr = substitute({.expr; invisible(TRUE)}, list(.expr=expr))
    # execute
    if(!parallel){
        x = lapply(rscl, RS.eval, expr, lazy = FALSE)
    }
    if(parallel){
        invisible(lapply(rscl, RS.eval, expr, wait = FALSE, lazy = FALSE))
        x = lapply(rscl, RS.collect)
    }
    # format results
    if(rbind && is.data.table(x[[1L]])){
        x = rbindlist(x)
    } else if(simplify && length(x) && length(x[[1L]])==1 && is.atomic(x[[1L]])){
        if(all(sapply(x, length)==1L) && all(sapply(x, is.atomic)) && length(unique(sapply(x, typeof)))==1L) x = simplify2array(x)
    }
    return(x)
}

#' @title big.data.table assign object
#' @description Saves the object to nodes, handles partitioning.
#' @param x big.data.table.
#' @param name character variable name to which assign *value* in each node, for *x* data.table it should be equal to `x`.
#' @param value an R object to save on node, if it is data.table then it will be partitioned accroding to *x* partitions.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
bdt.assign = function(x, name, value, parallel = TRUE){
    stopifnot(is.big.data.table(x) || is.rsc(x, silent = FALSE))
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
        rscid = seq_len(nnodes)
        if(length(partitions)){
            value = lapply(rscid, function(i){
                if(i <= nrow(partitions)) value[partitions[i], nomatch = 0L, on = key(partitions)] else value[0L]
            })
        }
        if(!length(partitions)){
            partition.map = if(nrow(value)) cut(seq_len(nrow(value)), min(nnodes, nrow(value)), labels = FALSE) else integer()
            value = lapply(rscid, function(i) value[partition.map==i])
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
#' @return big.data.table object.
bdt.partition = function(x, partition.by, copy = FALSE, validate = TRUE, parallel = TRUE){
    stopifnot(is.big.data.table(x) || is.rsc(x, silent = FALSE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    nnodes = length(rscl)
    partitions = if(is.big.data.table(x)) attr(x, "partitions") else data.table(NULL)
    if(!missing(partition.by)){
        if(!identical(partition.by, nondotnames(partitions))) warning("Provided partition.by does not match to currently declared partitions, they will be updated, if ", immediate. = TRUE)
        partition.by = nondotnames(partitions)
        # update partitions
        qcall = substitute(unique(x, by = partition.by)[, c(partition.by), with=FALSE], list(partition.by=partition.by))
        partitions = unique(bdt.eval(x, expr = qcall, lazy = FALSE, parallel=parallel), by = partition.by)
        x = big.data.table(x = setattr(x, "class", c("data.table","data.frame"))[0L], rscl = rscl, partitions = partitions)
    }
    if(isTRUE(copy)){
        lapply(seq_len(nnodes), function(i){
            # copy 'x' to local session
            cat(sprintf("big.data.table: processing %s of %s nodes.\n", i, nnodes))
            # check if anything to copy
            qcall = substitute(x[!partition.key], list(partition.key=partitions[i]))
            tmp = bdt.eval(rscl[i], expr = qcall, lazy = FALSE, parallel=FALSE)
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
        r = bdt.eval(x, expr = qcall, lazy = FALSE, parallel = parallel)
        if(!all(r)) stop(sprintf("big.data.table partitioning has been finished but validation of data didn't pass for %s nodes.", which(!r)))
    }
    return(x)
}

#' @title Subset from big.data.table
#' @param x big.data.table object.
#' @param \dots arguments passed to each node `[.data.table` call: *i, j, by, keyby...*.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param outer.aggregate logical, if *TRUE* will able the same query to rbind of results from each node, should not be used with `.SD`, `.N`, etc.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @note Results from nodes are rbinded and the same call is evalated on combined results. That means the column names cannot be renamed or simplified to vector in `...` call. Use `[[.big.data.table` for deeper flexibility.
#' @return data.table object.
"[.big.data.table" = function(x, ..., new.var, new.copy = FALSE, parallel = TRUE, outer.aggregate = getOption("bigdatatable.outer.aggregate",FALSE), .log = getOption("bigdatatable.log",FALSE)){
    dtq = match.call(expand.dots = FALSE)$`...`
    var = attr(x, "var", exact = TRUE)
    dtcall = as.call(c(list(as.symbol("["), x = as.name(var)), dtq))
    # allow copy results into new variable
    if(!missing(new.var)){
        stopifnot(is.character(new.var), length(new.var)==1L)
        if(isTRUE(new.copy)) dtcall = substitute(copy(.dtcall), list(.dtcall = dtcall))
        dtcall = substitute(.var <- .expr, list(.var = as.name(new.var), .expr = dtcall))
        send = TRUE
    } else send = FALSE
    # bdt node eval
    if(!.log){
        x = bdt.eval(x, expr = dtcall, lazy = FALSE, parallel = parallel, .log = .log)
    }
    if(.log){
        # substitute to have a nice logr.expr field
        bdt.expr = substitute(
            bdt.eval(x, expr = .expr, lazy = TRUE, parallel = .parallel, .log = .log),
            list(.expr = dtcall, .send = send, .parallel = parallel, ..log = .log)
        )
        x = eval(substitute(
            logR(.expr, silent = FALSE, .log = ..log),
            list(.expr=bdt.expr, ..log=.log)
        ))
    }
    # potentially return new big.data.table
    if(!missing(new.var)){
        warninigs("!missing(new.var) return new big.data.table instead of returninig data")
        return(big.data.table(var = new.var))
    }
    # aggregate results from nodes
    if(outer.aggregate) x = eval(dtcall)
    return(x)
}

#' @title Extract from big.data.table
#' @param x big.data.table object.
#' @param j numeric scalar, if provided then all other arguments are ignored and subset behaves the same way as for data.table, but returns 0 length column.
#' @param expr expression to be evaluated on node where data.table objects are stored as `x` variable in `.GlobalEnv`.
#' @param lazy logical if TRUE then *expr* is substituted.
#' @param send logical, if TRUE submit expression appended with `TRUE` to not fetch potentially big results from provided *expr*, useful for data.table *set** or `:=` functions.
#' @param i numeric restrict expression to particular nodes
#' @param \dots ignored.
#' @param simplify logical passed to `bdt.eval`, affects the type of returned object.
#' @param rbind logical passed to `bdt.eval`, affects the type of returned object.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @return When using *j* arg the 0 length variable from underlying data is returned. Otherwise the results from expression evaluated as `lapply*. When using *rbind* or *simplify* the returned list be can simplified.
"[[.big.data.table" = function(x, j, expr, lazy = TRUE, send = FALSE, i, ..., simplify = TRUE, rbind = TRUE, parallel = TRUE, .log = getOption("bigdatatable.log",FALSE)){
    # when `j` provided it return empty column from bdt to get a class of column
    if(!missing(j) && !is.null(j) && length(j)==1L && (is.numeric(j) || is.character(j))) return(unclass(x)[[j]])
    if(isTRUE(lazy)) expr = substitute(expr)
    rscl = attr(x, "rscl")
    if(missing(i)){
        all.nodes = TRUE
        i = seq_along(rscl)
    } else {
        stopifnot(is.numeric(i))
        all.nodes = FALSE
    }
    if(!.log){
        bdt.eval(rscl[i], expr = expr, lazy = FALSE, send = send, simplify = simplify, rbind = rbind, parallel = parallel, .log = .log)
    } else {
        # substitute to have a nice logr.expr field
        rscl.i = if(!all.nodes) call("[",as.name("rscl"), i) else as.name("rscl")
        bdt.expr = substitute(
            bdt.eval(.rscl, expr = .expr, lazy = TRUE, send = .send, simplify = .simplify, rbind = .rbind, parallel = .parallel, .log = ..log),
            list(.rscl = rscl.i, .expr = expr, .send = send, .simplify = simplify, .rbind = rbind, .parallel = parallel, ..log = .log)
        )
        eval(substitute(
            logR(.expr, silent = FALSE, .log = ..log),
            list(.expr=bdt.expr, ..log=.log)
        ))
    }
}
