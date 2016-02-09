#' @title big.data.table-package
#' @description Leverage data.table onto set of Rserve nodes.
#' @details
#' Use `as.big.data.table` methods to extract/load/assign/populate data to a list of R node connections.
#' Methods for `function` and `call` will be evaluated remotely, method for `data.table` will be evaluated locally and assigned to nodes.
#' @aliases bdt big.dt big.data.table
#' @docType package
#' @author Jan Gorecki
#' @name big.data.table-package
NULL

# *.big.data.table ----

#' @title Test if object is big.data.table
#' @param x R object.
#' @param check.nodes logical default FALSE, when TRUE it will validate that nodes have data.table as defined variable.
#' @return For `check.nodes=FALSE` (default) a scalar logical if *x* inherits from *big.data.table*. For `check.nodes=TRUE` vector of results from expression `exists("x") && is.data.table(x)` on each node.
is.big.data.table = function(x, check.nodes = FALSE){
    if(!inherits(x, "big.data.table")) return(FALSE)
    if(!check.nodes) return(TRUE)
    var = attr(x, "var")
    rscl = attr(x, "rscl")
    is.node = substitute(exists(.var) && is.data.table(.x), list(.var = var, .x = as.name(var)))
    rscl.eval(rscl, is.node, lazy = FALSE)
}

dim.big.data.table = function(x){
    nodes.ok = is.big.data.table(x, check.nodes = TRUE)
    var = attr(x, "var")
    rscl = attr(x, "rscl")
    if(!all(nodes.ok)) stop(sprintf("Variable '%s' data.table does not exist on %s nodes.", var, paste(which(!nodes.ok), collapse = ", ")))
    dimq = substitute(dim(.x), list(.x = as.name(var)))
    dimx = rscl.eval(rscl, dimq, lazy = FALSE, simplify = FALSE)
    nr = sapply(dimx, `[`, 1L)
    nc = sapply(dimx, `[`, 2L)
    if(uniqueN(nc)!=1L) stop("big.data.table nodes varies in number of columns.")
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
    var = attr(x, "var")
    rscl = attr(x, "rscl")
    if(length(rscl)==1L) warning("Running big.data.table with single node, print will be incorrect.")
    qh = substitute(head(.x, topn), list(.x = as.name(var), topn = topn))
    head.dt = RS.eval(rscl[[1L]], qh, wait = TRUE, lazy = FALSE)
    if(!nrow(head.dt)) warning("First node doesn't have any rows, print will be incorrect.")
    qt = substitute(tail(.x, topn), list(.x = as.name(var), topn = topn))
    tail.dt = RS.eval(rscl[[length(rscl)]], qt, wait = TRUE, lazy = FALSE)
    if(!nrow(tail.dt)) warning("Last node doesn't have any rows, print will be incorrect.")
    prnt = list()
    if(nrow(head.dt)) prnt[["head"]] = capture.output(print(head.dt, row.names=FALSE))
    if(nrow(head.dt) || nrow(tail.dt)) prnt[["line"]] = "---"
    if(nrow(tail.dt)) prnt[["tail"]] = capture.output(print(tail.dt, row.names=FALSE))[-1L] # dropping col names
    cat(unlist(prnt), sep = "\n")
    return(invisible())
}

str.big.data.table = function(object, unclass = FALSE, ...){
    var = attr(object, "var")
    rscl = attr(object, "rscl")
    if(unclass){
        str(core.data.table(object))
        return(invisible())
    }
    qdim = substitute(dim(.x), list(.x = as.name(var)))
    dims.nodes = rscl.eval(rscl, qdim, lazy = FALSE, simplify = FALSE)
    nrows = sapply(dims.nodes, `[[`, 1L)
    ncols = unique(sapply(dims.nodes, `[[`, 2L))
    nnodes = length(dims.nodes)
    if(length(ncols)!=1L) stop("Nodes differs in data.table structure in terms of columns number. Use: `bdt.eval(bdt, capture.output(str(x)))` to investigate.")
    core.dt = core.data.table(object)
    dtcols = capture.output(str(core.dt, give.attr = FALSE))[-1L]
    prnt = character()
    prnt["header"] = sprintf("'big.data.table': %s obs. of %s variable%s across %s node%s%s", sum(nrows), ncols, if(ncols!=1L) "s" else "", nnodes, if(nnodes!=1L) "s" else "", if(ncols > 0L) ":" else "")
    if(ncols > 0L) prnt["columns"] = paste(dtcols, collapse="\n")
    prnt["nodes_header"] = sprintf("rows count by node:")
    prnt["nodes_nrow"] = paste(capture.output(print(nrows)), collapse="\n")
    if(length(partitions <- attr(object, "partitions"))) prnt["partitions"] = sprintf("'big.data.table' partitioned by '%s'.", paste(nondotnames(partitions), collapse = ", "))
    cat(prnt, sep="\n")
    invisible()
}

# bdt.* ----

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
#' @param outer.aggregate logical or a function, if *TRUE* will able the same query to rbind of results from each node, should not be used with `.SD`, `.N`, etc. Also conflicts with filtering in `i`. Can be also a function taking first argument `x` rbinded data.table.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @details The `bdt.eval.log` function is wrapper on `bdt.eval` with `logR()` call, so use only when logR connected.
#' @return Depending on *simplify, rbind* the results of evaluated expression.
bdt.eval = function(x, expr, lazy = TRUE, send = FALSE, simplify = TRUE, rbind = TRUE, parallel = TRUE, silent = TRUE, outer.aggregate = FALSE, .log = getOption("bigdatatable.log",FALSE)){
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = TRUE), is.logical(lazy), is.logical(send), is.logical(simplify), is.logical(rbind), is.logical(parallel), is.logical(outer.aggregate) | is.function(outer.aggregate), is.logical(silent), is.logical(.log))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    var = if(is.big.data.table(x)) attr(x, "var")
    in_rows = if(is.big.data.table(x)) call("nrow", as.name(var)) else NA_integer_
    if(lazy) expr = substitute(expr)
    org.expr = expr
    # - [x] logging and error catching for R nodes
    # - [x] when `send` used it will return the boolean status of evaluated expression
    if(!.log){
        if(silent) expr = substitute(try(.expr, silent = TRUE), list(.expr=expr))
        if(send) expr = substitute(!inherits(.expr,"try-error"), list(.expr=expr))
    } else {
        expr = substitute(
            logR(.expr, parent = .parent, alert = .alert, in_rows = .in_rows, silent = .silent, boolean = .boolean, .log = ..log),
            list(.expr = expr, .parent = getOption("logR.id"), .in_rows = in_rows, .alert = !silent, .silent = silent, .boolean = send, ..log = .log)
        )
    }
    # - [x] execute sequentially or parallely
    x = rscl.eval(rscl, expr, lazy = FALSE, parallel = parallel, simplify = FALSE)
    # - [x] format results: rbind and simplify
    if(rbind && is.data.table(x[[1L]])){
        x = rbindlist(x)
        # - [x] aggregate results from nodes with `outer.aggregate` arg, custom function or re-call expr
        if(is.function(outer.aggregate)){
            if(!length(formals(outer.aggregate))) stop("`outer.aggregate` function needs to have at least one argument to accept data.table.")
            x = eval(as.call(list(outer.aggregate, x)))
        } else if(isTRUE(outer.aggregate)){
            # that may failed when filtering and aggregate, also .N, .SD won't work
            x = tryCatch(eval(org.expr), error = function(e) e)
            if(inherits(x, "error")) stop(sprintf("Re-calling expression in `outer.aggregate=TRUE` failed, use `outer.aggregate` as function, or operate on re-call'able expressions. Actual error: %s", as.character(x$message)))
        }
    } else if(simplify && length(x) && length(x[[1L]])==1 && is.atomic(x[[1L]])){
        if(all(sapply(x, length)==1L) && all(sapply(x, is.atomic)) && length(unique(sapply(x, typeof)))==1L) x = simplify2array(x)
    }
    return(x)
}

#' @rdname bdt.eval
bdt.eval.log = function(x, expr, lazy = TRUE, send = FALSE, simplify = TRUE, rbind = TRUE, parallel = TRUE, silent = TRUE, outer.aggregate = FALSE, .log = getOption("bigdatatable.log",FALSE)){
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = TRUE), is.logical(lazy), is.logical(send), is.logical(simplify), is.logical(rbind), is.logical(parallel), is.logical(outer.aggregate) | is.function(outer.aggregate), is.logical(silent), is.logical(.log))
    if(lazy) expr = substitute(expr)
    # - [x] logging on client side
    if(!.log){
        if(silent) try(bdt.eval(x = x, expr, lazy = FALSE, send = send, simplify = simplify, rbind = rbind, parallel = parallel, outer.aggregate = outer.aggregate, silent = TRUE, .log = .log), silent = TRUE) else {
            bdt.eval(x = x, expr, lazy = FALSE, send = send, parallel = parallel, simplify = simplify, outer.aggregate = outer.aggregate, silent = TRUE, .log = .log)
        }
    } else {
        qexpr = substitute(
            bdt.eval(x, .expr, lazy = TRUE, send = .send, simplify = .simplify, rbind = .rbind, parallel = .parallel, outer.aggregate = .outer.aggregate, silent = TRUE, .log = ..log),
            list(.expr = expr, .send = send, .simplify = simplify, .rbind = rbind, .parallel = parallel, .outer.aggregate = outer.aggregate, ..log = .log)
        )
        in_rows = if(is.big.data.table(x)) nrow(x) else NA_integer_
        # it was force silent to nodes, so client side should already catch child errors
        if(requireNamespace("logR", quietly = TRUE)){
            logR::logR(expr = qexpr, lazy = FALSE, parent = getOption("logR.id"), in_rows = in_rows, silent = silent, .log = .log)
        } else stop("To use logging feature you need to have logR package installed, and be connected to postgres database.")
    }
}

#' @title big.data.table assign object
#' @description Saves the object to nodes, handles chunking/partitioning.
#' @param x big.data.table.
#' @param name character variable name to which assign *value* in each node, for *x* data.table it should be equal to `x`.
#' @param value an R object to save on node, if it is data.table then it will be partitioned into chunks, if not partition defined it will make equal rows chunks.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param .log currently ignored.
bdt.assign = function(x, name, value, parallel = TRUE, .log = getOption("bigdatatable.log",FALSE)){
    # TO DO: add logging + tests
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = FALSE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    nnodes = length(rscl)
    partitions = if(is.big.data.table(x)) attr(x, "partitions") else data.table(NULL)
    # return begins here
    if(!is.data.table(value)) rscl.assign(rscl, name = name, value = value, parallel = parallel) else {
        rscid = seq_len(nnodes)
        # partition data by column or into equal rows chunks
        if(length(partitions)){
            # chunk data.table into partitions
            value = lapply(rscid, function(i){
                if(i <= nrow(partitions)) value[partitions[i], nomatch = 0L, on = key(partitions)] else value[0L]
            })
        } else {
            # equal chunks
            partition.map = if(nrow(value)) cut(seq_len(nrow(value)), min(nnodes, nrow(value)), labels = FALSE) else integer()
            value = lapply(rscid, function(i) value[partition.map==i])
        }
        # execute sequantially or parallely
        if(!parallel) lapply(rscid, function(i) RS.assign(rsc = rscl[[i]], name = name, value = value[[i]], wait = TRUE)) else {
            invisible(lapply(rscid, function(i) RS.assign(rsc = rscl[[i]], name = name, value = value[[i]], wait = FALSE)))
            rscl.collect(rscl, simplify = FALSE)
        }
    }
}

#' @title Controls partitioning
#' @description By default function will validate if data partitioning is enforced on the nodes.
#' @param x big.data.table.
#' @param partition.by character.
#' @param parallel, see `bdt.eval`.
#' @param .log passed to `bdt.eval`.
#' @return big.data.table object.
bdt.partition = function(x, partition.by, parallel = TRUE, .log = getOption("bigdatatable.log",FALSE)){
    # TO DO: add logging + update `.log` arg doc + tests
    stopifnot(is.big.data.table(x) || is.rscl(x, silent = FALSE))
    rscl = if(is.big.data.table(x)) attr(x, "rscl") else x
    nnodes = length(rscl)
    partitions = if(is.big.data.table(x)) attr(x, "partitions") else data.table(NULL)
    if(!missing(partition.by)){
        if(!identical(partition.by, nondotnames(partitions))) warning("Provided partition.by does not match to currently declared partitions, they will be updated, if ", immediate. = TRUE)
        partition.by = nondotnames(partitions)
        # update partitions
        qcall = substitute(unique(x, by = partition.by)[, c(partition.by), with=FALSE], list(partition.by=partition.by))
        partitions = unique(bdt.eval.log(x, expr = qcall, lazy = FALSE, parallel=parallel, .log = .log), by = partition.by)
        big.data.table(var = "x", rscl = rscl, partitions = partitions)
    } else big.data.table(var = "x", rscl = rscl)
    #' #' @description It can also rearrange data between nodes to update partitioning.
    #' #' @param copy logical, default FALSE, or a data.table.
    #' #' @param validate if TRUE (default) it will validate that data are correctly partitioned.
    #' if(isTRUE(copy)){
    #'     lapply(seq_len(nnodes), function(i){
    #'         # copy 'x' to local session
    #'         cat(sprintf("big.data.table: processing %s of %s nodes.\n", i, nnodes))
    #'         # check if anything to copy
    #'         qcall = substitute(x[!partition.key], list(partition.key=partitions[i]))
    #'         tmp = bdt.eval.log(rscl[i], expr = qcall, lazy = FALSE, parallel=FALSE)
    #'         if(nrow(tmp)){
    #'             # send it to potentially multiple nodes
    #'             # TO DO: fix, it currently overrides 'x', not rbind to it
    #'             bdt.assign(x, name = "x", tmp, parallel = parallel, .log = .log)
    #'         }
    #'         TRUE
    #'     })
    #' }
    #' if(is.data.table(copy)){
    #'     bdt.assign(x, name = "x", copy, parallel = parallel)
    #' }
    #' if(validate){
    #'     qcall = quote(nrow(unique(x, by = partition.by))==1L)
    #'     r = bdt.eval.log(x, expr = qcall, lazy = FALSE, parallel = parallel, .log = .log)
    #'     if(!all(r)) stop(sprintf("big.data.table partitioning has been finished but validation of data didn't pass for %s nodes.", which(!r)))
    #' }
    # return(x)
}

# sub.bdt ----

#' @title Subset from big.data.table
#' @param x big.data.table object.
#' @param \dots arguments passed to each node `[.data.table` call: *i, j, by, keyby...*.
#' @param new.var character scalar, name of new variable where query results should be cached.
#' @param new.copy logical if *TRUE* it will make deep copy while saving to *new.var*.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param outer.aggregate logical or a function, if *TRUE* will able the same query to rbind of results from each node, should not be used with `.SD`, `.N`, etc. Also conflicts with filtering in `i`. Can be also a function taking first argument `x` rbinded data.table.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @note Results from nodes are rbinded and the same call is evalated on combined results. That means the column names cannot be renamed or simplified to vector in `...` call. Use `[[.big.data.table` for deeper flexibility.
#' @return data.table object.
"[.big.data.table" = function(x, ..., new.var, new.copy = FALSE, parallel = TRUE, outer.aggregate = getOption("bigdatatable.outer.aggregate",FALSE), .log = getOption("bigdatatable.log",FALSE)){
    dtq = match.call(expand.dots = FALSE)$`...`
    rscl = attr(x, "rscl", exact = TRUE)
    var = attr(x, "var", exact = TRUE)
    # - [x] build `[.data.table` call for R nodes
    dtcall = as.call(c(list(as.symbol("["), x = as.name(var)), dtq))
    # - [x] allow copy results into new variable
    if(!missing(new.var)){
        stopifnot(is.character(new.var), length(new.var)==1L)
        if(isTRUE(new.copy)) dtcall = substitute(copy(.dtcall), list(.dtcall = dtcall))
        dtcall = substitute(.var <- .expr, list(.var = as.name(new.var), .expr = dtcall))
        send = TRUE
    } else send = FALSE
    # - [x] redirect hard work to `bdt.eval`
    x = bdt.eval.log(x, expr = dtcall, send = send, lazy = FALSE, parallel = parallel, outer.aggregate = outer.aggregate, .log = .log)
    # - [x] when saving to new.var then new big.data.table will be returned
    if(!missing(new.var)){
        return(big.data.table(var = new.var, rscl = rscl))
    }
    return(x)
}

#' @title Extract from big.data.table
#' @param x big.data.table object.
#' @param j numeric scalar, if provided then all other arguments are ignored and subset behaves the same way as for data.table, but returns 0 length column.
#' @param expr expression to be evaluated on node where data.table objects are stored as `x` variable in `.GlobalEnv`.
#' @param lazy logical if TRUE then *expr* is substituted.
#' @param send logical, if TRUE submit expression appended with `TRUE` to not fetch potentially big results from provided *expr*, useful for data.table __set*__ or `:=` functions.
#' @param i numeric restrict expression to particular nodes
#' @param \dots ignored.
#' @param simplify logical passed to `bdt.eval`, affects the type of returned object.
#' @param rbind logical passed to `bdt.eval`, affects the type of returned object.
#' @param parallel logical if parallel *TRUE* (default) it will send expression to nodes using `wait=FALSE` and collect results afterward executing each node in parallel.
#' @param outer.aggregate logical or a function, if *TRUE* will able the same query to rbind of results from each node, should not be used with `.SD`, `.N`, etc. Also conflicts with filtering in `i`. Can be also a function taking first argument `x` rbinded data.table.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @return When using *j* arg the 0 length variable from underlying data is returned. Otherwise the results from expression evaluated as *lapply*. When using *rbind* or *simplify* the returned list be can simplified.
"[[.big.data.table" = function(x, j, expr, lazy = TRUE, send = FALSE, i, ..., simplify = TRUE, rbind = TRUE, parallel = TRUE, outer.aggregate = getOption("bigdatatable.outer.aggregate",FALSE), .log = getOption("bigdatatable.log",FALSE)){
    # when `j` provided it return empty column from bdt to get a class of column
    if(!missing(j) && !is.null(j) && length(j)==1L && (is.numeric(j) || is.character(j))) return(core.data.table(x)[[j]])
    if(isTRUE(lazy)) expr = substitute(expr)
    rscl = attr(x, "rscl")
    # - [x] allow to evaluate `expr` on subset of nodes
    if(missing(i)){
        all.nodes = TRUE
        i = seq_along(rscl)
    } else {
        stopifnot(is.numeric(i))
        all.nodes = FALSE
    }
    bdt.eval.log(rscl[i], expr = expr, lazy = FALSE, send = send, simplify = simplify, rbind = rbind, parallel = parallel, outer.aggregate = outer.aggregate, .log = .log)
}

# utils ----

nondotnames = function(x){
    nm = names(x)
    nm[!sapply(nm, substr, 1L, 1L)=="."]
}

#' @title Row bind lapply results
#' @description Wrapper on `rbindlist(lapply(...))`.
#' @param X vector (atomic or list) passed to `lapply`.
#' @param FUN function passed to `lapply`.
#' @param \dots optional arguments to `FUN`.
#' @param use.names logical passed to `rbindlist`.
#' @param fill logical passed to `rbindlist`.
#' @param idcol logical or  character passed to `rbindlist`.
rbindlapply = function(X, FUN, ..., use.names = fill, fill = FALSE, idcol = NULL){
    rbindlist(lapply(X = X, FUN = FUN, ... = ...), use.names=use.names, fill=fill, idcol=idcol)
}

#' @title Core of data.table from nodes
#' @param x big.data.table
#' @return 0 rows data.table class object, rbind of each 0L subsets.
core.data.table = function(x){
    stopifnot(is.big.data.table(x))
    var = attr(x, "var")
    rscl = attr(x, "rscl")
    qcall = call("[", as.name(var), 0L)
    r = rscl.eval(rscl, qcall, lazy = FALSE, simplify = FALSE)
    rbindlist(r)
}
