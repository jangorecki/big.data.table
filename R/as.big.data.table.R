
# big.data.table ----

big.data.table = function(var = "x", rscl, partitions){
    stopifnot(is.character(var), is.rscl(rscl, silent=FALSE))
    if(!missing(partitions) && !is.null(partitions)){
        stopifnot(is.data.table(partitions))
    } else partitions = data.table(NULL)
    partition.by = nondotnames(partitions)
    setkeyv(partitions, if(length(partition.by)) partition.by)
    if(nrow(partitions) > length(rscl)) stop(sprintf("Number of new partitions is %s while number of defined nodes only %s. You can create new column unique for each node.", nrow(partitions), length(rscl)))
    bdt = data.table(NULL)
    structure(bdt, class = "big.data.table", rscl = rscl, var = var, partitions = partitions)
}

# as.big.data.table ----

#' @title Convert to big.data.table
#' @param x object to cast into big.data.table, can be data.table, function, quoted call or a list.
#' @param \dots arguments passed to methods.
#' @param rscl list of Rserve connections, can be validated by `is.rscl(rscl, silent=FALSE)`.
#' @param partition.by character vector of column names to be used for partitioning, `uniqueN` by those columns should be lower than number of nodes.
#' @param partitions data.table of unique combinations of values in *partition.by* columns.
#' @param parallel logical, see `?bdt.eval`.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @note Supported `x` data types are *data.table* (will be automatically spread across the nodes), *function* or quoted *call* will be evaluated on each node and assigned to `x` variable, a *list* will struct big.data.table on already working set of nodes, all having `x` data.tables.
#' @return big.data.table object.
as.big.data.table = function(x, ...){
    UseMethod("as.big.data.table")
}

# .function - having cluster working and source data available to each node

#' @rdname as.big.data.table
as.big.data.table.function = function(x, rscl, partition.by, partitions, parallel = TRUE, ..., .log = getOption("bigdatatable.log",FALSE)){
    # assign function to nodes
    fun.var = substitute(x)
    if(!is.name(fun.var)) stop("Function provided as 'x' arg must not be an anonymous function, assign it to variable locally and pass the variable to 'x'.")
    rscl.assign(rscl, as.character(fun.var), value = x)
    # prepare call
    fun.args = match.call(expand.dots = FALSE)$`...`
    qcall = as.call(c(list(fun.var), fun.args))
    # redirect to .call method
    as.big.data.table.call(x = qcall, rscl = rscl, partition.by = partition.by, partitions = partitions, parallel = parallel, .log = .log)
}

# .call - having cluster working and source data available to each node

#' @rdname as.big.data.table
as.big.data.table.call = function(x, rscl, partition.by, partitions, parallel = TRUE, ..., .log = getOption("bigdatatable.log",FALSE)){
    # execute function on nodes
    assign_x = substitute(x <- qcall, list(qcall = x))
    # populate bdt from call
    r = bdt.eval.log(rscl, expr = assign_x, lazy = FALSE, send = TRUE, parallel = parallel, .log = .log)
    if(!all(r)) stop(sprintf("Some nodes failed to evaluate expression: %s.", paste(which(!r), collapse=", ")))
    # redirect to .list method
    as.big.data.table.list(x = rscl, partition.by = partition.by, partitions = partitions, parallel = parallel, .log = .log)
}

# .list - having cluster working and loaded with data already

#' @rdname as.big.data.table
as.big.data.table.list = function(x, partition.by, partitions, parallel = TRUE, ..., .log = getOption("bigdatatable.log",FALSE)){
    stopifnot(is.rscl(x, silent=FALSE))
    # general check for partition.by and partitions in creation of big.data.table
    if(missing(partitions) || !length(partitions)) partitions = data.table(NULL)
    if(missing(partition.by) || !length(partition.by)) partition.by = character(0)
    stopifnot(is.data.table(partitions), is.character(partition.by))
    if(any(sapply(partition.by, substr, 1L, 1L)==".")) stop(sprintf("partition.by column names should not start with dots '.', those are kept so user can store own partitions attributes."))
    # provided partitions
    if(length(partitions)){
        if(!length(partition.by)) partition.by = nondotnames(partitions)
        if(length(partition.by)) stopifnot(identical(nondotnames(partitions), partition.by))
        if(!nrow(unique(partitions, by = partition.by))==nrow(partitions)) stop(sprintf("Partitions provided are not unique. If using custom columns then be sure to prefix it with dot a '.': '.id', '.partition_feature'."))
    }
    # validate existing data
    qvalidate = quote(exists("x") && is.data.table(x))
    if(length(partition.by)) qvalidate = substitute(qvalidate && all(partition.by %in% names(x)), list(qvalidate = qvalidate, partition.by = partition.by))
    rvalidate = rscl.eval(x, qvalidate, lazy = FALSE)
    if(!all(rvalidate)) stop(sprintf("Following nodes does not have 'x' data.table in R_GlobalEnv: %s. You should use *.function or *.call methods. If sending data from local R session then use *.data.table method.", paste(which(!rvalidate), collapse=", ")))
    # check colnames match
    colnames = rscl.eval(x, names(x), simplify = FALSE)
    if(!identical(colnames[[1L]], unique(unname(unlist(colnames))))) stop(sprintf("Data stored on the nodes varies in structure. Column names does not match."))
    # provided partition.by but not partitions - it will compute partitions
    if(length(partition.by) && !length(partitions)){
        qpartition = substitute(unique(x, by = partition.by)[, c(partition.by), with=FALSE], list(partition.by = partition.by))
        partitions = unique(bdt.eval.log(x, expr = qpartition, lazy = FALSE, parallel = parallel, .log = .log), by = partition.by)
    }
    # return big.data.table class
    big.data.table(var = "x", rscl = x, partitions = partitions)
}

# .data.table - having data loaded locally in R

#' @rdname as.big.data.table
as.big.data.table.data.table = function(x, rscl, partition.by, partitions, parallel = TRUE, ..., .log = getOption("bigdatatable.log",FALSE)){
    stopifnot(is.rscl(rscl, silent=FALSE))
    # general check for partition.by and partitions in creation of big.data.table
    if(missing(partitions) || !length(partitions)) partitions = data.table(NULL)
    if(missing(partition.by) || !length(partition.by)) partition.by = character(0)
    stopifnot(is.data.table(partitions), is.character(partition.by))
    if(any(sapply(partition.by, substr, 1L, 1L)==".")) stop(sprintf("partition.by column names should not start with dots '.', those are kept so user can store own partitions attributes."))
    # provided partitions
    if(length(partitions)){
        if(!length(partition.by)) partition.by = nondotnames(partitions)
        if(length(partition.by)) stopifnot(identical(nondotnames(partitions), partition.by))
        if(!nrow(unique(partitions, by = partition.by))==nrow(partitions)) stop(sprintf("Partitions provided are not unique. If using custom columns then be sure to prefix it with dot a '.': '.id', '.partition_feature'."))
    }
    if(length(partition.by) && !length(partitions)){
        partitions = unique(x, by = partition.by)[, c(partition.by), with=FALSE]
    }
    # struct object to return
    bdt = big.data.table(var = "x", rscl = rscl, partitions = partitions)
    # send data to nodes
    bdt.assign(bdt, name = "x", value = x, parallel = parallel, .log = .log)
    # validate
    qvalidate = quote(exists("x") && is.data.table(x))
    if(length(partition.by)) qvalidate = substitute(qvalidate && all(partition.by %in% names(x)), list(qvalidate = qvalidate, partition.by = partition.by))
    rvalidate = rscl.eval(rscl, qvalidate, lazy = FALSE)
    if(!all(rvalidate)) stop(sprintf("Assigning data.table to nodes failed. Following nodes does not have 'x' data.table in R_GlobalEnv: %s.", paste(which(!rvalidate), collapse=", ")))
    return(bdt)
}

# as.*.big.data.table ----

# as.data.table.big.data.table - extracting data from nodes to local R session

#' @title Extracting data from nodes to local R session
#' @param x big.data.table
#' @param \dots arguments passed to `bdt.eval`.
#' @param .log logical if *TRUE* then logging will be done using logR to postgres db.
#' @return data.table
as.data.table.big.data.table = function(x, ..., .log = getOption("bigdatatable.log",FALSE)){
    bdt.eval.log(x, x, silent=TRUE, ..., .log = .log)
}
