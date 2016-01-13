nondotnames = function(x){
    nm = names(x)
    nm[!sapply(nm, substr, 1L, 1L)=="."]
}

# big.data.table ----

big.data.table = function(x, rscl, partitions){
    stopifnot(is.rsc(rscl, silent=FALSE))
    bdt = if(!missing(x) && !is.null(x)){
        stopifnot(is.data.table(x))
        x[0L]
    } else data.table(NULL)
    if(!missing(partitions) && !is.null(partitions)){
        stopifnot(is.data.table(partitions))
    } else partitions = data.table(NULL)
    partition.by = nondotnames(partitions)
    setkeyv(partitions, if(length(partition.by)) partition.by)
    if(nrow(partitions) > length(rscl)) stop(sprintf("Number of new partitions is %s while number of defined nodes only %s. You can create new column unique for each node.", nrow(partitions), length(rscl)))
    structure(bdt, class = "big.data.table", rscl = rscl, partitions = partitions)
}

# as.big.data.table ----

#' @title Convert to big.data.table
#' @param x object to cast into big.data.table, can be data.table, function, quoted call or a list.
#' @param \dots arguments passed to methods.
#' @param rscl list of Rserve connections, can be validated by `is.rsc(rscl, silent=FALSE)`.
#' @param partition.by character vector of column names to be used for partitioning, `uniqueN` by those columns should be lower than number of nodes.
#' @param partitions data.table of unique combinations of values in *partition.by* columns.
#' @param parallel logical, see `?bdt.eval`.
#' @note Supported `x` data types are *data.table* (will be automatically spread across the nodes), *function* or quoted *call* will be evaluated on each node and assigned to `x` variable, a *list* will struct big.data.table on already working set of nodes, all having `x` data.tables.
#' @return big.data.table object.
as.big.data.table = function(x, ...){
    UseMethod("as.big.data.table")
}

# .function - having cluster working and source data available to each node ----

#' @rdname as.big.data.table
as.big.data.table.function = function(x, rscl, partition.by, partitions, parallel = TRUE, ...){
    fun.args = match.call(expand.dots = FALSE)$`...`
    qcall = as.call(c(list(x), fun.args))
    as.big.data.table(x = qcall, rscl = rscl, partition.by = partition.by, partitions = partitions, parallel = parallel)
}

# .call - having cluster working and source data available to each node ----

#' @rdname as.big.data.table
as.big.data.table.call = function(x, rscl, partition.by, partitions, parallel = TRUE, ...){
    # execute function on nodes
    assign_x = substitute(x <- qcall, list(qcall = x))
    bdt.eval(rscl, expr = assign_x, lazy = FALSE, send = TRUE, parallel = parallel)
    # redirect to list method
    as.big.data.table(x = rscl, partition.by = partition.by, partitions = partitions, parallel = parallel)
}

# .list - having cluster working and loaded with data already ----

#' @rdname as.big.data.table
as.big.data.table.list = function(x, partition.by, partitions, parallel = TRUE, ...){
    stopifnot(is.rsc(x, silent=FALSE))
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
    rvalidate = sapply(x, RS.eval, qvalidate, lazy = FALSE)
    if(!all(rvalidate)) stop(sprintf("Following nodes does not have 'x' data.table in R_GlobalEnv: %s. You should use *.function or *.call methods. If sending data from local R session then use *.data.table method.", paste(which(!rvalidate), collapse=", ")))
    # check colnames match
    colnames = lapply(x, RS.eval, names(x))
    if(!identical(colnames[[1L]], unique(unname(unlist(colnames))))) stop(sprintf("Data stores on the nodes varies in structure. Column names does not match."))
    # provided partition.by but not partitions - it will compute partitions
    if(length(partition.by) && !length(partitions)){
        qpartition = substitute(unique(x, by = partition.by)[, c(partition.by), with=FALSE],
                                list(partition.by = partition.by))
        partitions = unique(bdt.eval(x, expr = qpartition, lazy = FALSE, parallel = parallel), by = partition.by)
    }
    # return big.data.table class
    bdt = RS.eval(x[[1L]], x[0L])
    big.data.table(x = bdt, rscl = x, partitions = partitions)
}

# .data.table - having data loaded locally in R ----

#' @rdname as.big.data.table
as.big.data.table.data.table = function(x, rscl, partition.by, partitions, parallel = TRUE, ...){
    stopifnot(is.rsc(rscl, silent=FALSE))
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
    bdt = x[0L]
    bdt = big.data.table(x = bdt, rscl = rscl, partitions = partitions)
    # send data to nodes
    bdt.assign(bdt, name = "x", value = x, parallel = parallel)
    # validate
    qvalidate = quote(exists("x") && is.data.table(x))
    if(length(partition.by)) qvalidate = substitute(qvalidate && all(partition.by %in% names(x)), list(qvalidate = qvalidate, partition.by = partition.by))
    rvalidate = sapply(rscl, RS.eval, qvalidate, lazy = FALSE)
    if(!all(rvalidate)) browser()#stop(sprintf("Following nodes does not have 'x' data.table in R_GlobalEnv: %s. You should use *.function or *.call methods. If sending data from local R session then use *.data.table method.", paste(which(!rvalidate), collapse=", ")))
    return(bdt)
}

# as.data.table.big.data.table - extracting data from nodes to local R session ----

as.data.table.big.data.table = function(x, ...){
    bdt.eval(x, x, try=TRUE)
}
