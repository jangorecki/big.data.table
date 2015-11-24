library(RSclient)
library(data.table)
library(big.data.table)

# connect
port = 9411:9414
rscl = rsc(port)
# sapply(rscl, RS.close)
# sapply(rscl, RS.collect)
# sapply(rscl, function(x) try(RS.collect(x), silent=TRUE))

# .function - having cluster working and source data in csv on disk of each node ----

f = function(n = 1e5, ...) data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n))
bdt = as.big.data.table(f, rscl, n = 2e4)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .call - having cluster working and source data in csv on disk of each node ----

qcall = substitute(data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n)),
                   list(n = 2e4))
bdt = as.big.data.table(qcall, rscl = rscl)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .list - having cluster working and loaded with data already ----

bdt = as.big.data.table(rscl)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .data.table - having data loaded locally in R ----

set.seed(1)
n = 1e5
# high/normal/low cardinality columns
x = data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n))
bdt = as.big.data.table(x, rscl)
stopifnot(
    dim(bdt)==c(1e5L, 5L),
    nrow(bdt)==1e5L,
    ncol(bdt)==5L,
    names(bdt)==c("year", "high", "normal", "low", "value")
)

# as.data.table.big.data.table - extracting data from nodes to local R session ----

dt = as.data.table(bdt)
stopifnot(all.equal(
    x[,.(value=sum(value), count=.N),, c("year","high","normal","low")],
    dt[,.(value=sum(value), count=.N),, c("year","high","normal","low")]
))

# as.big.data.table - edge cases ----

# data.table(NULL)
dt = data.table(NULL)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    dim(dt)==c(0L,0L),
    capture.output(print(bdt))=="Null data.table (0 rows and 0 cols)",
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval(bdt, nrow(x))), c(0L,0L,0L,0L))
)
# data.table nrow < nodes
dt = data.table(a=1:3)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl) > nrow(dt),
    identical(capture.output(print(bdt)), c("    a", " 1: 1", "---  ")),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval(bdt, nrow(x))), c(1L,1L,1L,0L))
)
# data.table nrow == nodes
dt = data.table(a=1:4)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl) == nrow(dt),
    identical(capture.output(print(bdt)), c("    a", " 1: 1", "---  ", " 4: 4")),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval(bdt, nrow(x))), c(1L,1L,1L,1L))
)
# data.table nrow == nodes+1L
dt = data.table(a=1:5)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl)+1L == nrow(dt),
    identical(capture.output(print(bdt)), c("    a", " 1: 1", " 2: 2", "---  ", " 5: 5")),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval(bdt, nrow(x))), c(2L,1L,1L,1L))
)

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))

# disconnect
sapply(rscl, RS.close)
