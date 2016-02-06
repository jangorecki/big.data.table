library(RSclient)
library(data.table)
library(big.data.table)

# connect
host = "127.0.0.1"
port = 33311:33314
rscl = rscl.connect(port, host, pkgs = "data.table")

# .function - having cluster working and source data in csv on disk of each node ----

f = function(n = 1e5, ...) data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n))
bdt = as.big.data.table(f, rscl, n = 2e4)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval.log(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .call - having cluster working and source data in csv on disk of each node ----

qcall = substitute(data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n)),
                   list(n = 2e4))
bdt = as.big.data.table(qcall, rscl = rscl)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval.log(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .list - having cluster working and loaded with data already ----

bdt = as.big.data.table(rscl)
stopifnot(
    all.equal(dim(bdt), c(8e4, 5L)),
    all.equal(bdt.eval.log(bdt, dim(x)), lapply(1:4, function(i) c(2e4, 5L)), check.attributes = FALSE)
)

# .data.table - having data loaded locally in R ----

set.seed(1)
n = 1e5
# high/normal/low cardinality columns
x = data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(10, n, TRUE), value = rnorm(n))
bdt = as.big.data.table(x, rscl)
stopifnot(
    all.equal(dim(bdt), c(1e5L, 5L)),
    all.equal(nrow(bdt),1e5L),
    all.equal(ncol(bdt),5L),
    all.equal(unlist(unique(bdt[[expr = names(x), simplify = TRUE]])), c("year", "high", "normal", "low", "value")),
    all.equal(names(bdt), character()),
    all.equal(dim(bdt[, .N, year][, sum(N), year]), c(4L,2L))
)

# as.data.table.big.data.table - extracting data from nodes to local R session ----

dt = as.data.table(bdt)
stopifnot(all.equal(
    x[,.(value=sum(value), count=.N),, c("year","high","normal","low")
      ][,.(value=sum(value), count=sum(count)),, c("year","high","normal","low")],
    dt[,.(value=sum(value), count=.N),, c("year","high","normal","low")]
))

# as.big.data.table - edge cases ----

# data.table(NULL)
dt = data.table(NULL)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    all.equal(dim(bdt), c(0L,0L)),
    all.equal(capture.output(print(bdt)), "Null data.table (0 rows and 0 cols)"),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval.log(bdt, nrow(x))), c(0L,0L,0L,0L)),
    all.equal(capture.output(str(bdt))[1:2], c("'big.data.table': 0 obs. of 0 variables across 4 nodes", "rows count by node:"))
)
# data.table nrow < nodes
dt = data.table(a=1:3)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl) > nrow(dt),
    identical(suppressWarnings(capture.output(print(bdt))), c(" a", " 1", "---")), # edge case
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval.log(bdt, nrow(x))), c(1L,1L,1L,0L)),
    all.equal(capture.output(str(bdt))[1:3], c("'big.data.table': 3 obs. of 1 variable across 4 nodes:", " $ a: int ", "rows count by node:"))
)
# data.table nrow == nodes
dt = data.table(a=1:4)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl) == nrow(dt),
    identical(capture.output(print(bdt)), c(" a", " 1", "---", " 4")),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval.log(bdt, nrow(x))), c(1L,1L,1L,1L)),
    all.equal(capture.output(str(bdt))[1:3], c("'big.data.table': 4 obs. of 1 variable across 4 nodes:", " $ a: int ", "rows count by node:"))
)
# data.table nrow == nodes+1L
dt = data.table(a=1:5)
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    length(rscl)+1L == nrow(dt),
    identical(capture.output(print(bdt)), c(" a", " 1", " 2", "---", " 5")),
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval.log(bdt, nrow(x))), c(2L,1L,1L,1L))
)
# data.table partition.by cardinality lower than number of nodes
dt = data.table(a=1:3, b=rnorm(6))
bdt = as.big.data.table(x = dt, rscl = rscl, partition.by = "a")
stopifnot(
    length(rscl)-1L == uniqueN(dt$a),
    (co <- capture.output(str(bdt)))[length(co)]=="'big.data.table' partitioned by 'a'.",
    all(is.big.data.table(bdt, check.nodes = TRUE)),
    all.equal(unname(bdt.eval.log(bdt, nrow(x))), c(2L,2L,2L,0L))
)

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))

# disconnect
rscl.close(rscl)
