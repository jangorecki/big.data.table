
## Connect to R nodes ----

library(RSclient)
library(data.table)
library(big.data.table)

Sys.setenv(RSERVE_HOST="127.0.0.1")
port = 33311:33314
# wrapper to sapply on RS.connect with recycling, auto require pkgs
rscl = rscl.connect(port, pkgs = "data.table")
stopifnot(is.rscl(rscl))

# R version on computing nodes using `RSclient::RS.eval` and `sapply`
r = sapply(rscl, RS.eval, R.version.string)
stopifnot(is.character(r), length(r)==4L)

## Using rscl.* wrappers ----

r1 = rscl.eval(rscl, ls())
r2 = rscl.ls(rscl)
stopifnot(all.equal(r1, r2), sapply(r1, length)==0L)

# populate data on R node site
qdf = quote({
    x <- data.frame(a = sample(letters,100,TRUE), b = rnorm(100), stringsAsFactors = FALSE)
    TRUE # to avoid collection of `<-` call
})
r = rscl.eval(rscl, qdf, lazy = FALSE)
stopifnot(r, length(r)==4L)

r = rscl.ls(rscl)
stopifnot(all.equal(r, setNames(rep("x",4L), port)))
r = capture.output(rscl.ls.str(rscl))
stopifnot(length(r)==16L)

# sum by group
df.r = rscl.eval(rscl, aggregate(b ~ a, x, sum), simplify = FALSE)
r1 = rbindlist(df.r)[, .(b = sum(b)),, a]

# using data.table
rscl.require(rscl, "data.table")
rscl.eval(rscl, is.data.table(setDT(x))) # is.data.table to avoid collection
dt.r = rscl.eval(rscl, x[, .(b = sum(b)), a], simplify = FALSE)
r2 = rbindlist(dt.r)[, .(b = sum(b)),, a]

# query parallely
invisible(rscl.eval(rscl, x[, .(b = sum(b)), a], wait = FALSE))
dt.r = rscl.collect(rscl, simplify = FALSE)
r3 = rbindlist(dt.r)[, .(b = sum(b)),, a]

# auto collect from parallel query
dt.r = rscl.eval(rscl, x[, .(b = sum(b)), a], parallel = TRUE, simplify = FALSE)
r4 = rbindlist(dt.r)[, .(b = sum(b)),, a]

stopifnot(
    nrow(r1)==26L,
    all.equal(r1, r2),
    all.equal(r2, r3),
    all.equal(r3, r4)
)

# sequential/parallel sleep
r1 = system.time(rscl.eval(rscl, Sys.sleep(0.1)))
r2 = system.time({
    rscl.eval(rscl, Sys.sleep(0.1), wait = FALSE)
    rscl.collect(rscl)
})
r3 = system.time(rscl.eval(rscl, Sys.sleep(0.1), parallel = TRUE))
stopifnot(
    r1[["elapsed"]] > 0.38,
    r2[["elapsed"]] < 0.12,
    r3[["elapsed"]] < 0.12
)

### Ways to create big.data.table ----

# populate source data on nodes from a function
f = function() CJ(1:1e2,1:5e2) # 1e2 smaller dataset in tests
bdt = as.big.data.table(f, rscl = rscl)
stopifnot(
    length(capture.output(print(bdt)))==12L,
    nrow(bdt)==200000L,
    length(capture.output(str(bdt)))==6L
)

# populate csv data on nodes, then load using function
rscl.eval(rscl, write.csv(iris, file = "data.csv", row.names = FALSE))
stopifnot(all.equal(rscl.eval(rscl, file.exists("data.csv")), setNames(rep(TRUE, 4L), port)))
# read from csv by function
f = function(file = "data.csv") fread(input = file)
bdt = as.big.data.table(f, rscl = rscl)
stopifnot(
    length(capture.output(print(bdt)))==12L,
    nrow(bdt)==600L,
    length(capture.output(str(bdt)))==9L,
    length(capture.output(rscl.ls.str(rscl)))==32L
)

# clean up
rscl.eval(rscl, rm(x, f))
stopifnot(all.equal(capture.output(rscl.ls.str(rscl)), sprintf("# Rserve node %s ----", port)))
rscl.eval(rscl, file.remove("data.csv"))
stopifnot(all.equal(rscl.eval(rscl, file.exists("data.csv")), setNames(rep(FALSE, 4L), port)))

# read data from call
qcall = quote(as.data.table(iris))
bdt = as.big.data.table(qcall, rscl = rscl)
stopifnot(
    nrow(bdt)==600L,
    length(capture.output(str(bdt)))==9L
)

# from data.table created locally
dt = as.data.table(iris)
bdt = as.big.data.table(dt, rscl = rscl)
stopifnot(
    nrow(bdt)==150L,
    length(capture.output(str(bdt)))==9L
)

# from rscl - data already in R nodes `.GlobalEnv$x`
bdt = as.big.data.table(x = rscl)
stopifnot(length(capture.output(str(bdt)))==9L)

### Query big.data.table ----

gen.data = function(n = 5e6, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl, n = 5e4) # 1e2 smaller dataset in tests
stopifnot(length(capture.output(str(bdt)))==9L)

# `[.big.data.table` will not aggregate results from nodes by default
r1 = bdt[, .(value = sum(value))]
r2 = bdt[, .(value = sum(value))][, .(value = sum(value))]
r3 = bdt[, .(value = sum(value)), outer.aggregate = TRUE]
stopifnot(
    nrow(r1)==4L,
    nrow(r2)==1L,
    all.equal(r2, r3)
)

r1 = bdt[, .(value = sum(value)), year, outer.aggregate = TRUE]
r2 = bdt[, .(value = sum(value)), .(year, low), outer.aggregate = TRUE]
r3 = bdt[, .(value = sum(value)), .(year, normal), outer.aggregate = TRUE]
stopifnot(
    nrow(r1)==4L,
    nrow(r2) > nrow(r1),
    nrow(r2) < nrow(r3)
)

# use `outer.aggregate=TRUE` only when used columns are not renamed
r1 = bdt[, .N, year, outer.aggregate = TRUE] # incorrect
r2 = bdt[, .N, year]
r3 = bdt[, .N, year][, sum(N), year] # correct
stopifnot(
    nrow(r1)==4L,
    all.equal(r1$N, rep(4L, 4L)),
    nrow(r2)==16L,
    nrow(r3)==4L,
    all.equal(r2[, sum(N), year], r3)
)

# some extra tests
stopifnot(
    # expected dims after aggregation
    all.equal(dim(bdt[, .(value = sum(value))]), c(4L,1L)),
    all.equal(dim(bdt[, .(value = sum(value))][, .(value = sum(value))]), c(1L,1L)),
    all.equal(dim(bdt[, .(value = sum(value)), year, outer.aggregate = TRUE]), c(4L,2L)),
    # parallel match to non-parallel
    all.equal(
        bdt[, .(value = sum(value)), .(year, high)],
        bdt[, .(value = sum(value)), .(year, high), parallel = FALSE]
    ),
    # expected print length
    all.equal(length(capture.output(print(bdt))), 12L),
    all.equal(length(capture.output(print(bdt, topn=2))), 6L),
    all.equal(length(capture.output(print(bdt, topn=1))), 4L),
    all.equal(length(capture.output(print(bdt, topn=10))), 22),
    # expected str
    isTRUE(capture.output(str(bdt, unclass=TRUE))[1L] %like% "0 obs. of  5 variables:"),
    all.equal(capture.output(str(bdt))[1L],"'big.data.table': 200000 obs. of 5 variables across 4 nodes:"),
    all.equal((pp <- capture.output(str(bdt)))[length(pp)-2L], "rows count by node:"),
    all.equal((pp <- capture.output(str(bdt)))[length(pp)], "50000 50000 50000 50000 ")
)

### Features of big.data.table ----

bdt = as.big.data.table(x = quote(as.data.table(iris)), rscl = rscl)

# dynamic metadata
stopifnot(
    all.equal(dim(bdt), c(600L, 5L)),
    nrow(bdt)==600L,
    ncol(bdt)==5L,
    all.equal(bdt[, .N], setNames(rep(150L, 4L), port)),
    bdt[, .(.N)][, sum(N)]==600L
)

# to be addressed by wrappers
# col names
r1 = RS.eval(rscl[[1L]], names(x))
# col class
r2 = RS.eval(rscl[[1L]], lapply(x, class))
stopifnot(
    all.equal(r1, c("Sepal.Length", "Sepal.Width", "Petal.Length", "Petal.Width", "Species")),
    all.equal(r2, list(Sepal.Length = "numeric", Sepal.Width = "numeric", Petal.Length = "numeric", Petal.Width = "numeric", Species = "factor"))
)

# `[.big.data.table` - `new.var` create new big.data.table from existing one

bdty = bdt[, mean(Petal.Width), Species, new.var = "y"]
stopifnot(
    last(capture.output(str(bdty)))=="    3     3     3     3 ",
    last(capture.output(str(bdt)))=="  150   150   150   150 "
)
# can be multiple bdt pointing to same machine but different variable names
r = rscl.ls(rscl)
stopifnot(
    all.equal(r[1,], setNames(rep("gen.data", 4L), port)),
    all.equal(r[2,], setNames(rep("x", 4L), port)),
    all.equal(r[3,], setNames(rep("y", 4L), port))
)

# `[[.big.data.table`

r = bdt[[expr = nrow(x)]]
stopifnot(all.equal(r, setNames(rep(150L, 4L), port)))

# nrow of both datasets on nodes
r1 = rscl.eval(rscl, c(x=nrow(x), y=nrow(y)))
r2 = bdt[[expr = c(x=nrow(x), y=nrow(y))]]
r3 = bdty[[expr = c(x=nrow(x), y=nrow(y))]]
stopifnot(
    all.equal(r1[1,], setNames(rep(150L, 4L), port)),
    all.equal(r1[2,], setNames(rep(3L, 4L), port)),
    sapply(r2, function(x) all(names(x)==c("x","y"), x==c(150L,3L))),
    all.equal(r2, r3)
)

# same query different ways
r1 = bdt[, lapply(.SD, sum), Species]
r2 = rscl.eval(rscl, x[, lapply(.SD, sum), Species], simplify = FALSE)
r3 = bdt[[expr = x[, lapply(.SD, sum), Species]]]
# re-aggregate after rbind
r4 = bdt[, lapply(.SD, sum), Species, outer.aggregate=TRUE]
stopifnot(
    all.equal(r1, rbindlist(r2)),
    all.equal(r1, r3),
    all.equal(r1[, lapply(.SD, sum), Species], r4)
)

# having two big.data.tables and `[[` we can easily join within the scope of node
r = bdt[[expr = y[x, on = "Species"]]]
stopifnot("V1" %in% names(r))

# size
r1 = bdt[[expr = sprintf("%.4f MB", object.size(x)/(1024^2))]]
r2 = sprintf("total size: %.4f MB", sum(bdt[[expr = object.size(x)]])/(1024^2))
stopifnot(
    is.character(r1),
    length(r1)==4L,
    is.character(r2),
    length(r2)==1L
)

### Partitioning ----

dt = gen.data(n=2e5)

# no partitioning
bdt = as.big.data.table(x = dt, rscl = rscl)
stopifnot(
    nrow(bdt)==200000,
    identical(bdt[[expr = nrow(x)]], setNames(rep(50000L, 4), port))
)
r.no.part = bdt[[expr = x[, .N, year], rbind = FALSE]]
stopifnot(
    is.list(r.no.part),
    sapply(r.no.part, nrow)==4L
)

# partition by "year"
partition.by = "year"
bdt = as.big.data.table(x = dt, rscl = rscl, partition.by = partition.by)
stopifnot(
    nrow(bdt)==200000,
    !identical(bdt[[expr = nrow(x)]], setNames(rep(50000L, 4), port)) # non-identical
)
r.part = bdt[[expr = x[, .N, year], rbind = FALSE]]
stopifnot(
    is.list(r.part),
    sapply(r.part, nrow)==1L
)

# fetch data from all nodes to local session
r = as.data.table(bdt)
stopifnot(all.equal(
    identical(dim(r[, .N, year]), c(4L, 2L)),
    sum(r$value)==sum(dt$value),
    length(r$value)==length(dt$value),
    all.equal(r[, .(value=sum(value), .N),, .(year)], dt[, .(value=sum(value), .N),, .(year)]),
    all.equal(r[, .(value=sum(value), .N),, .(low)], dt[, .(value=sum(value), .N),, .(low)])
))

rm(r, dt)
r = rscl.ls(rscl)
stopifnot(
    is.character(r),
    identical(dim(r), c(3L,4L)) # gen.data, x, y
)

# closing workspace ----

# disconnect
rscl.close(rscl)
