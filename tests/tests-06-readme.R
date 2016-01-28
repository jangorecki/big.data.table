
## Connect to R nodes ----
# TO DO: add tests
library(RSclient)
library(data.table)
library(big.data.table)

port = 33311:33314
# wrapper to sapply on RS.connect with recycling, auto require pkgs
rscl = rscl.connect(port, pkgs = "data.table")

# R version on computing nodes using `RSclient::RS.eval` and `sapply`
sapply(rscl, RS.eval, R.version.string)

## Using rscl.* wrappers

rscl.eval(rscl, ls())
rscl.ls(rscl)

# populate data on R node site
qdf = quote({
    x <- data.frame(a = sample(letters,100,TRUE), b = rnorm(100))
    TRUE # to avoid collection of `<-` call
})
rscl.eval(rscl, qdf, lazy = FALSE)

rscl.ls(rscl)
rscl.ls.str(rscl)

# sum by group
df.r = rscl.eval(rscl, aggregate(b ~ a, x, sum), simplify = FALSE)
rbindlist(df.r)[, .(b = sum(b)), a]

# using data.table
rscl.require(rscl, "data.table")
rscl.eval(rscl, is.data.table(setDT(x))) # is.data.table to avoid collection
dt.r = rscl.eval(rscl, x[, .(b = sum(b)), a], simplify = FALSE)
rbindlist(dt.r)[, .(b = sum(b)),, a]

# query parallely
rscl.eval(rscl, x[, .(b = sum(b)), a], wait = FALSE)
dt.r = rscl.collect(rscl, simplify = FALSE)
rbindlist(dt.r)[, .(b = sum(b)), a]

# sequential/parallel sleep
system.time(rscl.eval(rscl, Sys.sleep(1)))
system.time({
    rscl.eval(rscl, Sys.sleep(1), wait = FALSE)
    rscl.collect(rscl)
})

### Ways to create big.data.table ----
# TO DO: add tests
# populate source data on nodes from a function
f = function() CJ(1:1e3,1:5e3) # 5M rows
bdt = as.big.data.table(f, rscl = rscl)
print(bdt)
nrow(bdt)
str(bdt)

# populate csv data on nodes from function
rscl.eval(rscl, write.csv(iris, file = "data.csv", row.names = FALSE))
# read from csv by function
f = function(file = "data.csv") fread(input = file)
bdt = as.big.data.table(f, rscl = rscl)
print(bdt)
nrow(bdt)
str(bdt)
rscl.ls.str(rscl)

# clean up
rscl.eval(rscl, rm(x, f))
rscl.eval(rscl, file.remove("data.csv"))

# read data from call
qcall = quote(data.table(iris))
bdt = as.big.data.table(qcall, rscl = rscl)
str(bdt)

# from data.table created locally
dt = data.table(iris)
bdt = as.big.data.table(dt, rscl = rscl)
str(bdt)

# from rscl - data already in R nodes `.GlobalEnv$x`
bdt = as.big.data.table(x = rscl)
str(bdt)

### Query big.data.table ----

gen.data = function(n = 5e6, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl, n = 5e4) # 1e2 smaller dataset in tests
str(bdt)

# `[.big.data.table` will not aggregate results from nodes by default
bdt[, .(value = sum(value))]
bdt[, .(value = sum(value))][, .(value = sum(value))]
bdt[, .(value = sum(value)), outer.aggregate = TRUE]

bdt[, .(value = sum(value)), year, outer.aggregate = TRUE]
bdt[, .(value = sum(value)), .(year, low), outer.aggregate = TRUE]
bdt[, .(value = sum(value)), .(year, normal), outer.aggregate = TRUE]

# use `outer.aggregate=TRUE` only when used columns are not renamed
bdt[, .N, year, outer.aggregate = TRUE] # incorrect
bdt[, .N, year]
bdt[, .N, year][, sum(N), year] # correct

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
# TO DO: add tests
bdt = as.big.data.table(x = quote(as.data.table(iris)), rscl = rscl)

# dynamic metadata
dim(bdt)
nrow(bdt)
ncol(bdt)
bdt[, .N]
bdt[, .(.N)]

# to be addressed by wrappers
# col names
RS.eval(rscl[[1L]], names(x))
# col class
RS.eval(rscl[[1L]], lapply(x, class))

# `[.big.data.table` - `new.var` create new big.data.table from existing one

bdty = bdt[, mean(Petal.Width), Species, new.var = "y"]
str(bdty)
str(bdt)
# can be multiple bdt pointing to same machine but different variable names
rscl.ls(rscl)

# `[[.big.data.table`

bdt[[expr = nrow(x)]]

# nrow of both datasets on nodes
rscl.eval(rscl, c(x=nrow(x), y=nrow(y)))
bdt[[expr = c(x=nrow(x), y=nrow(y))]]
bdty[[expr = c(x=nrow(x), y=nrow(y))]]

# same query different ways
bdt[, lapply(.SD, sum), Species]
rscl.eval(rscl, x[, lapply(.SD, sum), Species], simplify = FALSE)
bdt[[expr = x[, lapply(.SD, sum), Species]]]
# re-aggregate after rbind
bdt[, lapply(.SD, sum), Species, outer.aggregate=TRUE]

# having two big.data.tables and `[[` we can easily join then
bdt[[expr = y[x, on = "Species"]]]

# size
bdt[[expr = sprintf("%.4f MB", object.size(x)/(1024^2))]]
sprintf("total size: %.4f MB", sum(bdt[[expr = object.size(x)]])/(1024^2))

### Partitioning ----

dt = gen.data(n=2e5)

# no partitioning
bdt = as.big.data.table(x = dt, rscl = rscl)
bdt[[expr = nrow(x)]]
r.no.part = bdt[[expr = x[, .N, year], rbind = FALSE]]
print(r.no.part)

# partition by "year"
partition.by = "year"
bdt = as.big.data.table(x = dt, rscl = rscl, partition.by = partition.by)
bdt[[expr = nrow(x)]]
r.part = bdt[[expr = x[, .N, year], rbind = FALSE]]
print(r.part)

# size
bdt[[expr = sprintf("%.4f MB", object.size(x)/(1024^2))]]
sprintf("total size: %.4f MB", sum(bdt[[expr = object.size(x)]])/(1024^2))

stopifnot(
    sapply(r.no.part, nrow)==4L,
    sapply(r.part, nrow)==1L
)

# fetch data from all nodes to local session
r = as.data.table(bdt)
r[, .N, year]

stopifnot(all.equal(
    sum(r$value)==sum(dt$value),
    length(r$value)==length(dt$value),
    all.equal(r[, .(value=sum(value), .N),, .(year)], dt[, .(value=sum(value), .N),, .(year)]),
    all.equal(r[, .(value=sum(value), .N),, .(low)], dt[, .(value=sum(value), .N),, .(low)])
))

rm(r, dt)
rscl.ls(rscl)

# disconnect
rscl.close(rscl)
