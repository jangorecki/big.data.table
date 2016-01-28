Distributed parallel computing on data.table.  

# Installation

Below commands will install latest big.data.table with its dependencies.  

```r
install.packages(c("RSclient","Rserve"), repos = "https://rforge.net")
install.packages("data.table", repos = "https://cran.rstudio.com")
install.packages("big.data.table", repos = "http://jangorecki.gitlab.io/big.data.table")
```

# Starting nodes

Below are two options to start Rserve nodes. Follow with the second option for reproducibility on a localhost machine.  

## Run nodes as docker services

You can use docker image based on Ubuntu 14.04 configured for `big.data.table`, image details: [jangorecki/r-data.table-pg](https://hub.docker.com/r/jangorecki/r-data.table-pg).  
It may be useful for fast ad-hoc remote environment setup R nodes. Commands will also work locally if you have docker installed.  

```sh
docker run -d -h rnode11 -p 33311:6311 --name=rnode11 jangorecki/r-data.table-pg
docker run -d -h rnode12 -p 33312:6311 --name=rnode12 jangorecki/r-data.table-pg
docker run -d -h rnode13 -p 33313:6311 --name=rnode13 jangorecki/r-data.table-pg
docker run -d -h rnode14 -p 33314:6311 --name=rnode14 jangorecki/r-data.table-pg
```

## Run nodes from R

```r
library(Rserve)
port = 33311:33314
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))
```

# Usage

You should have nodes already started.

## Connect to R nodes

```r
library(RSclient)
library(data.table)
library(big.data.table)

port = 33311:33314
# wrapper to sapply on RS.connect with recycling, auto require pkgs
rscl = rscl.connect(port, pkgs = "data.table")

# R version on computing nodes using `RSclient::RS.eval` and `sapply`
sapply(rscl, RS.eval, R.version.string)
```

## Using rscl.* wrappers

`rscl.*` functions are kind of intermediate step on which whole `big.data.table` package is built upon. They makes some of the `RSclient::RS.*` functions vectorized, allowing to process a list of connections to R nodes.  
It can be effectively utilized for batch access/processing any data spread across R nodes.  

```r
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
rbindlist(df.r)[, .(b = sum(b)),, a]

# using data.table
rscl.require(rscl, "data.table")
rscl.eval(rscl, is.data.table(setDT(x))) # is.data.table to avoid collection
dt.r = rscl.eval(rscl, x[, .(b = sum(b)), a], simplify = FALSE)
rbindlist(dt.r)[, .(b = sum(b)),, a]

# query parallely
rscl.eval(rscl, x[, .(b = sum(b)), a], wait = FALSE)
dt.r = rscl.collect(rscl, simplify = FALSE)
rbindlist(dt.r)[, .(b = sum(b)),, a]

# sequential/parallel sleep
system.time(rscl.eval(rscl, Sys.sleep(1)))
system.time({
    rscl.eval(rscl, Sys.sleep(1), wait = FALSE)
    rscl.collect(rscl)
})
```

## Using big.data.table

`big.data.table` class stores `rscl` attribute having list of connections to R nodes always on hand. It catches `[.big.data.table` calls and forward them to R nodes and execute as `[.data.table` calls on chunks of data.  
It has some useful features like auto collection from parallel processing, row bind results from nodes, exception handling, logging and metadata collection.  

### Ways to create big.data.table

```r
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
```

### Query big.data.table

```r
gen.data = function(n = 5e6, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
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
```

### Features of big.data.table

```r
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
```

### Partitioning big.data.table

```r
dt = gen.data(n=2e7)

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

# fetch data from all nodes to local session
r = as.data.table(bdt)
r[, .N, year]

rm(r, dt)
rscl.ls(rscl)
```

## Disconnect nodes

```r
rscl.close(rscl)
```

### Shutdown nodes started from R

```r
port = 33311:33314
l = lapply(setNames(nm = port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))
```

### Shutdown nodes started as docker images

```sh
docker stop rnode11 rnode12 rnode13 rnode14
```

# Notes

Interesting finding by Szilard Pafka why you may not even need big.data.table package in future: [Big RAM is eating big data – Size of datasets used for analytics](http://datascience.la/big-ram-is-eating-big-data-size-of-datasets-used-for-analytics/)  
