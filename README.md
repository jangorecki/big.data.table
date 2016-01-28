Distributed parallel computing on data.table.  

# Installation

Below commands will install latest big.data.table with its dependencies.  

```r
install.packages(c("RSclient","Rserve"), repos = "https://rforge.net")
install.packages("data.table", repos = "https://cran.rstudio.com")
install.packages("big.data.table", repos = "https://jangorecki.gitlab.io/big.data.table")
```

# Starting nodes

Nodes are started as processes working in the background as services.  
You can use docker image based on Ubuntu 14.04 configured for `big.data.table` with postgres drivers preinstalled. That still requires you to have a postgres instance - those are easy to start from docker too, see [postgres in docker hub](https://hub.docker.com/r/library/postgres/).  

## Run nodes as docker services

Docker image details: [jangorecki/r-data.table-pg](https://hub.docker.com/r/jangorecki/r-data.table-pg)

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
# start cluster
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))
```

# Usage

You should have nodes already started.

## Connect to R nodes

Connect to R nodes.  

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

`rscl.*` function are kind of intermediate step on which whole `big.data.table` package is built uppon. They makes some of the `RSclient::RS.*` functions vectorized, allowing to process a list of connections to R nodes.  
It can be effectively utilized for batch access/processing any data spread across R nodes.  
`RS.eval` allows to send expression and later collect results with `RS.collect` which allows for parallel processing.  

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
```

## Using big.data.table

`big.data.table` class stores `rscl` attribute having list of connections to R nodes always on hand. It catches `[.big.data.table` calls and forward them to R nodes and execute as `[.data.table` calls on chunks of data.  
I has some useful features like auto collection from parallel processing, row bind results from nodes, exception handling, logging and metadata collection.  

### Ways to create big.data.table

```r
# populate source data on nodes from a function
f = function() CJ(1:1e3,1:5e3) # 5M rows
bdt = as.big.data.table(f, rscl = rscl)
print(bdt)
str(bdt)

# populate csv data on nodes
lapply(rscl, RS.eval, write.csv(iris, file = "data.csv", row.names = FALSE))
# read from csv by function
f = function(file = "data.csv") fread(input = file)
bdt = as.big.data.table(f, rscl = rscl)
print(bdt)
str(bdt)

# clean up
sapply(rscl, RS.eval, rm(x))
rm(bdt)
sapply(rscl, RS.eval, file.remove("data.csv"))

# read data from call
qcall = quote(data.table(iris))
bdt = as.big.data.table(qcall, rscl = rscl)
print(bdt)
str(bdt)

sapply(rscl, RS.eval, rm(x))
rm(bdt)

# from data.table created locally
dt = data.table(iris)
bdt = as.big.data.table(dt, rscl = rscl)
print(bdt)
str(bdt)

# from list - data must be already in the node R session
bdt = as.big.data.table(x = rscl)
print(bdt)
str(bdt)
```

### Query big.data.table

```r
port = 33311:33314
rscl = rscl.connect(port, pkgs = "data.table")

gen.data = function(n = 5e6, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
str(bdt)

bdt[, .(value = sum(value))]
bdt[, .(value = sum(value)), year]
bdt[, .(value = sum(value)), .(year, low)]
bdt[, .(value = sum(value)), .(year, normal)]

# processing timing
op = options("bigdatatable.verbose"=TRUE)
bdt[, .(value = sum(value)), .(year, high)]
options(op)
```

### Features of big.data.table

```r
names(bdt)
dim(bdt)
nrow(bdt)
ncol(bdt)
sapply(bdt, class)
# bdt exposes 0 rows copy of distributed data.table
sapply(bdt, length)
# to get expected output
l = lapply(rscl, RS.eval, sapply(x, length))
Reduce("+", l)
# or use conviniet wrapper
l = bdt[[expr = sapply(x, length)]]
Reduce("+", l)
# or a one liner with chained data.table query
bdt[[expr = as.data.table(lapply(x, length))]][, lapply(.SD, sum)]

# partitioning can be handled automatically

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

# size
bdt[[expr = sprintf("%.4f MB", object.size(x)/(1024^2))]]
sprintf("total size: %.4f MB", sum(bdt[[expr = object.size(x)]])/(1024^2))

# [.big.data.table - [.data.table redirection

# this will not work as data to aggregate has been renamed
bdt[, .(value = sum(value)), .(year, normal2 = normal)]
# you can always workaround that directly using `bdt[[expr = ...]]`
r = bdt[[expr = x[, .(value = sum(value)), .(year, normal2 = normal)]]]
r[, .(value = sum(value)), .(year, normal2)]

# fetch data from all nodes to local session
r = as.data.table(bdt)
r[, .N, year]
rm(r, dt)
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

If you are stuck with uncollected results from nodes on Rserve connections you can force collect by `lapply(attr(bdt, "rscl"), function(x) try(RS.collect(x), silent=TRUE))`.  
  
Interesting finding by Szilard Pafka why you may not even need big.data.table package in future: [Big RAM is eating big data – Size of datasets used for analytics](http://datascience.la/big-ram-is-eating-big-data-size-of-datasets-used-for-analytics/)  
