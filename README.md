Distributed parallel computing on data.table.  

# Installation

Below commands will install latest big.data.table release.  

```r
install.packages(c("RSclient","Rserve"), repos = "https://rforge.net")
install.packages("data.table", repos = "https://cran.rstudio.com")
install.packages("big.data.table", repos = "https://jangorecki.github.io/big.data.table")
```

To use development version install from [big.data.table](https://gitlab.com/jangorecki/big.data.table) repo.  

# Starting nodes

Nodes are started as processes working in the background as services.  
You can use docker image based on Ubuntu 14.04 configured for `big.data.table` with postgres connection support. That still requires you to have a postgres instance - those are easy to start from docker too, see []().  

## Run nodes as docker services

Docker image details: [jangorecki/r-data.table](https://hub.docker.com/r/jangorecki/r-data.table)

```sh
docker run -d -h rnode11 -p 33311:6311 --name=rnode11 jangorecki/r-data.table-pg
docker run -d -h rnode12 -p 33312:6311 --name=rnode12 jangorecki/r-data.table-pg
docker run -d -h rnode13 -p 33313:6311 --name=rnode13 jangorecki/r-data.table-pg
docker run -d -h rnode14 -p 33314:6311 --name=rnode14 jangorecki/r-data.table-pg
```

## Run nodes from R

```r
library(Rserve)
library(RSclient)

port = 33311:33314
# start cluster
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))
```

# Usage

You should have nodes already started.

```r
library(Rserve)
library(RSclient)
library(data.table)
library(big.data.table)

port = 33311:33314
# wrapper to lapply on RS.connect with recycling
rscl = rsc(port, host="172.17.0.1")
# print objects in working directory of each node
lapply(rscl, RS.eval, ls())

# populate source data on nodes from function
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

## Compute on big.data.table

```r
port = 33311:33314
rscl = rsc(port)

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

## Features of big.data.table

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

# disconnect
sapply(rscl, RS.close)
```

## Closing nodes

### Shutdown nodes started from R

```r
port = 33311:33314
l = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))
```

### Shutdown nodes started as docker images

```sh
docker stop rnode11 rnode12 rnode13 rnode14
```

# Notes

If you are stuck with uncollected results from nodes on Rserve connections you can force collect by `lapply(attr(bdt, "rscl"), function(x) try(RS.collect(x), silent=TRUE))`.  
  
Interesting finding by Szilard Pafka why you may not even need big.data.table package in future: [Big RAM is eating big data – Size of datasets used for analytics](http://datascience.la/big-ram-is-eating-big-data-size-of-datasets-used-for-analytics/)  
