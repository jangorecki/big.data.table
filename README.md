Distributed parallel computing on data.table.  

# Installation

```r
install.packages("microbenchmarkCore", repos = "https://olafmersmann.github.io/drat")
install.packages(c("RSclient","Rserve"), repos = "https://rforge.net")
install.packages("data.table", repos = "https://cran.rstudio.com")
install.packages("big.data.table", repos = "https://jangorecki.github.io/big.data.table")
```

# Usage

## Initialize big.data.table

```r
library(Rserve)
library(RSclient)
library(data.table)
library(big.data.table)

port = 9411:9414
# start cluster
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))

# wrapper to lapply on RS.connect with recycling
rscl = rsc(port)
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
port = 9411:9414
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

# shutdown nodes
l = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))
```

# Notes

If you are stuck with uncollected results from nodes on Rserve connections you can force collect by `lapply(attr(bdt, "rscl"), function(x) try(RS.collect(x), silent=TRUE))`.  
  
Interesting finding by Szilard Pafka why you may not even need big.data.table package in future: [Big RAM is eating big data – Size of datasets used for analytics](http://datascience.la/big-ram-is-eating-big-data-size-of-datasets-used-for-analytics/)  
