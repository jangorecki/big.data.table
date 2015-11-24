library(RSclient)
library(data.table)
library(big.data.table)

# connect
port = 9411:9414
rscl = rsc(port)
stopifnot(is.rsc(rscl))

# Initialize big.data.table ----

# populate source data
lapply(rscl, RS.eval, write.csv(iris, file = "data.csv", row.names = FALSE))

# from function
f = function(file = "data.csv") fread(input = file)
bdt = as.big.data.table(f, rscl = rscl)
stopifnot(is.big.data.table(bdt), nrow(bdt)==600L)

# clean up
sapply(rscl, RS.eval, rm(x))
rm(bdt)
sapply(rscl, RS.eval, file.remove("data.csv"))

# from call
qcall = quote(data.table(iris))
bdt = as.big.data.table(qcall, rscl = rscl)
stopifnot(is.big.data.table(bdt), nrow(bdt)==600L)

sapply(rscl, RS.eval, rm(x))
rm(bdt)

# from data.table
dt = data.table(iris)
bdt = as.big.data.table(dt, rscl = rscl)
stopifnot(is.big.data.table(bdt), nrow(bdt)==150L)

# from list - data must be already in the node R session
bdt = as.big.data.table(x = rscl)
stopifnot(is.big.data.table(bdt), nrow(bdt)==150L)

# Compute on big.data.table ----

gen.data = function(n = 5e4, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
stopifnot(
    dim(bdt[, .(value = sum(value))])==c(1L,1L),
    dim(bdt[, .(value = sum(value)), year])==c(4L,2L),
    all.equal(
        bdt[, .(value = sum(value)), .(year, high)],
        bdt[, .(value = sum(value)), .(year, high), parallel = FALSE]
    )
)

# Features of big.data.table ----

dt = gen.data(n=2e5)

# no partitioning
bdt = as.big.data.table(x = dt, rscl = rscl)
r.no.part.nr = bdt[[expr = nrow(x)]]
r.no.part = bdt[[expr = x[, .N, year], rbind = FALSE]]

# partition by "year"
partition.by = "year"
bdt = as.big.data.table(x = dt, rscl = rscl, partition.by = partition.by)
r.part.nr = bdt[[expr = nrow(x)]]
r.part = bdt[[expr = x[, .N, year], rbind = FALSE]]

stopifnot(
    sapply(bdt, length)==0L,
    r.no.part.nr==2e5/4L,
    !all(r.part.nr==2e5/4L),
    sapply(r.no.part, nrow)==4L,
    sapply(r.part, nrow)==1L
)

# extract to local data.table

r = as.data.table(bdt)
stopifnot(all.equal(
    sum(r$value)==sum(dt$value),
    length(r$value)==length(dt$value),
    all.equal(r[, .(value=sum(value), .N),, .(year)], dt[, .(value=sum(value), .N),, .(year)]),
    all.equal(r[, .(value=sum(value), .N),, .(low)], dt[, .(value=sum(value), .N),, .(low)])
))

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))
stopifnot(all.equal(lapply(attr(bdt,"rscl"), RS.eval, ls()), lapply(rep(0,4), character), check.attributes = FALSE))

# disconnect
sapply(rscl, RS.close)
