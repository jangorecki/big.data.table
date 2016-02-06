library(RSclient)
library(data.table)
library(big.data.table)

# connect
host = "127.0.0.1"
port = 33311:33314
rscl = rscl.connect(port, host, pkgs = "data.table")

# populate
gen.data = function(n = 5e4, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
stopifnot(
    all.equal(dim(bdt), c(length(port)*5e4, 5L)),
    all.equal(bdt.eval.log(bdt, dim(x)), lapply(1:4, function(i) c(5e4, 5L)), check.attributes = FALSE),
    all.equal(bdt[, .(value = sum(value)), outer.aggregate = TRUE]$value, sum(bdt[, .(value = sum(value)), year]$value)),
    all.equal(bdt[, .(value = sum(value)), .(year, high)], bdt[, .(value = sum(value)), .(year, high), parallel = FALSE])
)

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))

# disconnect
rscl.close(rscl)
