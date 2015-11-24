library(RSclient)
library(data.table)
library(big.data.table)

# connect
port = 9411:9414
rscl = rsc(port)

# populate
gen.data = function(n = 5e4, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
stopifnot(
    all.equal(dim(bdt), c(length(port)*5e4, 5L)),
    all.equal(bdt.eval(bdt, dim(x)), lapply(1:4, function(i) c(5e4, 5L)), check.attributes = FALSE),
    bdt[, .(value = sum(value))]$value==sum(bdt[, .(value = sum(value)), year]$value),
    all.equal(bdt[, .(value = sum(value)), .(year, high)], bdt[, .(value = sum(value)), .(year, high), parallel = FALSE])
)

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))

# disconnect
sapply(rscl, RS.close)
