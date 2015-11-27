library(RSclient)
library(data.table)
library(big.data.table)

# connect
port = 33311:33314
rscl = rsc(port)

# populate
gen.data = function(n = 5e4, seed = 123, ...){
    set.seed(seed)
    data.table(year = sample(2011:2014, n, TRUE), high = sample(n*0.9, n, TRUE), normal = sample(n*0.1, n, TRUE), low = sample(letters, n, TRUE), value = rnorm(n))
}
bdt = as.big.data.table(x = gen.data, rscl = rscl)
stopifnot(
    all.equal(bdt[[expr = ls()]], setNames(rep("x", 4L), port)),
    all.equal(bdt[[expr = ls(), simplify = FALSE]], as.list(setNames(rep("x", 4L), port))),
    all.equal(bdt[[expr = is.data.table(x)]], setNames(rep(TRUE, 4L), port)),
    all.equal(bdt[[expr = exists("bdt")]], setNames(rep(FALSE, 4L), port)),
    all.equal(names(bdt[[expr = x[, .N, .(year, high)]]]), c("year","high","N")),
    all.equal(nrow(bdt[[expr = x[, .N, .(year, high)]]]), sum(bdt[[expr = nrow(x[, .N, .(year, high)])]])),
    all.equal(nrow(bdt[[expr = x[, .N, .(year, high)]]]), nrow(bdt[[expr = x[, .N, .(year, high)]]][, .(N=sum(N)), .(year, high)]) * 4L), # not auto aggregate results from nodes
    all.equal(lapply(bdt[[expr = x[, .N, .(year, high)], rbind=FALSE]], nrow), lapply(setNames(rep(43587L, 4),port), function(x) x)),
    all.equal(bdt[[1L]], integer(0)),
    all.equal(bdt[[4L]], character(0)),
    all.equal(bdt[["year"]], integer(0)),
    all.equal(bdt[["low"]], character(0)),
    all.equal(lapply(bdt, function(x) x), list(year = integer(0), high = integer(0), normal = integer(0), low = character(0), value = numeric(0))) # empty columns on `[[` with numeric
)

# closing workspace ----

# cleanup all objects in all nodes, excluding prefixed with dot '.'
r = sapply(attr(bdt,"rscl"), RS.eval, rm(list=ls()))

# disconnect
sapply(rscl, RS.close)
