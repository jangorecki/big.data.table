# wrap nodes into big.data.table, then expose it as gateway node so Rserve clients can consume big.data.table interface

library(Rserve)
library(data.table)
library(big.data.table)

# initialize R big.data.table gateways
invisible(Rserve(debug = FALSE, port = 23311L, args = c("--no-save")))
invisible(Rserve(debug = FALSE, port = 23312L, args = c("--no-save")))
rsc = rscl.connect(port = c(23311:23312))
rscl.require(rsc, package = c("data.table","big.data.table"))

# rsc - big.data.table gateways to data, usually length 1L (testing on length 2L, so x2 gives 8 R nodes)
# rscl - list of nodes
qsetup = quote({
    rscl = rscl.connect(port = 33311:33314)
    rscl.require(rscl, package = "data.table")
    data = quote(as.data.table(iris))
    bdt = as.big.data.table(data, rscl)
    TRUE
})
rscl.eval(rsc, qsetup, lazy = FALSE)

rvar = rscl.eval(rsc, rscl.ls(rscl))
r = rbindlist(rscl.eval(rsc, bdt[, lapply(.SD, mean), Species], simplify = FALSE)) # binding results from 2 gateways

stopifnot(
    all(rvar=="x"),
    is.data.table(r), ncol(r)==5L, nrow(r)==24L
)

# shutdown gateway

rscl.close(rsc)
rsc = RSclient::RSconnect(port = 23311)
RSclient::RSshutdown(rsc)
rsc = RSclient::RSconnect(port = 23312)
RSclient::RSshutdown(rsc)
rm(rsc)
