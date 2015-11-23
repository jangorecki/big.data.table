library(RSclient)
library(data.table)
library(big.data.table)

# connecet
port = 9411:9414
rscl = rsc(port)
stopifnot(is.rsc(rscl))

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

sapply(rscl, RS.close)
