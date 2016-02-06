library(Rserve)

port = 33311:33314

# shutdown any running nodes
rscl = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(rscl, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))

# start cluster
invisible(sapply(port, function(port) Rserve(debug = FALSE, port = port, args = c("--no-save"))))

options("bigdatatable.log" = FALSE) # for interactive running tests
invisible(TRUE)