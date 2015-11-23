library(RSclient)

port = 9411:9414

# shutdown any running nodes
l = lapply(setNames(port, port), function(port) tryCatch(RSconnect(port = port), error = function(e) e, warning = function(w) w))
invisible(lapply(l, function(rsc) if(inherits(rsc, "sockconn")) RSshutdown(rsc)))

invisible(TRUE)