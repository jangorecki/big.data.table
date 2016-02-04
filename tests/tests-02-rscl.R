library(RSclient)
library(data.table)
library(big.data.table)

# connect recycling ----

## 4u port, 1u host
host = "127.0.0.1"
port = 33311:33314
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.ls(rscl),
    setNames(rep(list(character(0)),length(port)), port)
))
## 4u port, 4 host
port = 33311:33314
host = rep("127.0.0.1", length(port))
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.ls(rscl),
    rep(list(character(0)),length(host))
))
## 4 port, 4 host
host = rep("127.0.0.1", 4L)
port = rep(33311, length(host))
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.ls(rscl),
    rep(list(character(0)),length(host))
))
## 1u port, 4 host
host = rep("127.0.0.1", 4L)
port = 33311
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.ls(rscl),
    setNames(rep(list(character(0)),length(host)), host)
))

# env vars ----

Sys.setenv(RSERVE_HOST="0.42.42.42") # invalid ip
port = 33311:33314
r = tryCatch(rscl.connect(port), error = function(e) e)
stopifnot(inherits(r, "error"))

Sys.setenv(RSERVE_HOST="127.0.0.1")
port = 33311:33314
rscl = rscl.connect(port)
stopifnot(all.equal(
    rscl.ls(rscl),
    setNames(rep(list(character(0)),length(port)), port)
))

# require ----

# single package require
stopifnot(rscl.require(rscl, "methods"))

# multi node multi package require
stopifnot(rscl.require(rscl, c("methods","data.table")))

# non-available package require return FALSE
stopifnot(!rscl.require(rscl, c("asdasdasdasd","asdasdasdasd2")))

# ls.str ----

stopifnot(
    rscl.eval(rscl, {x <- data.table(iris); TRUE}, lazy = TRUE),
    length(capture.output(rscl.ls.str(rscl))) == 28L
)

stopifnot(
    rscl.eval(rscl, quote({y <- data.table(iris); TRUE}), lazy = FALSE),
    length(capture.output(rscl.ls.str(rscl))) == 52L
)

# disconnect
rscl.close(rscl)
