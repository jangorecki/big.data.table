library(RSclient)
library(data.table)
library(big.data.table)

# connect recycling ----

## 4u port, 1u host
host = "127.0.0.1"
port = 33311:33314
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.eval(rscl, ls()),
    setNames(rep(list(character(0)),length(port)), port)
))
## 4u port, 4 host
port = 33311:33314
host = rep("127.0.0.1", length(port))
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.eval(rscl, ls()),
    rep(list(character(0)),length(host))
))
## 4 port, 4 host
host = rep("127.0.0.1", 4L)
port = rep(33311, length(host))
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.eval(rscl, ls()),
    rep(list(character(0)),length(host))
))
## 1u port, 4 host
host = rep("127.0.0.1", 4L)
port = 33311
rscl = rscl.connect(port, host)
stopifnot(all.equal(
    rscl.eval(rscl, ls()),
    setNames(rep(list(character(0)),length(host)), host)
))

# connect env vars ----

#Sys.setenv()

# require ----

# single package require
stopifnot(rscl.require(rscl, "pg"))

# multi node multi package require
stopifnot(rscl.require(rscl, c("data.table","pg")))

# non-available package require return FALSE
stopifnot(!rscl.require(rscl, c("asdasdasdasd","asdasdasdasd2")))

# ls.str ----

# lazy TRUE
stopifnot(
    rscl.eval(rscl, {x <- data.table(iris); TRUE}),
    length(capture.output(rscl.ls.str(rscl))) == 32L
)

# lazy FALSE
stopifnot(
    rscl.eval(rscl, quote({y <- data.table(iris); TRUE}), lazy = FALSE),
    length(capture.output(rscl.ls.str(rscl))) == 56L
)

# lazy FALSE, expr list
expr_list = list(
    clean = quote(rm(list=ls())),
    def1 = quote({a <- 1:3; TRUE}),
    def2 = quote({b <- letters[1:2]; TRUE})
)
rscl.eval(rscl, expr_list, lazy = FALSE)
stopifnot(length(capture.output(rscl.ls.str(rscl))) == 16L)
