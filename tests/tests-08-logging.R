## for check on localhost use postgres service
# docker run --rm -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres --name bdt-logging-test postgres:9.5

# if no gitlab-ci then skip if R client doesn't have logR or cannot connect db ----

skipifnot = function(x){
    gl = identical(Sys.getenv("CI_SERVER_NAME"), "GitLab CI")
    if(!gl & !isTRUE(all(x))) q(save = "no", status = 0)
}

# skip tests if logR not available on client
skipifnot(requireNamespace("logR", quietly = TRUE))

library(RSclient)
library(data.table)
library(big.data.table)
library(logR)

# connect R nodes
host = "127.0.0.1"
port = 33311:33314
rscl = rscl.connect(port, host, pkgs = "data.table")

# stop if logR not available on nodes, should be because client already passed
stopifnot(rscl.require(rscl, "logR"))

# logR connect to postgres database, use db connection from the client machine
skipifnot(logR_connect())

# stop if no database available from R nodes, it should be available as is for client
stopifnot(rscl.eval(rscl, logR_connect(quoted = TRUE), lazy = FALSE))

# create db structure ----

logR_schema(drop = TRUE)

# actual tests ----

# default non logging scenario produces 0 logs
options("bigdatatable.log" = FALSE)
bdt = as.big.data.table(quote(as.data.table(iris)), rscl)
stopifnot(nrow(logR_dump())==0L)
bdt[, lapply(.SD, mean), Species]
stopifnot(nrow(logR_dump())==0L)

# logR logging produces log for client session and for each of the nodes
options("bigdatatable.log" = TRUE)
# as.big.data.table
bdt = as.big.data.table(quote(as.data.table(iris)), rscl)
r = logR_dump()
stopifnot(
    nrow(r)==5L,
    r$status=="success",
    identical(r$out_rows, c(NA, rep(150L, 4))),
    identical(r$in_rows, rep(NA_integer_, 5)),
    substr(r[1L, expr], 1, 36)=="bdt.eval(x, x <- as.data.table(iris)",
    r[2L, expr]=="x <- as.data.table(iris)",
    r[2:5, uniqueN(expr)==1L]
)
# big.data.table query
bdt[, lapply(.SD, mean), Species]
r = logR_dump()
stopifnot(
    nrow(r)==10L,
    r$status=="success",
    identical(tail(r$out_rows, 5L), c(12L, rep(3L, 4))),
    identical(tail(r$in_rows, 5L), c(600L, rep(150L, 4))),
    substr(r[6L, expr], 1, 56)=="bdt.eval(x, x[, lapply(.SD, mean), Species], lazy = TRUE",
    r[7L, expr]=="x[, lapply(.SD, mean), Species]",
    r[7:10, uniqueN(expr)==1L]
)
# query with sleep
bdt[, {Sys.sleep(0.5); .(.N)}]
r = tail(logR_dump(), 5L)
stopifnot(
    r$timing > 0.5,
    which.max(r$timing)==1L,
    r[1, substr(expr, 1, 17)]=="bdt.eval(x, x[, {",
    r[2L, expr]=="x[, {\n    Sys.sleep(0.5)\n    .(.N)\n}]",
    r[2:5, uniqueN(expr)==1L],
    identical(r$out_rows, c(4L, rep(1L, 4))),
    identical(r$in_rows, c(600L, rep(150L, 4)))
)
# quoted as expression
jj = quote(.(Sepal.Length = sum(Sepal.Length)))
bdt[, jj]
r = tail(logR_dump(), 5L)
stopifnot(
    r[1, substr(expr, 1, 19)] == "bdt.eval(x, x[, jj]",
    r[2:5, expr] == "x[, jj]",
    r[2:5, cond_message] == "object 'jj' not found",
    r[2:5, status] == "error",
    r[1, status] == "success",
    identical(r$out_rows, rep(NA_integer_, 5)),
    identical(r$in_rows, c(600L, rep(150L, 4)))
)
# substitute quoted
eval(substitute(bdt[, jj], list(jj = jj)))
r = tail(logR_dump(), 5L)
stopifnot(
    r[1, substr(expr, 1, 19)] == "bdt.eval(x, x[, .(S",
    r[2:5, expr] == "x[, .(Sepal.Length = sum(Sepal.Length))]",
    r[2:5, status] == "success",
    r[1, status] == "success",
    identical(r$out_rows, c(4L, rep(1L, 4))),
    identical(r$in_rows, c(600L, rep(150L, 4)))
)
# query with error
jj = quote({
    f = function(x) stop("error")
    .(Sepal.Length = f(Sepal.Length))
})
eval(substitute(bdt[, jj], list(jj = jj)))
r = tail(logR_dump(), 5L)
stopifnot(
    r[1, substr(expr, 1, 17)] == "bdt.eval(x, x[, {",
    r[2:5, substr(expr, 1, 5)] == "x[, {",
    r[2:5, status] == "error",
    r[1, status] == "success",
    identical(r$out_rows, rep(NA_integer_, 5)),
    identical(r$in_rows, c(600L, rep(150L, 4)))
)

# print logs to Rout
r = logR_dump()
print(r)

# closing workspace ----

# database disconnect
rscl.eval(rscl, logR_disconnect(quoted = TRUE), lazy = FALSE)
logR_disconnect()

# R disconnect
rscl.close(rscl)
