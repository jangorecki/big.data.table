## for check on localhost use postgres service
# docker run --rm -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres --name pg postgres:9.5

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
port = 33311:33314
rscl = rscl.connect(port, pkgs = "data.table")

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
    identical(r$out_rows, c(NA, rep(150L, 4)))
)
# big.data.table query
bdt[, lapply(.SD, mean), Species]
r = logR_dump()
stopifnot(
    nrow(r)==10L,
    r$status=="success",
    identical(tail(r$out_rows, 5L), c(NA, rep(3L, 4)))
)
# query with sleep
bdt[, {Sys.sleep(0.5); .(.N)}]
r = tail(logR_dump(), 5L)
stopifnot(
    r$timing > 0.5,
    which.max(r$timing)==1L
)

# preview logs to Rout
print(logR_dump())

# closing workspace ----

# database disconnect
rscl.eval(rscl, logR_disconnect(quoted = TRUE), lazy = FALSE)
logR_disconnect()

# R disconnect
rscl.close(rscl)
