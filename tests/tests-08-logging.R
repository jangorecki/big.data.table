
# skip if R client or R nodes doesn't have logR or cannot reach postgres database ----

skipifnot = function(x) if(!isTRUE(all(x))) q(save = "no", status = 0)

# skip tests if logR not available on client
skipifnot(requireNamespace("logR", quietly = TRUE))

library(RSclient)
library(data.table)
library(big.data.table)
library(logR)

# connect R nodes
port = 33311:33314
rscl = rscl.connect(port, pkgs = "data.table")

# skip tests if logR not available on R nodes
skipifnot(rscl.require(rscl, "logR"))

## docker postgres
# docker run --rm -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=postgres --name pg postgres:9.5

# logR connect to postgres database, use db connection from the client machine
q.dbConnect = substitute({
    r = tryCatch2(dbConnect(PostgreSQL(), host = .host, port = .port, dbname = .dbname, user = .user, password = .password))
    if(is.null(r$error) && is.null(r$warning) && typeof(r$value)=="S4" && inherits(r$value, "PostgreSQLConnection")){
        options("logR.conn" = r$value)
        r = TRUE
    } else r = FALSE
    r
}, env = list(
    .host = Sys.getenv("POSTGRES_HOST", "127.0.0.1"), 
    .port = Sys.getenv("POSTGRES_PORT", "5432"), 
    .dbname = Sys.getenv("POSTGRES_DB", "postgres"), 
    .user = Sys.getenv("POSTGRES_USER", "postgres"), 
    .password = Sys.getenv("POSTGRES_PASSWORD", "postgres")
))

conn = dbConnect(PostgreSQL(), host = Sys.getenv("POSTGRES_HOST", "127.0.0.1"), port = Sys.getenv("POSTGRES_PORT", "5432"), dbname = Sys.getenv("POSTGRES_DB", "postgres"), user = Sys.getenv("POSTGRES_USER", "postgres"), password = Sys.getenv("POSTGRES_PASSWORD", "postgres"))

# skip if not database available from client
skipifnot(eval(q.dbConnect))

# skip if not database available from R nodes
skipifnot(rscl.eval(rscl, q.dbConnect, lazy = FALSE))

# create db structure ----

logR_schema(drop = TRUE)

# actual tests ----

logR_dump = function(.conn = getOption("logR.conn"), .table = getOption("logR.table","logr"), .schema = getOption("logR.schema","public")){
    sql = sprintf("SELECT * FROM %s.%s;", .schema, .table)
    tryCatch(
        logr <- setDT(dbGetQuery(.conn, sql)),
        error = function(e) stop(sprintf("Query to logR table fails. See below sql and error for details.\n%s\n%s",sql, as.character(e)), call. = FALSE)
    )
    if(ncol(logr)) logr[order(logr_id)] else logr
}

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

# closing workspace ----

# database disconnect
q.dbDisconnect = quote(dbDisconnect(getOption("logR.conn")))
rscl.eval(rscl, q.dbDisconnect, lazy = FALSE)
eval(q.dbDisconnect)

# R disconnect
rscl.close(rscl)
