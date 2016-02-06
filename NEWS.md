# big.data.table 0.3.3 (devel)

* TO DO: processing of `outer.aggregate` push down to `bdt.eval` to include in timing of client task.
* `outer.aggregate` arg can accept function.
* new function `bdt.eval.log` just for handling logR and nicely parsed expressions to logs.
* CI moved to `quay.io/jangorecki/r-data.table` for easier rebuilding deps.
* thanks to above building `logR` is now pushed down to `rscl.eval`, making `expr` log in db more verbose.
* Unit tests for logging to postgres db, auto skip when desired.
* Using `postgres` service in CI.
* Single CI job, generating tech homepage with `drat@artifacts`.

# big.data.table 0.3.2

* `rscl.*` vectorized wrappers to `RSclient::RS.*`.
* Simplified use of `rscl` connection list.
* Multiple data.tables per node when storing with different variable names, `new.var` arg in `[.big.data.table`.
* Remove timing in code, for timing use `options("bigdatatable.log"=TRUE)` to use suggested logR package - requires postgres db.
* Logging on client side pushed down to `bdt.eval`.
* Use new `logR::logR(boolean=TRUE)` instead of own `btd.eval(send=TRUE)`.
* CI base image changed to `jangorecki/r-data.table-pg`.
* Included in CI: create drat repo and basic html website.
* More unit tests.

# big.data.table 0.2

* First stable release.
* CI to build and check unit tests from `jangorecki/r-base-dev` image.
