Reproducing test results

Start a cockroachdb instance (22.1 alpha / recent)

    ./cockroach start-single-node --insecure

Open a SQL shell to run the following setup:

    ./cockroach sql --insecure
    
    SET CLUSTER SETTING changefeed.experimental_poll_interval = '0.2s';

    CREATE TABLE public.foo (
        a INT8 NOT NULL,
        value STRING NULL,
        i INT8 NULL,
        CONSTRAINT foo_pkey PRIMARY KEY (a ASC),
        FAMILY "primary" (a, value, i)
    )

To start the run, from this directory launch

    go run . -q -qps=-1 -for=1m -writers=96 -extra-watchers=0

which runs as fast as possible with 96 writers, 96 readers, and no extra readers for one minute.
