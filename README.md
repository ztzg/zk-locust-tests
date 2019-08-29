# Load-testing ZooKeeper using Locust

## Description

An experimental "test harness" for ZooKeeper.

## Quick Start

 1. Install [Locust](https://locust.io/).  (Versions 0.9.0 and HEAD
    have been tested, with Python 3.7.4.)

 2. Install [Kazoo](https://kazoo.readthedocs.io/en/latest/).
    (Versions 2.6.1 and `HEAD` have been tested.  `HEAD` is required
    for SASL support.)

 3. Install and configure ZooKeeper (left as an exercise for the
    reader).

 4. `export ZK_LOCUST_HOSTS=<ensemble>`.

 5. Run a simple test using the Web UI:

        $ locust -f locust_set.py
        INFO/locust.main: Starting web monitor at *:8089
        INFO/locust.main: Starting Locust 0.11.1

    In the Web UI, enter e.g. 128 (users), 32 (/second), and activate
    "Start swarming."  Click "STOP" then kill the `locust` command
    when satisfied.

 6. Run a 7-worker instance, "distributed" as processes on a single
    machine:

        $ mkdir -p tmp
        $ ./multi-locust.sh 7 tmp -f locust_set.py
        locust.main: Starting web monitor at *:8089
        locust.main: Starting Locust 0.11.1
        locust.runners: Client 'teek_14bd0d516df3487a8d173d6cd5018fdf' reported as ready. Currently 1 clients ready to swarm.
        […]
        locust.runners: Client 'teek_370e1be12b7a454284dc3b3bee37c709' reported as ready. Currently 7 clients ready to swarm.

    Note how it is now possible to keep a multicore machine busy.

 7. Run a headless (`--no-web`) 7-worker instances on a number of test
    cases, collecting (some) statistics, using the provided Make
    recipe:

        $ make
        […]
        $ ls out/*.csv | wc -l
        18
        $ tail -n 8 out/set_and_get.log
        Percentage of the requests completed within given times
         Name                                                           # reqs    50%    66%    75%    80%    90%    95%    98%    99%   100%
        --------------------------------------------------------------------------------------------------------------------------------------------
         get                                                            207341     28     29     30     30     32     34     40     47     61
         set                                                             20819     28     29     30     31     32     35     42     48     61
        --------------------------------------------------------------------------------------------------------------------------------------------
         Total                                                          228160     28     29     30     30     32     34     40     47     61

 8. Run a many-worker instances, across a fleet of machines.  (Left as
    an exercise for the reader.)

## Locust Utilities

A number of utilities are provided in `common.py`:

  * `KazooLocustClient`: A Locust "client" object which provides
    helper methods as well as direct access to the Kazoo client object
    via `get_zk_client`;

  * `ZKLocustClient`: Similar to `KazooLocustClient`, but its backend
    is thin wrapper around `zkpython`--which allows exercising the
    "official" ZooKeeper client library;

  * `ZKLocust`: A Locust subclass which can host task sets and is
    automatically initialized with an instance of `KazooLocustClient`
    (default) or `ZKLocustClient` as the client;

  * `LocustTimer`: A Python "Context Manager" which makes it easy to
    time requests or segments.

## Environment variables

  * `ZK_LOCUST_HOSTS`: A ZooKeeper "connect string" including the
    addresses of the ensemble;

  * `ZK_LOCUST_CLIENT`: Selects the `ZKLocust` backend, unless
    overriden by a subclass.  Valid values include `kazoo` (default)
    and `zkpython`;

  * `ZK_LOCUST_PSEUDO_ROOT`: A "pseudo root" for tests.  Note that
    this is not a "chroot" in the ZooKeeper sense; it is purely
    advisory;

  * `KAZOO_LOCUST_HANDLER`: Selects the Kazoo concurrency "handler."
    Valid values include `threading` and `gevent`.  The default
    depends on Kazoo, but normally corresponds to `threading`;

  * `KAZOO_LOCUST_SASL_OPTIONS`: An optional JSON-encoded dictionary
    of SASL options for the Kazoo backend.  The default is to not
    authenticate with the server.

## Base "Locustfiles"

The `locust_*.py` files are "locustfiles," and test various aspects of
the target ZooKeeper ensemble.

  * TODO(ddiederen): Refactor into composable bits;

  * TODO(ddiederen): Generate more representative loads.

## Pictures

### Captured data

The CSV files produced by `--no-web` do not contain full histograms,
but rather focus on on tail latencies.  While these do not lend
themselves to smooth curves, they can still be quickly visualized and
compared by plotting:

![](doc/distributions.png)

### Web UI Screenshots

![](doc/locust-stats.png)

![](doc/locust-charts.png)
