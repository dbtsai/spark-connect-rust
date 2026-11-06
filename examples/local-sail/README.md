<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

# Local Sail prototype

Run the Rust-backed PySpark client against a Sail server that starts automatically
in the same Python process. This example loads the existing `pyspark._pyspark` and
`pysail._native` extensions and connects them through localhost Spark Connect
gRPC. Sail remains an optional runtime dependency.

The helper provides explicit local sessions. It leaves static Rust linking and
automatic placement of individual queries for future work.

## Install and run

From the repository root, create a separate Python 3.12 environment. The Rust and
official PySpark clients both provide the `pyspark` namespace, so use one client
per environment.

```sh
python3.12 -m venv examples/local-sail/.venv
examples/local-sail/.venv/bin/python -m pip install -r examples/local-sail/requirements.txt
examples/local-sail/.venv/bin/python examples/local-sail/sail_local.py
```

The demo prints the result of `SELECT 1 + 1` and an aggregation over ten rows.
Local Sail execution needs neither Java nor a separate Spark installation.

The requirements install `pyspark-client-rust==4.2.0` and `pysail==0.7.3`, plus
`psutil` for the benchmark. This example and its recorded evidence use an installed
4.2.0 client wheel. Testing the checkout's 4.3.0 source build is separate work.

## Use the session helper

Start a Python interpreter with the example directory on its import path:

```sh
PYTHONPATH=examples/local-sail examples/local-sail/.venv/bin/python
```

Then create a local session:

```python
from sail_local import LocalSailSession

with LocalSailSession(**{"spark.sql.session.timeZone": "UTC"}) as spark:
    print(spark.sql("SELECT 1 + 1 AS answer").collect())
    print(spark.range(10000).selectExpr("sum(id) AS total").collect())
```

Entering the context starts a server on `127.0.0.1` with an operating-system-assigned
port and creates the client session. All queries inside the context reuse that
server and session. Exiting closes the session and stops the server, including
when the context body raises an exception. Each helper instance can be entered
once. Use its DataFrames before the context closes.

## Run the benchmark

Run only Sail using the environment installed above:

```sh
examples/local-sail/.venv/bin/python examples/local-sail/try_sail.py \
  --samples 3 --iterations 10 --warmups 2 \
  --output examples/local-sail/runs/sail-only
```

For a Spark comparison, obtain the complete Spark 4.2.0 Connect binary distribution
and set `JAVA_HOME` to a compatible JDK; the recorded run used Java 21. The harness
starts and stops its own local Spark server. On a POSIX host:

```sh
examples/local-sail/.venv/bin/python examples/local-sail/try_sail.py \
  --spark-home /path/to/spark-4.2.0-bin-hadoop3-connect \
  --java-home "$JAVA_HOME" \
  --samples 3 --iterations 10 --warmups 2 \
  --output examples/local-sail/runs/comparison
```

Use a new output directory for each run; existing directories are rejected.
Generated runs stay under the example's ignored `runs/` directory. Each run retains
the report, raw timings, logs, local Parquet input, and copies of the scripts.

## Recorded evidence and limits

The [November 5 report](evidence/2026-11-05/REPORT.md) and
[raw summary](evidence/2026-11-05/summary.json) preserve a local comparison using
the installed 4.2.0 Rust client for both engines. The evidence also retains source
snapshots and hashes. It covers seven cases: a SQL literal, range aggregation,
grouped aggregation, a small join, a local DataFrame, a Parquet read, and decimal
and null expressions.

Each engine runs in three fresh workers, with ten timed actions after two warmups
per case. Engine order reverses on alternating samples. Every action checks
returned values, and the first action of each case also checks Spark data types.
These small cases do not establish general Spark semantic parity.

Warm timings include plan construction, RPC, engine execution, and collection;
validation follows the timer. Cold time to the first result includes client and
engine imports, setup, and memory-sampling instrumentation. It excludes Python
interpreter and harness module startup. Server startup endpoints differ: Sail's
`start()` return and Spark's TCP listener becoming available.

These are exploratory measurements with warm operating-system caches. Spark uses
`local[2]`, two shuffle partitions, and WARN logging; Sail uses its defaults. The
engines therefore have different resource configurations. RSS includes the client
and child processes, including Spark's JVM, and records snapshots rather than
peak memory. The native Sail size metric measures its installed extension file,
not its wheel or complete installation. Results apply to these local fixtures;
larger workloads and remote-cluster latency need separate evaluation.
