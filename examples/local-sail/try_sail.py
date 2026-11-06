# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""Real local-engine experiment using the same Rust client for Sail and Spark.

Run with python examples/local-sail/try_sail.py --help.
Results are exploratory: fresh workers, repeated warm actions, full validation.
"""

import argparse
import json
import os
import shutil
import signal
import socket
import statistics
import subprocess
import sys
import time
from pathlib import Path


def rss_mib():
    import psutil

    process = psutil.Process()
    total = process.memory_info().rss
    for child in process.children(recursive=True):
        try:
            total += child.memory_info().rss
        except psutil.NoSuchProcess:
            pass
    return total / 1024**2


def worker(args):
    worker_started = started = time.perf_counter()
    if args.sail_site_packages:
        sys.path.append(str(Path(args.sail_site_packages).resolve()))
    import importlib.metadata as metadata
    from decimal import Decimal

    import pyspark
    import pyspark._pyspark as native_client
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    result = {
        "engine": args.engine,
        "client": metadata.version("pyspark-client-rust"),
        "client_path": pyspark.__file__,
        "native_client_path": native_client.__file__,
        "python": sys.version,
        "client_import_ms": (time.perf_counter() - started) * 1000,
        "rss_before_engine_mib": rss_mib(),
        "cases": {},
        "failures": [],
    }
    spark = local = process = log = None
    try:
        if args.engine == "sail":
            started = time.perf_counter()
            import pysail._native as native_sail

            # Import the Sail extension before timing server creation separately.
            from pysail.spark import SparkConnectServer  # noqa: F401
            from sail_local import LocalSailSession

            result["engine_import_ms"] = (time.perf_counter() - started) * 1000
            result["engine_version"] = metadata.version("pysail")
            result["native_sail_mib"] = Path(native_sail.__file__).stat().st_size / 1024**2
            local = LocalSailSession(**{"spark.sql.session.timeZone": "UTC"})
            spark = local.__enter__()
            result["server_start_ms"] = local.server_start_ms
            result["session_create_ms"] = local.session_create_ms
        else:
            spark_home = Path(args.spark_home).resolve()
            result["engine_version"] = (spark_home / "RELEASE").read_text().strip()
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            env = os.environ.copy()
            env.update(
                SPARK_HOME=str(spark_home), SPARK_LOCAL_IP="127.0.0.1", SPARK_CONNECT_MODE="0"
            )
            if args.java_home:
                env["JAVA_HOME"] = args.java_home
            connect_jars = list((spark_home / "jars").glob("spark-connect_*.jar"))
            if len(connect_jars) != 1:
                raise RuntimeError("Expected exactly one bundled spark-connect server jar")
            log_config = Path(args.worker_output).with_suffix(".log4j2.properties")
            log_config.write_text(
                "status = warn\nname = LocalEngineExperiment\n"
                "appender.console.type = Console\nappender.console.name = stderr\n"
                "appender.console.target = SYSTEM_ERR\n"
                "appender.console.layout.type = PatternLayout\n"
                "appender.console.layout.pattern = %d{HH:mm:ss} %-5p %c{1}: %m%n\n"
                "rootLogger.level = warn\nrootLogger.appenderRef.stderr.ref = stderr\n"
            )
            log = Path(args.worker_output).with_suffix(".spark.log").open("w")
            started = time.perf_counter()
            process = subprocess.Popen(
                [
                    str(spark_home / "bin/spark-submit"),
                    "--class",
                    "org.apache.spark.sql.connect.service.SparkConnectServer",
                    "--jars",
                    str(connect_jars[0]),
                    "--master",
                    "local[2]",
                    "--conf",
                    "spark.ui.enabled=false",
                    "--conf",
                    f"spark.driver.extraJavaOptions=-Dlog4j.configurationFile={log_config.as_uri()}",
                    "--conf",
                    "spark.connect.grpc.binding.address=127.0.0.1",
                    "--conf",
                    f"spark.connect.grpc.binding.port={port}",
                    "--conf",
                    "spark.sql.shuffle.partitions=2",
                ],
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
                start_new_session=True,
            )
            while True:
                if process.poll() is not None:
                    raise RuntimeError(f"Spark server exited; see {log.name}")
                try:
                    with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                        break
                except OSError:
                    if time.perf_counter() - started > 60:
                        raise TimeoutError(
                            "Spark server did not listen within 60 seconds"
                        ) from None
                    time.sleep(0.1)
            result["server_start_ms"] = (time.perf_counter() - started) * 1000
            started = time.perf_counter()
            spark = (
                SparkSession.builder.remote(f"sc://127.0.0.1:{port}")
                .config("spark.sql.session.timeZone", "UTC")
                .getOrCreate()
            )
            result["session_create_ms"] = (time.perf_counter() - started) * 1000

        started = time.perf_counter()
        first = spark.sql("SELECT 1 + 1 AS answer").collect()
        result["first_query_ms"] = (time.perf_counter() - started) * 1000
        if [tuple(row) for row in first] != [(2,)]:
            raise ValueError(f"Unexpected first-query result: {first!r}")
        result["cold_to_first_result_ms"] = (time.perf_counter() - worker_started) * 1000
        result["rss_after_first_query_mib"] = rss_mib()

        # Prepared local input and fixture creation are outside warm action timers.
        data = [(i, i * 2) for i in range(1000)]
        parquet = str(Path(args.fixture).resolve())
        cases = [
            ("sql_literal", lambda: spark.sql("SELECT 1 + 1 AS answer"), [(2,)], ["int"]),
            (
                "range_sum_10k",
                lambda: spark.range(10000, numPartitions=2).selectExpr("sum(id) AS total"),
                [(49995000,)],
                ["bigint"],
            ),
            (
                "group_sum_10k",
                lambda: spark.range(10000, numPartitions=2)
                .selectExpr("id % 10 AS bucket", "id")
                .groupBy("bucket")
                .agg(F.sum("id").alias("total")),
                [(b, 4995000 + b * 1000) for b in range(10)],
                ["bigint", "bigint"],
            ),
            (
                "join_10k_100",
                lambda: spark.range(10000, numPartitions=2)
                .join(spark.range(100, numPartitions=2), on="id")
                .selectExpr("sum(id) AS total"),
                [(4950,)],
                ["bigint"],
            ),
            (
                "local_dataframe_1k",
                lambda: spark.createDataFrame(data, "id long, value long")
                .filter("id % 3 = 0")
                .selectExpr("sum(value) AS total"),
                [(333666,)],
                ["bigint"],
            ),
            (
                "parquet_sum_10k",
                lambda: spark.read.parquet(parquet)
                .filter("id % 2 = 0")
                .selectExpr("sum(value) AS total"),
                [(49990000,)],
                ["bigint"],
            ),
            (
                "decimal_null",
                lambda: spark.sql(
                    "SELECT CAST('1.235' AS DECIMAL(5,2)), "
                    "CAST(NULL AS INT), coalesce(CAST(NULL AS INT), 7)"
                ),
                [(Decimal("1.24"), None, 7)],
                ["decimal(5,2)", "int", "int"],
            ),
        ]
        for name, build, expected, expected_types in cases:
            durations = []
            try:
                for index in range(args.warmups + args.iterations):
                    started = time.perf_counter()
                    frame = build()
                    rows = frame.collect()
                    elapsed = (time.perf_counter() - started) * 1000
                    actual = [tuple(row) for row in rows]
                    if sorted(actual) != sorted(expected):
                        raise ValueError(f"Unexpected values: {actual!r}; expected {expected!r}")
                    if index == 0:
                        actual_types = [
                            field.dataType.simpleString() for field in frame.schema.fields
                        ]
                        if actual_types != expected_types:
                            raise ValueError(
                                f"Unexpected types: {actual_types!r}; expected {expected_types!r}"
                            )
                    if index >= args.warmups:
                        durations.append(elapsed)
                result["cases"][name] = {
                    "durations_ms": durations,
                    "median_ms": statistics.median(durations),
                    "rows": len(expected),
                    "types": actual_types,
                    "validated": True,
                }
            except Exception as error:
                result["failures"].append({"case": name, "error": repr(error)})
        result["rss_after_cases_mib"] = rss_mib()
    except Exception as error:
        result["failures"].append({"case": "setup", "error": repr(error)})
    finally:
        try:
            if local is not None:
                local.__exit__(None, None, None)
            elif spark is not None:
                spark.stop()
        finally:
            if process is not None:
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                    process.wait(timeout=15)
                except ProcessLookupError:
                    pass
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
            if log is not None:
                log.close()
            Path(args.worker_output).write_text(json.dumps(result, indent=2) + "\n")
    return bool(result["failures"])


def main(args):
    output = Path(args.output).resolve()
    output.mkdir(parents=True, exist_ok=False)
    source = output / "source"
    source.mkdir()
    for name in ("try_sail.py", "sail_local.py"):
        shutil.copy2(Path(__file__).with_name(name), source / name)
    import pyarrow as pa
    import pyarrow.parquet as pq

    fixture = output / "input.parquet"
    pq.write_table(
        pa.table({"id": list(range(10000)), "value": [i * 2 for i in range(10000)]}), fixture
    )
    engines = ["sail", "spark"] if args.spark_home else ["sail"]
    runs = []
    for sample in range(args.samples):
        for engine in engines[:: 1 if sample % 2 == 0 else -1]:
            target = output / f"{engine}-{sample + 1}.json"
            command = [
                sys.executable,
                str(source / "try_sail.py"),
                "--worker",
                "--engine",
                engine,
                "--worker-output",
                str(target),
                "--fixture",
                str(fixture),
                "--iterations",
                str(args.iterations),
                "--warmups",
                str(args.warmups),
            ]
            if args.java_home:
                command += ["--java-home", args.java_home]
            if args.sail_site_packages:
                command += ["--sail-site-packages", args.sail_site_packages]
            if args.spark_home:
                command += ["--spark-home", args.spark_home]
            started = time.perf_counter()
            with target.with_suffix(".worker.log").open("w") as log:
                completed = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                try:
                    completed.wait(timeout=180)
                except BaseException:
                    # Spark starts in a separate process group. Clean up descendants
                    # before killing the worker, which otherwise could orphan its JVM.
                    import psutil

                    try:
                        descendants = psutil.Process(completed.pid).children(recursive=True)
                    except psutil.NoSuchProcess:
                        descendants = []
                    for child in reversed(descendants):
                        try:
                            child.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    completed.terminate()
                    _, alive = psutil.wait_procs(descendants, timeout=3)
                    for child in alive:
                        try:
                            child.kill()
                        except psutil.NoSuchProcess:
                            pass
                    try:
                        completed.wait(timeout=3)
                    except subprocess.TimeoutExpired:
                        completed.kill()
                        completed.wait()
                    raise
            if not target.exists():
                raise RuntimeError(
                    f"Worker exited {completed.returncode}; see {target.with_suffix('.worker.log')}"
                )
            run = json.loads(target.read_text())
            run["exit_code"] = completed.returncode
            if completed.returncode and not run["failures"]:
                run["failures"].append(
                    {
                        "case": "worker_exit",
                        "error": f"Exit code {completed.returncode}; see worker log",
                    }
                )
            run["process_total_ms"] = (time.perf_counter() - started) * 1000
            run["sample"] = sample + 1
            runs.append(run)
            print(
                f"{engine} sample {sample + 1}: {len(run['cases'])} validated cases, "
                f"{len(run['failures'])} failures",
                flush=True,
            )
    summary = {
        "runs": runs,
        "samples": args.samples,
        "iterations": args.iterations,
        "warmups": args.warmups,
        "platform": sys.platform,
        "cpu_count": os.cpu_count(),
        "timing": "Warm actions include plan construction, RPC, engine execution, collect; validation follows timing. "
        "All iterations check values; first warmup also checks Spark data types. "
        "cold_to_first_result_ms runs from worker entry through first validated result, excluding interpreter startup.",
        "limits": "Exploratory microbenchmarks, warm OS caches; Spark local[2]/2 shuffle partitions/WARN logging, Sail defaults. "
        "RSS is sampled client plus child processes, not peak memory. Startup boundaries differ: "
        "Sail start() return vs Spark TCP listener. Neither implies warmed execution.",
    }
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    lines = [
        "# Local Sail experiment",
        "",
        f"{args.samples} fresh processes per engine; {args.iterations} timed actions "
        f"after {args.warmups} warmups per case. Same installed Rust client for both engines.",
        "",
        "| Case | Sail median ms | Spark median ms | Spark / Sail |",
        "|---|---:|---:|---:|",
    ]
    names = dict.fromkeys(name for run in runs for name in run["cases"])
    for name in names:
        medians = {}
        for engine in engines:
            selected = [
                run["cases"][name]["median_ms"]
                for run in runs
                if run["engine"] == engine and name in run["cases"]
            ]
            if len(selected) == args.samples:
                medians[engine] = statistics.median(selected)
        sail, spark = medians.get("sail"), medians.get("spark")
        lines.append(f"| {name} | {sail:.3f}" if sail is not None else f"| {name} | unavailable")
        lines[-1] += (
            f" | {spark:.3f} | {spark / sail:.1f}x |"
            if spark is not None and sail
            else " | unavailable | — |"
        )
    lines += ["", "| Metric | Sail | Spark |", "|---|---:|---:|"]
    for metric in (
        "client_import_ms",
        "engine_import_ms",
        "server_start_ms",
        "session_create_ms",
        "first_query_ms",
        "cold_to_first_result_ms",
        "rss_before_engine_mib",
        "rss_after_first_query_mib",
        "rss_after_cases_mib",
        "native_sail_mib",
    ):
        values = []
        for engine in ("sail", "spark"):
            selected = [run[metric] for run in runs if run["engine"] == engine and metric in run]
            values.append(f"{statistics.median(selected):.3f}" if selected else "—")
        lines.append(f"| {metric} | {' | '.join(values)} |")
    lines += ["", summary["timing"], "", summary["limits"], "", "Failures:"]
    failures = [
        {"engine": run["engine"], "sample": run["sample"], **failure}
        for run in runs
        for failure in run["failures"]
    ]
    lines += [f"- {failure}" for failure in failures] or ["- None."]
    (output / "REPORT.md").write_text("\n".join(lines) + "\n")
    print("\n".join(lines), flush=True)
    return bool(failures)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(Path(__file__).with_name("runs") / "sail-local"))
    parser.add_argument("--samples", type=int, default=3)
    parser.add_argument("--iterations", type=int, default=10)
    parser.add_argument("--warmups", type=int, default=2)
    parser.add_argument(
        "--sail-site-packages",
        help="Reuse an existing Sail installation without changing either environment",
    )
    parser.add_argument(
        "--spark-home", help="Optional complete Spark Connect distribution for an engine comparison"
    )
    parser.add_argument(
        "--java-home",
        default=os.environ.get("JAVA_HOME"),
        help="JDK for optional Spark comparison; otherwise use JAVA_HOME or java on PATH",
    )
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument(
        "--engine", choices=("sail", "spark"), default="sail", help=argparse.SUPPRESS
    )
    parser.add_argument("--worker-output", help=argparse.SUPPRESS)
    parser.add_argument("--fixture", help=argparse.SUPPRESS)
    options = parser.parse_args()
    if options.samples < 1 or options.iterations < 1 or options.warmups < 0:
        parser.error("samples and iterations must be positive; warmups must be nonnegative")
    raise SystemExit(worker(options) if options.worker else main(options))
