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

"""Explicit local Sail sessions; install pysail beside pyspark-client-rust.

    with LocalSailSession() as spark:
        print(spark.sql("SELECT 1 + 1 AS answer").collect())

The server starts on entry, stays alive for the context, and stops on exit.
"""

from time import perf_counter


class LocalSailSession:
    def __init__(self, **configs):
        self.configs = configs
        self.server = None
        self.spark = None
        self.server_start_ms = None
        self.session_create_ms = None

    def __enter__(self):
        from pysail.spark import SparkConnectServer
        from pyspark.sql import SparkSession

        if self.server is not None:
            raise RuntimeError("LocalSailSession cannot be entered twice")
        started = perf_counter()
        self.server = SparkConnectServer(ip="127.0.0.1", port=0)
        try:
            self.server.start()
            self.server_start_ms = (perf_counter() - started) * 1000
            _, port = self.server.listening_address
            started = perf_counter()
            builder = SparkSession.builder.remote(f"sc://127.0.0.1:{port}")
            for key, value in self.configs.items():
                builder = builder.config(key, value)
            self.spark = builder.getOrCreate()
            self.session_create_ms = (perf_counter() - started) * 1000
            return self.spark
        except BaseException:
            if self.server.running:
                self.server.stop()
            raise

    def __exit__(self, *_):
        try:
            if self.spark is not None:
                self.spark.stop()
        finally:
            if self.server is not None and self.server.running:
                self.server.stop()


if __name__ == "__main__":
    with LocalSailSession() as spark:
        print(spark.sql("SELECT 1 + 1 AS answer").collect())
        print(spark.range(10).selectExpr("sum(id) AS total").collect())
