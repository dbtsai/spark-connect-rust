# Local Sail experiment

3 fresh processes per engine; 10 timed actions after 2 warmups per case. Same installed Rust client for both engines.

| Case | Sail median ms | Spark median ms | Spark / Sail |
|---|---:|---:|---:|
| sql_literal | 0.313 | 16.944 | 54.1x |
| range_sum_10k | 0.506 | 36.787 | 72.7x |
| group_sum_10k | 1.003 | 37.492 | 37.4x |
| join_10k_100 | 0.746 | 40.306 | 54.0x |
| local_dataframe_1k | 1.758 | 27.106 | 15.4x |
| parquet_sum_10k | 1.172 | 60.846 | 51.9x |
| decimal_null | 0.421 | 9.071 | 21.6x |

| Metric | Sail | Spark |
|---|---:|---:|
| client_import_ms | 48.921 | 38.584 |
| engine_import_ms | 6.066 | — |
| server_start_ms | 2.041 | 2721.509 |
| session_create_ms | 5.427 | 180.311 |
| first_query_ms | 1.556 | 1506.580 |
| cold_to_first_result_ms | 239.713 | 4572.908 |
| rss_before_engine_mib | 30.484 | 30.578 |
| rss_after_first_query_mib | 67.859 | 538.922 |
| rss_after_cases_mib | 171.969 | 999.891 |
| native_sail_mib | 129.179 | — |

Warm actions include plan construction, RPC, engine execution, collect; validation follows timing. All iterations check values; first warmup also checks Spark data types. cold_to_first_result_ms runs from worker entry through first validated result, excluding interpreter startup.

Exploratory microbenchmarks, warm OS caches; Spark local[2]/2 shuffle partitions/WARN logging, Sail defaults. RSS is sampled client plus child processes, not peak memory. Startup boundaries differ: Sail start() return vs Spark TCP listener. Neither implies warmed execution.

Failures:
- None.
