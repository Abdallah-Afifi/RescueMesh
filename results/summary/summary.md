# Phase 1 results summary
1 runs, 20937 measured operations.

## Latency and throughput (mean over seeds, 95% CI half-width)
| series | label | x | seeds | throughput | throughput_ci95 | mean_ms | mean_ms_ci95 | p50_ms | p50_ms_ci95 | p95_ms | p95_ms_ci95 | p99_ms | p99_ms_ci95 | retried_frac | retried_frac_ci95 | gave_up_frac | gave_up_frac_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| local | local | nan | 1 | 523 | nan | 684 | nan | 986 | nan | 1.57e+03 | nan | 1.91e+03 | nan | 0.599 | nan | 0.121 | nan |

## Injected failures (mean over injections)
| label | failover_s | view_removal_s | recovery_s | peak_p99_ms | retries_during |
|---|---|---|---|---|---|
| local | 0 | 0.508 | 0.311 | 3.17e+03 | 533 |

## Load distribution
| label | seed | jain_busy | jain_count | busy_share_n1 | busy_share_n2 | busy_share_n3 |
|---|---|---|---|---|---|---|
| local | 0 | 0.997 | 0.998 | 0.357 | 0.317 | 0.326 |

## Counters
| label | seed | election_start | election_won | step_down | suspect | view_change | replan | duplicate_suppressed | busy | commit | inject_start | replan_rate |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| local | 0 | 0 | 0 | 0 | 0 | 5 | 0 | 0 | 0 | 2.94e+04 | 5 | 0 |
