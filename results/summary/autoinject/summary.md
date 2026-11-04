# Phase 1 results summary
10 runs, 599576 measured operations. Every `_ci95` is the 95% t-interval half-width over seeds; values are averaged within a seed first.

## Data-quality warnings
- autoinject-auto-r1000-s1: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r1000-s2: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r1000-s3: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r1000-s4: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r1000-s5: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r200-s1: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r200-s2: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r200-s3: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r200-s4: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.
- autoinject-auto-r200-s5: no usable clock offset from client1 (unavailable), client2 (unavailable), client3 (unavailable), cloud1 (unavailable), cloud2 (unavailable), cloud3 (unavailable): their timestamps are not corrected, so cross-machine timelines (failover, detection, elections) carry their clock error. Harmless if every process ran on one machine; on the lab machines check chrony (or timesyncd) and that chronyc is on the PATH of a non-interactive ssh shell.

## Latency and throughput
Throughput and latency are over requests with an `ok` reply. Requests that never got a final reply (`unresolved_frac`) are EXCLUDED from both, so check that column: above 0.1% the run was past saturation and these numbers flatter it.

| sweep | series | label | x | seeds | x_name | mode | concurrent_users | throughput | throughput_ci95 | mean_ms | mean_ms_ci95 | p50_ms | p50_ms_ci95 | p95_ms | p95_ms_ci95 | p99_ms | p99_ms_ci95 | retried_frac | retried_frac_ci95 | gave_up_frac | gave_up_frac_ci95 | unresolved_frac | unresolved_frac_ci95 | service_mean_us | service_mean_us_ci95 | service_p50_us | service_p50_us_ci95 | service_p99_us | service_p99_us_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto inject | auto-r1000 | 1000 | 5 | offered load with automatic injection (ops/s) | open | 600 | 999 | 3.13 | 14.4 | 1.46 | 12.5 | 0.0724 | 28.2 | 1.43 | 54.5 | 30.1 | 0.00228 | 0.00487 | 0 | 0 | 0 | 0 | 101 | 4.59 | 82.4 | 3.47 | 384 | 31.8 |
| autoinject | auto inject | auto-r200 | 200 | 5 | offered load with automatic injection (ops/s) | open | 600 | 200 | 2.43 | 14.9 | 1.1 | 14.1 | 0.0349 | 23.4 | 0.535 | 35.5 | 10.2 | 0.00219 | 0.00426 | 0 | 0 | 0 | 0 | 136 | 8.72 | 111 | 12.2 | 481 | 28.2 |

## Injected failures (only silences inside the measured window; failover definition in analyze.failures; a censored failover counts as its horizon, a lower bound)
| sweep | label | victim_role | seeds | failover_s | failover_s_ci95 | detect_s | detect_s_ci95 | view_removal_s | view_removal_s_ci95 | recovery_s | recovery_s_ci95 | sync_s | sync_s_ci95 | catch_up_entries | catch_up_entries_ci95 | peak_p99_ms | peak_p99_ms_ci95 | retries_during | retries_during_ci95 | injections | censored | no_baseline | worked_before_active |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | follower | 4 | 0 | 0 | 0.246 | 0.023 | 0.25 | 0.0242 | 0.212 | 0.037 | 0.212 | 0.037 | 5.56e+03 | 1.28e+03 | 63.8 | 18.3 | 3.92 | 2.39 | 11 | 0 | 0 | 0 |
| autoinject | auto-r1000 | coordinator | 2 | 0.583 | 1.06 | 0.238 | 0.116 | 0.34 | 0.371 | 0.199 | 0.0027 | 0.199 | 0.00169 | 5.46e+03 | 7.72e+03 | 395 | 686 | 242 | 807 | 4 | 0 | 0 | 0 |
| autoinject | auto-r200 | follower | 4 | 0.146 | 0.167 | 0.249 | 0.0237 | 0.254 | 0.0236 | 0.201 | 0.000636 | 0.201 | 0.00064 | 1.11e+03 | 244 | 61.3 | 15.4 | 1.17 | 0.919 | 11 | 0 | 0 | 0 |
| autoinject | auto-r200 | coordinator | 2 | 0.917 | 5.29 | 0.239 | 0.0998 | 0.332 | 0.357 | 0.211 | 0.138 | 0.211 | 0.138 | 1.09e+03 | 1.42e+03 | 335 | 387 | 49.8 | 48.7 | 4 | 0 | 0 | 0 |

## Before / during / after each silence
| sweep | label | phase | seeds | throughput | throughput_ci95 | mean_ms | mean_ms_ci95 | p50_ms | p50_ms_ci95 | p99_ms | p99_ms_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | before | 5 | 1e+03 | 2.35 | 13.7 | 0.229 | 12.5 | 0.104 | 42.4 | 1.47 |
| autoinject | auto-r1000 | during | 5 | 1e+03 | 8.67 | 17.3 | 9.06 | 12.2 | 0.246 | 125 | 178 |
| autoinject | auto-r1000 | after | 5 | 1e+03 | 11 | 14.2 | 0.313 | 12.8 | 0.156 | 43.7 | 1.72 |
| autoinject | auto-r200 | before | 5 | 200 | 3.24 | 14.5 | 0.0568 | 14.3 | 0.0761 | 30.4 | 0.755 |
| autoinject | auto-r200 | during | 5 | 199 | 2.01 | 17 | 6.9 | 12.4 | 0.464 | 104 | 159 |
| autoinject | auto-r200 | after | 5 | 201 | 3.86 | 14.6 | 0.0483 | 14.3 | 0.0583 | 32 | 0.598 |

## Coordinator elections after start-up
| sweep | label | seeds | duration_s | duration_s_ci95 | from_suspect_s | from_suspect_s_ci95 | from_inject_s | from_inject_s_ci95 |
|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 2 | 0.0205 | 0.0113 | 0.0987 | 0.251 | 0.337 | 0.367 |
| autoinject | auto-r200 | 2 | 0.0128 | 0.0157 | 0.0898 | 0.258 | 0.329 | 0.358 |

## False suspicions and wrongful removals (pooled: total count / total node-hours)
| sweep | label | node_hours | suspects | false_suspects | view_removals | wrongful_removals | false_suspects_per_node_hour | wrongful_removals_per_node_hour |
|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 0.458 | 30 | 0 | 15 | 0 | 0 | 0 |
| autoinject | auto-r200 | 0.458 | 30 | 0 | 15 | 0 | 0 | 0 |

## Load distribution (jain_busy_per_core: fairness against capacity)
| sweep | label | seeds | jain_busy | jain_busy_ci95 | jain_busy_per_core | jain_busy_per_core_ci95 | jain_count | jain_count_ci95 | busy_share_n1 | busy_share_n1_ci95 | busy_share_n2 | busy_share_n2_ci95 | busy_share_n3 | busy_share_n3_ci95 | capacity_share_n1 | capacity_share_n1_ci95 | capacity_share_n2 | capacity_share_n2_ci95 | capacity_share_n3 | capacity_share_n3_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 5 | 0.996 | 0.00376 | 0.996 | 0.00376 | 0.996 | 0.00318 | 0.323 | 0.0355 | 0.345 | 0.0233 | 0.332 | 0.0209 | 0.333 | 0 | 0.333 | 0 | 0.333 | 0 |
| autoinject | auto-r200 | 5 | 0.998 | 0.000856 | 0.998 | 0.000856 | 0.997 | 0.00184 | 0.328 | 0.0281 | 0.34 | 0.0108 | 0.332 | 0.0205 | 0.333 | 0 | 0.333 | 0 | 0.333 | 0 |

## Planner service time (plans finished in the measured window)
| sweep | label | seeds | plans | plans_ci95 | service_mean_us | service_mean_us_ci95 | service_p50_us | service_p50_us_ci95 | service_p99_us | service_p99_us_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 5 | 1.3e+05 | 1.22e+03 | 101 | 4.59 | 82.4 | 3.47 | 384 | 31.8 |
| autoinject | auto-r200 | 5 | 2.05e+04 | 404 | 136 | 8.72 | 111 | 12.2 | 481 | 28.2 |

## Heartbeat and protocol overhead per node (rates over the whole run)
| sweep | label | node | seeds | heartbeats_per_s | heartbeats_per_s_ci95 | heartbeat_bytes_per_s | heartbeat_bytes_per_s_ci95 | net_tx_bytes_per_s | net_tx_bytes_per_s_ci95 | net_rx_bytes_per_s | net_rx_bytes_per_s_ci95 | peer_msgs_per_s | peer_msgs_per_s_ci95 | retransmits_per_s | retransmits_per_s_ci95 | bare_acks_per_s | bare_acks_per_s_ci95 | msgs_tx_election_per_s | msgs_tx_election_per_s_ci95 | msgs_tx_replication_per_s | msgs_tx_replication_per_s_ci95 | msgs_tx_claim_per_s | msgs_tx_claim_per_s_ci95 | msgs_tx_commit_per_s | msgs_tx_commit_per_s_ci95 | msgs_tx_other_per_s | msgs_tx_other_per_s_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 1 | 5 | 18.8 | 1.08 | 629 | 39.1 | 6.22e+05 | 1.6e+05 | 2.57e+05 | 3.39e+04 | 2.12e+03 | 652 | 61.2 | 37.2 | 6.07 | 2.92 | 0.00635 | 0.0176 | 1.02e+03 | 417 | 597 | 48.6 | 508 | 206 | 0.0127 | 0.0179 |
| autoinject | auto-r1000 | 2 | 5 | 19.6 | 0.563 | 670 | 20 | 6.77e+05 | 1.66e+05 | 2.48e+05 | 1.71e+04 | 2.3e+03 | 700 | 87.4 | 32.8 | 3.68 | 1.4 | 0.0157 | 0.0339 | 1.12e+03 | 460 | 620 | 24.3 | 561 | 218 | 0.0063 | 0.00818 |
| autoinject | auto-r1000 | 3 | 5 | 19 | 0.954 | 651 | 34.1 | 6.13e+05 | 1.29e+05 | 2.46e+05 | 1.71e+04 | 1.96e+03 | 468 | 63.8 | 39.7 | 6.18 | 2.11 | 0.0378 | 0.0491 | 926 | 286 | 573 | 45.4 | 463 | 156 | 0.0142 | 0.0127 |
| autoinject | auto-r200 | 1 | 5 | 18.8 | 1.07 | 617 | 33 | 5.96e+04 | 2.83e+04 | 6.33e+04 | 3.04e+04 | 426 | 131 | 20.7 | 13.8 | 7.17 | 3.25 | 0.00635 | 0.0176 | 236 | 95.4 | 112 | 10 | 78.1 | 31.7 | 0.0127 | 0.0112 |
| autoinject | auto-r200 | 2 | 5 | 19.5 | 0.571 | 641 | 35.1 | 6.99e+04 | 3.74e+04 | 5.25e+04 | 2.12e+04 | 465 | 146 | 30.9 | 12.6 | 5.28 | 2.72 | 0.0158 | 0.0339 | 260 | 109 | 118 | 4.13 | 87.1 | 34.3 | 0.00632 | 0.00819 |
| autoinject | auto-r200 | 3 | 5 | 19 | 0.954 | 629 | 33 | 7.8e+04 | 5.91e+04 | 4.73e+04 | 2.9e+03 | 399 | 93 | 22.4 | 13.8 | 6.96 | 2.68 | 0.0284 | 0.0525 | 215 | 65.9 | 112 | 7.87 | 71.6 | 22.9 | 0.00949 | 0.00825 |

## Counters
| sweep | label | seeds | elections_per_min | elections_per_min_ci95 | election_mean_s | election_mean_s_ci95 | election_max_s | election_max_s_ci95 | elections_after_startup | elections_after_startup_ci95 | view_changes | view_changes_ci95 | election_msgs_per_election | election_msgs_per_election_ci95 | election_msgs_per_s | election_msgs_per_s_ci95 | claim_msgs_per_req | claim_msgs_per_req_ci95 | commit_msgs_per_req | commit_msgs_per_req_ci95 | competing_claim_rate | competing_claim_rate_ci95 | replan_rate | replan_rate_ci95 | injections | injections_ci95 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 5 | 0.48 | 0.971 | 0.0205 | 0.0113 | 0.0265 | 0.0875 | 0.8 | 1.62 | 5.8 | 0.555 | 2.6 | 3.24 | 0.0597 | 0.0959 | 2.06 | 0.00866 | 1.76 | 0.038 | 0.0833 | 0.0048 | 0.285 | 0.0153 | 3 | 0 |
| autoinject | auto-r200 | 5 | 0.48 | 0.972 | 0.0128 | 0.0157 | 0.0156 | 0.0518 | 0.8 | 1.62 | 5.8 | 0.555 | 2 | 3.51 | 0.0503 | 0.102 | 1.96 | 0.00705 | 1.36 | 0.0293 | 0.0314 | 0.000973 | 0.0474 | 0.0111 | 3 | 0 |

## Health (worst run: events dropped, core-loop lag)
| sweep | label | core_lag_max_ms_n1 | core_lag_p99_ms_n1 | core_lag_max_ms_n2 | core_lag_p99_ms_n2 | core_lag_max_ms_n3 | core_lag_p99_ms_n3 | node_events_dropped | client_events_dropped | client_dropped_arrivals | unresolved | unresolved_frac | busy_replies | overload_dropped_n1 | client_backlog_p50_n1 | overload_dropped_n2 | client_backlog_p50_n2 | overload_dropped_n3 | client_backlog_p50_n3 | client_cpu_pct_client1 | client_cpu_pct_client2 | client_cpu_pct_client3 | client_cpu_pct_max | client_send_dropped |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| autoinject | auto-r1000 | 34 | 12 | 29 | 14.7 | 39 | 17.7 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 5.92 | 5.92 | 5.92 | 5.92 | 0 |
| autoinject | auto-r200 | 15 | 12.7 | 15 | 10.5 | 15 | 13.5 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 0 | 2.45 | 2.45 | 2.45 | 2.45 | 0 |
