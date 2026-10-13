# Phase 0 survey: corrections

Corrections to `104_survey_v1_submitted.pdf`, the survey as submitted on 2026-10-04. Page numbers refer to that PDF. All of these are applied in `104_survey_v2.docx`, which is the version to reuse in the Phase 1 and final reports.

Section 7 (the chosen design) is not patched line by line here. It is rewritten in version 2 to match `../protocol-v2.md`, and the reasons are in `../design-log.md`.

In version 2 the claims below marked **check** are reworded to what the abstracts support, and the two unconfirmed IEEE Xplore document numbers are replaced by DOIs. The evaluation plan is now Section 7.9 and the peer-to-peer section is 7.8.

Items marked **check** could not be confirmed online and should be verified against the paper itself before the text is finalised.

## A. Factual corrections

| # | Page | As written | Replace with |
|---|---|---|---|
| A1 | 10, 34 | Randomized algorithms "(Kutten et al., 2015)" need fewer than n messages with high probability, cited to *On the complexity of universal leader election*, JACM 62(1). | Cite Kutten, Pandurangan, Peleg, Robinson and Trehan, *Sublinear bounds for randomized leader election* (ICDCN 2013; journal version in Theoretical Computer Science, 2015; arXiv:1210.4822). The JACM paper proves a lower bound for general graphs and does not support this sentence. Keep it only if it is cited for lower bounds. |
| A2 | 7, 18, 21 | Bully "best case n − 1 messages", while the worked row for n = 3 counts 3. | State the convention once: "n messages when counted as sent, including the ELECTION and COORDINATOR messages addressed to the failed node; n − 2 if only COORDINATOR messages to lower-ID nodes are counted, as in Coulouris et al." Then the row of 3 is consistent. |
| A3 | 12 | Viewstamped Replication: "leadership rotates in a fixed order, so when the leader fails, the next node in line takes over without a vote." | "The next primary is fixed by rotation (Liskov and Cowling, 2012), but it takes over only after a quorum of replicas has agreed to the view change." |
| A4 | 20 | Viewstamped Replication message complexity "O(n) per view change". | "O(n²) per view change", because the view-change request is sent by every replica to every other. |
| A5 | 15 | Dynatune: "detection time cut by about 80%". | "about 78%" for Raft, which is the figure in the cited IEEE Access version. The 45% out-of-service figure is correct. |
| A6 | 16 | Jensen et al.: Pre-Vote "would have prevented a known outage". | "might have prevented", which is what the authors claim. |
| A7 | 12 | Pre-Vote attributed to Ongaro and Ousterhout (2014). | Cite Ongaro's PhD thesis for Pre-Vote (reference B10); it is not in the USENIX ATC paper. |
| A8 | 12 | "Raft and Zab elect the most up-to-date node." | "Raft elects a node whose log is at least as up to date as those of a majority; Zab's fast leader election prefers the most up-to-date server." |
| A9 | 12 | Zab vote ordering attributed to Junqueira et al. (2011). | Say that the ordering (epoch, last update, server ID) is ZooKeeper's Fast Leader Election as implemented; the 2011 paper specifies the broadcast protocol. |
| A10 | 10, 19 | FloodMax at n = 3: "one round trip". | "one round (a single message delay)". The count of 6 messages is correct. |
| A11 | 11, 19 | Itai–Rodeh: no assumption stated; table says "Expected O(n) per round". | Add that the ring size must be known to the nodes. Give the cost as expected O(n log n) messages in total. |
| A12 | 5–6 | "comparison-based election on a ring needs Ω(n log n) messages (Burns, 1980; Frederickson and Lynch, 1987)". | "Election on an asynchronous ring needs Ω(n log n) messages (Burns, 1980), and the same bound holds for comparison-based algorithms on synchronous rings (Frederickson and Lynch, 1987)." |
| A13 | 5 | Ω as the weakest failure detector, no citation at the sentence. | Cite Chandra, Hadzilacos and Toueg (1996), already in the reference list, and add "given a majority of correct processes". |
| A14 | 16 | Wen and Law: "Models how packet loss stretches the expected wait for elections and commits". | "Models how delay and packet loss affect log commitment in Raft". The paper is mainly about commitment. |
| A15 | 33 | "Tanenbaum, A. S. and van Steen, M." | "van Steen, M. and Tanenbaum, A. S. *Distributed Systems*, 4th ed., 2023." |
| A16 | 21–22 | Table 6.2, "Our coordinator" row gives one count. | Add that 8 messages is the case where the detecting node is also the best candidate, as the two Bully rows already distinguish. |
| A17 | 21 | Ring row has node 1 initiating. | Note the assumption (node 2 retries the silent node on the COORDINATOR pass). If node 2 remembers that node 3 is silent, the count is 5 with one skip-timeout. |
| A18 | 18 | "Almost no published work elects a node per request." | "Per-request selection of a server is studied mainly as load balancing and task allocation rather than as election"; then cite the work in Section C. |
| A19 | 13, 29 | Raft rejected because it "could hide the mechanisms the course assesses". | The project description restricts frameworks that hide mechanisms, not algorithms we write ourselves. Give the real reasons: implementation effort, and compute concentrated on the leader if it also executes. Note that v2 does adopt Raft's log rules for the commit path. |

## B. Incomplete references: full details

| # | Page | Entry as written | Full reference |
|---|---|---|---|
| B1 | 35 | Dynatune, IEEE Access 2026 | Shiozaki, K. and Nakamura, J. Dynamic tuning of election parameters for timely leader failover in state machine replication. *IEEE Access* 14, pp. 128472–128487, 2026, doi:10.1109/ACCESS.2026.3724865. The published title has no "Dynatune:" prefix. Earlier version: Dynatune: Dynamic tuning of Raft election parameters using network measurement, IPDPSW 2025, pp. 568–577. |
| B2 | 35 | SCRaft, "IEEE, 2025" | Wang, Y., Luo, H., Sun, G. and Yu, H. SCRaft: Achieving fast and stable elections in Raft consensus algorithm. IEEE HPCC 2024, pp. 561–568, doi:10.1109/HPCC64274.2024.00081. **Check** that Xplore document 11083162 is this paper. |
| B3 | 35 | Improved Raft networking method, "ACM, 2024" | Wu, R. The study of a distributed networking method based on an improved Raft protocol. CNML 2024, pp. 37–42, doi:10.1145/3701047.3701055. **Check** that the mechanism is "shorter countdown for higher priority"; the abstract only says node priority was added, evaluated in simulation. |
| B4 | 35 | Brief announcement, ICDCN 2023 | Kumar, M. and Molla, A. R. Brief announcement: On the message complexity of fault-tolerant computation: leader election and agreement. ICDCN 2023, pp. 294–295, doi:10.1145/3571306.3571424. A full version appeared in IEEE TPDS (2023); prefer it. |
| B5 | 35 | Improved tradeoffs, PODC 2023 | Kutten, S., Robinson, P., Tan, M. M. and Zhu, X. Improved tradeoffs for leader election. ACM PODC 2023, pp. 355–365. |
| B6 | 35 | Wen and Law, "IEEE, 2022" | Wen, Y. and Law, K. L. E. Delay and loss rate analysis of the log commitment process in Raft. IEEE HPCC/DSS/SmartCity/DependSys 2022, pp. 347–356. |
| B7 | 35 | 3-Phase leader election, "IEEE, c. 2019" | Chaparala, P., Atmakuri, A. R. and Rao, S. S. S. 3-Phase leader election algorithm for distributed systems. ICCMC 2019, pp. 898–904, doi:10.1109/ICCMC.2019.8819837. |
| B8 | 35 | A new approach for election algorithm | Gholipour, M., Kordafshari, M. S., Jahanshahi, M. and Rahmani, A. M. A new approach for election algorithm in distributed systems. CTRQ 2009, pp. 70–74, doi:10.1109/CTRQ.2009.32. |
| B9 | 36 | A leader election algorithm in a distributed computing system | Kim, T. W., Kim, E. H., Kim, J. K. and Kim, T. Y. A leader election algorithm in a distributed computing system. IEEE FTDCS 1995, pp. 481–485, doi:10.1109/FTDCS.1995.525021. **Check** the wording "while the current one runs". |
| B10 | new | Pre-Vote source | Ongaro, D. *Consensus: Bridging Theory and Practice*. PhD thesis, Stanford University, 2014 (Section 9.6). |
| B11 | 36 | SEER | Sakic, E., Vizarreta, P. and Kellerer, W. SEER: Performance-aware leader election in single-leader consensus. arXiv:2104.01355, 2021. |
| B12 | 36 | Raft-PLUS | Reference is correct. **Check** the "four election policies and three opposition policies" count against the paper. |

## C. Literature to add

These bear directly on the per-request selection design. The bibliographic details below are from memory and must be verified before they are cited.

| Topic | Work | Why it belongs |
|---|---|---|
| Bidding for tasks | Smith, R. G. The Contract Net protocol. *IEEE Transactions on Computers* C-29(12), 1980. | The fresh-bid variant of MWBE is this protocol. |
| Stale load information | Mitzenmacher, M. How useful is old information? *IEEE TPDS* 11(1), 2000. Dahlin, M. Interpreting stale load information. *IEEE TPDS* 11(10), 2000. | Explains why "send to the least loaded node according to a cached score" causes herding. Motivates the local correction in v2. |
| Two choices | Mitzenmacher, M. The power of two choices in randomized load balancing. *IEEE TPDS* 12(10), 2001. | Standard low-cost baseline. |
| Load sharing | Eager, D., Lazowska, E. and Zahorjan, J. Adaptive load sharing in homogeneous distributed systems. *IEEE TSE* 12(5), 1986. | Classic result that simple policies capture most of the gain. |
| Zero-message selection | Thaler, D. and Ravishankar, C. Using name-based mappings to increase hit rates. *IEEE/ACM ToN* 6(1), 1998 (rendezvous hashing). Karger, D. et al. Consistent hashing and random trees. ACM STOC 1997. | The election-free alternative that MWBE must be compared against. |
| Asynchronous election | Garcia-Molina (1982), Invitation algorithm, in the paper already cited for Bully. | The partition-tolerant counterpart to Bully; the survey says Bully assumes synchrony but omits it. |
| Membership | Birman, K. and Joseph, T. Exploiting virtual synchrony in distributed systems. ACM SOSP 1987. Das, A., Gupta, I. and Motivala, A. SWIM. IEEE DSN 2002. | The agreed-view mechanism in v2 is group membership. |
| Stable leaders | Aguilera, M. et al. Stable leader election. DISC 2001. | Background for "no takeover by a recovered node". |

## D. Presentation

- Number the three figures and refer to them by number.
- The coordinator-election figure (p. 27) omits the pre-vote step that the text and Table 6.2 count.
- Section 3 should report how many results each query returned and how many passed screening.
- Section 1 promises each technique is evaluated against both jobs; Table 6.1 has one suitability column. Split it into "per-request selection" and "coordinator".
- Scope note in Section 3 excludes ad hoc and sensor-network election, but Section 7.7 proposes a super-peer election among churning peers. Either cite that literature or drop the super-peer option.
