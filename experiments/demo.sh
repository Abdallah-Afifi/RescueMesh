#!/usr/bin/env bash
# Live demo of the four Phase 1 cloud scenarios (plus automatic injection).
#
#   experiments/demo.sh --local                 # three nodes on this machine
#   experiments/demo.sh                         # the six machines (experiments/machines.toml,
#                                               #  config/cluster.toml)
#   experiments/demo.sh --pause                 # wait for Enter before each scenario
#
# Options:
#   --local               start three nodes and one client on localhost (config/local.toml)
#   --machines FILE       machines file (default experiments/machines.toml)
#   --config FILE         cluster config (default config/cluster.toml, or config/local.toml with --local)
#   --pause               "press Enter to continue" before each scenario
#   --rate R              base load in requests/s over all client machines (default 150)
#   --burst-x F           step 1's burst adds F x the base rate for 10 s (default 4: 5x the base in total)
#   --top-k K             planner cost knob for the demo (default: the config's); larger = costlier plans
#   --knob-node N         node to load artificially (default 2)
#   --knob-threads T      burner threads (default 16; use at least the node's core count)
#   --silence TARGET      coordinator (default) or rank1 or a node id
#   --silence-ms MS       length of the silence (default 15000, at most 20000)
#   --skip-auto           skip the last step (automatic fault injection by the cloud)
#   --preflight / --no-preflight
#                         check commit, binaries, clocks and ports on all hosts first
#                         (default: on for the six machines, off with --local)
#   --keep                leave the cluster running at the end
#
# Stopping: Ctrl-C, or `kill -TERM <pid of demo.sh>`, stops every step at once
# and the cluster. A demo started as a background job (`demo.sh &`) ignores
# Ctrl-C (the shell gives background jobs SIGINT ignored): use kill -TERM.
#
# Needs Python 3.11+ (tomllib) as $PYTHON (default python3), the release
# binaries on every machine (cargo build --release), and for the six-machine
# mode password-less ssh to all of them. Every step only reads `rm-ctl status`
# and sends `rm-ctl` commands, so it can be rerun at any point.

set -u
cd "$(dirname "$0")/.."
PYTHON=${PYTHON:-python3}
LOCAL=0; MACHINES=experiments/machines.toml; CONFIG=""; PAUSE=0; RATE=150; BURST_X=4; TOP_K=""
KNOB_NODE=2; KNOB_THREADS=16; SILENCE=coordinator; SILENCE_MS=15000; AUTO=1; KEEP=0; PREFLIGHT=""

while [ $# -gt 0 ]; do
  case "$1" in
    --local) LOCAL=1 ;;
    --machines) MACHINES=$2; shift ;;
    --config) CONFIG=$2; shift ;;
    --pause) PAUSE=1 ;;
    --rate) RATE=$2; shift ;;
    --burst-x) BURST_X=$2; shift ;;
    --top-k) TOP_K=$2; shift ;;
    --knob-node) KNOB_NODE=$2; shift ;;
    --knob-threads) KNOB_THREADS=$2; shift ;;
    --silence) SILENCE=$2; shift ;;
    --silence-ms) SILENCE_MS=$2; shift ;;
    --skip-auto) AUTO=0 ;;
    --preflight) PREFLIGHT=1 ;;
    --no-preflight) PREFLIGHT=0 ;;
    --keep) KEEP=1 ;;
    -h|--help) sed -n '2,34p' "$0"; exit 0 ;;
    *) echo "unknown option $1 (see --help)"; exit 2 ;;
  esac
  shift
done
[ -z "$PREFLIGHT" ] && { [ $LOCAL = 1 ] && PREFLIGHT=0 || PREFLIGHT=1; }

if ! "$PYTHON" -c 'import sys, tomllib; sys.exit(sys.version_info < (3, 11))' 2>/dev/null; then
  echo "Python 3.11+ is needed (set PYTHON=...)"; exit 2
fi
if [ $LOCAL = 1 ]; then
  for b in rm-node rm-loadgen rm-ctl; do
    [ -x "target/release/$b" ] || { echo "target/release/$b is missing: run cargo build --release"; exit 2; }
  done
else
  command -v ssh >/dev/null || { echo "ssh is needed for the six-machine demo"; exit 2; }
  # No local binaries needed: nodes and load run on the six machines, and
  # rm-ctl runs here only if target/release/rm-ctl exists, else on client machine 1.
fi

H=("$PYTHON" experiments/demo_helper.py)
if [ $LOCAL = 1 ]; then H+=(--local); else H+=(--machines "$MACHINES"); fi
[ -n "$CONFIG" ] && H+=(--base-config "$CONFIG")

B=$'\033[1m'; R=$'\033[0m'
banner() { printf '\n%s==================================================================%s\n' "$B" "$R"
           printf '%s  %s%s\n' "$B" "$1" "$R"
           printf '%s==================================================================%s\n' "$B" "$R"
           [ -n "${2:-}" ] && printf '%s\n' "$2"; }
say() { printf '\n%s>> %s%s\n' "$B" "$*" "$R"; }
pause() { if [ $PAUSE = 1 ]; then read -r -p "   (press Enter to continue) " _; else sleep "${1:-2}"; fi; }

# Every helper step runs in the background and is waited for, so a signal
# reaches this script at once (bash defers traps while a foreground child runs).
CHILD=""; REC_PID=""; TAIL_PID=""
h() { "${H[@]}" "$@" & CHILD=$!; wait "$CHILD"; local rc=$?; CHILD=""; return $rc; }
cleanup() { if [ $KEEP = 0 ]; then say "stopping the cluster and the load"; "${H[@]}" stop; fi; }
on_signal() {
  trap '' INT TERM
  for p in $CHILD $REC_PID $TAIL_PID; do kill -TERM "$p" 2>/dev/null; done
  wait 2>/dev/null
  echo; KEEP=0; cleanup; exit 130
}
trap on_signal INT TERM

# ------------------------------------------------------------------ start
if [ $PREFLIGHT = 1 ]; then
  banner "Preflight" "Same commit, binaries built from it, clocks, ports, on every host."
  h preflight || { say "preflight refused (see above); --no-preflight skips it"; exit 1; }
fi
banner "Starting the cloud" "Three cloud nodes, one process each$([ $LOCAL = 1 ] && echo ' (here all on localhost)')."
SETUP=(setup); [ -n "$TOP_K" ] && SETUP+=(--top-k "$TOP_K")
h "${SETUP[@]}" || { cleanup; exit 1; }
h status

# ------------------------------------------------------------------ 1
BURST=$((RATE * BURST_X)); BURST_S=10
banner "1. Normal cloud operation" \
"Every request is sent to all three nodes, and the nodes elect its executor
from their current load scores (MWBE): each request is a separate election.
Below, once a second: how many plans each node executed, its share, its
machine's CPU and its planning queue. The split moves from second to second
because each request is elected on the scores of that moment."
pause
h load --rate "$RATE"   # until the demo stops it
h watch --secs 8
say "now a burst: +$BURST requests/s for ${BURST_S} s, $((RATE + BURST)) requests/s in total.
   Queues and CPU rise on whichever node is elected most, its score rises, and
   the next requests go elsewhere. At this scale the cloud still has spare
   capacity, so the split stays near even: step 2 shows a real imbalance."
h load --rate "$BURST" --secs "$BURST_S" --tag burst
h watch --secs $((BURST_S + 4))   # the burst, its 3 s drain, and a second of quiet
say "the burst is over: back to $RATE requests/s"

# ------------------------------------------------------------------ 2
banner "2. Load imbalance" \
"An artificial CPU burner starts inside node $KNOB_NODE ($KNOB_THREADS threads).
Its load score rises, so the election should send it less work."
pause
if h ctl load "$KNOB_NODE" --threads "$KNOB_THREADS"; then
  h watch --secs 15 --mark "$KNOB_NODE"
  say "turning the burner off: node $KNOB_NODE should get its share back"
  h ctl load "$KNOB_NODE" --threads 0
  h watch --secs 8 --mark "$KNOB_NODE"
else
  say "this rm-ctl cannot start the load knob (rm-ctl load); skipping the imbalance step"
fi

# ------------------------------------------------------------------ 3
case "$SILENCE" in
  coordinator) WHO="the coordinator (node $("${H[@]}" coordinator || echo '?'))" ;;
  rank1) WHO="the node ranked first for the next request" ;;
  *) WHO="node $SILENCE" ;;
esac
banner "3. Server failure" \
"Silencing $WHO for $((SILENCE_MS / 1000)) s during the load: it ignores all traffic
but keeps running. The other two detect it, remove it from the view and
$([ "$SILENCE" = coordinator ] && echo 'elect a new coordinator; ')keep serving."
pause
ok=0
for _ in 1 2 3 4 5 6; do   # refused while an earlier silence is still settling: retry
  if h ctl silence "$SILENCE" --duration-ms "$SILENCE_MS"; then ok=1; break; fi
  sleep 5
done
[ $ok = 1 ] || say "the silence was refused (see above)"
# Whatever the target was (coordinator, rank1, an id), the victim is the node
# that now reports itself silenced.
VICTIM=$("${H[@]}" silenced --wait 5) || VICTIM=""
REC_LOG=runs/demo/recovery.log
mkdir -p runs/demo
if [ -n "$VICTIM" ]; then
  say "node $VICTIM is silenced"
  # Watch the rejoin from now on, every 0.1 s, so step 4 can show it however
  # long this step and its pause take (SYNCING lasts well under a second).
  "${H[@]}" recovery --node "$VICTIM" --secs $(( SILENCE_MS / 1000 + 60 )) > "$REC_LOG" 2>&1 &
  REC_PID=$!
else
  say "could not see which node is silenced"
fi
WATCH_S=$(( SILENCE_MS / 1000 > 9 ? 6 : SILENCE_MS / 2000 ))
h watch --secs "$WATCH_S" --mark "${VICTIM:-none}"
say "service continues with two nodes; the new coordinator and view:"
h status

# ------------------------------------------------------------------ 4
banner "4. Server recovery" \
"When the silence ends, node ${VICTIM:-?} rejoins as SYNCING, catches up from the
coordinator, becomes ACTIVE in a new view, and only then takes work again.
(Watched every 0.1 s since the silence began.)"
pause 0
if [ -n "$REC_PID" ]; then
  tail -n +1 -f "$REC_LOG" & TAIL_PID=$!
  wait "$REC_PID"; REC_PID=""
  sleep 0.3; kill "$TAIL_PID" 2>/dev/null; wait "$TAIL_PID" 2>/dev/null; TAIL_PID=""
fi
h watch --secs 6 --mark "${VICTIM:-none}"

# ------------------------------------------------------------------ 5
if [ $AUTO = 1 ]; then
  banner "5. Automatic fault injection" \
"The cloud now injects failures by itself ([inject] enabled = true): every
20-25 s the coordinator picks a victim from hash(seed, k) and silences it for
4-8 s. Restarting the cluster with that configuration."
  pause
  h stop >/dev/null
  SETUP=(setup --auto-inject); [ -n "$TOP_K" ] && SETUP+=(--top-k "$TOP_K")
  h "${SETUP[@]}" || { cleanup; exit 1; }
  h load --rate "$RATE"
  h watch --secs 60
fi

banner "Done" "Event logs of this demo are under logs/demo-*/ on every machine;
python analysis/analyze.py <dir with the run directories> draws their timelines."
cleanup
