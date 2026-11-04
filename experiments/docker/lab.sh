#!/usr/bin/env bash
# The emulated lab: 3 cloud + 3 client containers and a controller on one
# private network, with the lab's measured round trip added to every packet
# (docs/deployment.md section 11, DL-077).
#
#   experiments/docker/lab.sh build        # the image (release build inside, a few minutes)
#   experiments/docker/lab.sh up           # network, ssh key, the seven containers
#   experiments/docker/lab.sh ctl ARGS...  # a command in the controller, e.g. a sweep
#   experiments/docker/lab.sh down         # remove the containers and the network
#
# Results land in this checkout's results/ (mounted into the controller).
set -euo pipefail
cd "$(dirname "$0")/../.."
IMAGE=rescuemesh-lab
NET=rescuemesh-lab
SUBNET=172.30.0.0/24
# The lab's ping: 3.3 ms average round trip, 0.7 ms deviation. Half of it on
# each direction's packets.
NETEM=${NETEM:-"delay 1.65ms 0.35ms distribution normal"}
CLOUD_CPUS=${CLOUD_CPUS:-4}
CLIENT_CPUS=${CLIENT_CPUS:-2}
KEYS=.docker-lab-keys

start() {  # name ip cpus netem
  docker run -d --name "$1" --hostname "$1" --network "$NET" --ip "$2" \
    --cpus "$3" --cap-add NET_ADMIN --ulimit nofile=65536:65536 \
    -e NETEM="$4" -v "$PWD/$KEYS:/keys:ro" "${@:5}" "$IMAGE" >/dev/null
  echo "  $1  $2  cpus $3  ${4:-no delay}"
}

case "${1:-}" in
  build)
    docker build -f experiments/docker/Dockerfile -t "$IMAGE" . ;;
  up)
    docker network inspect "$NET" >/dev/null 2>&1 || docker network create --subnet "$SUBNET" "$NET" >/dev/null
    mkdir -p "$KEYS" results
    [ -f "$KEYS/id_ed25519" ] || ssh-keygen -q -t ed25519 -N "" -f "$KEYS/id_ed25519"
    for i in 1 2 3; do start "cloud$i" "172.30.0.1$i" "$CLOUD_CPUS" "$NETEM"; done
    for i in 1 2 3; do start "client$i" "172.30.0.2$i" "$CLIENT_CPUS" "$NETEM"; done
    start controller 172.30.0.30 1 "" -v "$PWD/results:/root/RescueMesh/results"
    sleep 2
    docker exec controller sh -c 'for h in 11 12 13 21 22 23; do ssh -o BatchMode=yes root@172.30.0.$h hostname || exit 1; done' \
      && echo "ssh from the controller to all six: ok" ;;
  ctl)
    shift; docker exec -i -w /root/RescueMesh controller "$@" ;;
  down)
    docker rm -f controller cloud1 cloud2 cloud3 client1 client2 client3 >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    echo "removed" ;;
  *)
    sed -n '2,13p' "$0"; exit 2 ;;
esac
