#!/bin/sh
# Container start: the shared ssh key, the emulated network delay, sshd.
set -e
if [ -f /keys/id_ed25519.pub ]; then
  cp /keys/id_ed25519.pub /root/.ssh/authorized_keys && chmod 600 /root/.ssh/authorized_keys
fi
if [ -f /keys/id_ed25519 ]; then
  cp /keys/id_ed25519 /root/.ssh/id_ed25519 && chmod 600 /root/.ssh/id_ed25519
  printf 'Host *\n  StrictHostKeyChecking no\n  UserKnownHostsFile /dev/null\n  LogLevel ERROR\n' > /root/.ssh/config
fi
# NETEM, e.g. "delay 1.65ms 0.35ms distribution normal": added to every packet
# this container sends, so a round trip between two containers gets it twice.
if [ -n "${NETEM:-}" ]; then
  tc qdisc replace dev eth0 root netem $NETEM limit 100000
fi
exec /usr/sbin/sshd -D -e
