# One image for the seven containers of the emulated lab (docs/deployment.md
# section 11): Ubuntu 24.04 as in the lab, the release build of this commit,
# an ssh server so run_sweep.py drives it as it would six machines, and tc
# for the emulated network delay.
FROM ubuntu:24.04
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential ca-certificates curl git openssh-server openssh-client \
        iproute2 procps perl python3 tar && rm -rf /var/lib/apt/lists/*
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
ENV PATH=/root/.cargo/bin:$PATH
WORKDIR /root/RescueMesh
COPY . .
# A clean checkout of the committed tree, so the preflight's commit check holds.
RUN git checkout -- . && git status --porcelain --untracked-files=no && cargo build --release \
    && ls -l target/release/rm-node target/release/rm-loadgen target/release/rm-ctl
RUN mkdir -p /run/sshd /root/.ssh && chmod 700 /root/.ssh && ssh-keygen -A \
    && printf 'PermitRootLogin prohibit-password\nPasswordAuthentication no\nUseDNS no\nMaxStartups 100:30:200\nMaxSessions 100\n' > /etc/ssh/sshd_config.d/lab.conf
COPY experiments/docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh"]
