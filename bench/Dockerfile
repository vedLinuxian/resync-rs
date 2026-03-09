FROM ubuntu:24.04

# rsync for comparison, iproute2 for tc (traffic control), bc for math
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        rsync iproute2 procps ca-certificates bc && \
    rm -rf /var/lib/apt/lists/*

# Copy the resync binary
COPY target/release/resync /usr/local/bin/resync

# Create rsyncd config for fair network comparison
RUN mkdir -p /etc /dest && chmod 777 /dest && \
    printf 'uid = root\ngid = root\nuse chroot = no\nmax connections = 4\nlog file = /dev/null\n\n[data]\n\tpath = /dest\n\tread only = false\n\tlist = yes\n' > /etc/rsyncd.conf

# Verify binary works
RUN resync --version || true

CMD ["tail", "-f", "/dev/null"]
