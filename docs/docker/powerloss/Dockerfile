# Power-loss harness image: the official MariaDB 11.4 server + LazyFS (FUSE),
# a file system that keeps every write in its own page cache until fsync and
# can drop everything unsynced on command (lazyfs::clear-cache) or crash itself
# at a chosen syscall (lazyfs::crash::...).
#
#   docker build -f docs/docker/powerloss/Dockerfile -t moosedb-powerloss docs/docker/powerloss/
#
# FUSE needs the device and CAP_SYS_ADMIN at *run* time:
#   docker run --device /dev/fuse --cap-add SYS_ADMIN --security-opt apparmor:unconfined ...
# (or simply --privileged). See README.md / run.sh.
#
# The plugin is NOT part of the image: run.sh copies /repo/build/ha_moosedb.so
# into the server's plugin dir when the container starts, before mariadbd runs.
FROM mariadb:11.4

# LazyFS commit used by the 2026-10 runs (dsrhaslab/lazyfs main).
ARG LAZYFS_COMMIT=fa7d32ee9710d55a7e529ac3c375f6616563c78b

RUN apt-get update \
 && apt-get install -y --no-install-recommends git ca-certificates cmake g++ make \
      pkg-config fuse3 libfuse3-dev python3 python3-pymysql procps \
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/dsrhaslab/lazyfs.git /opt/lazyfs \
 && cd /opt/lazyfs && git checkout "$LAZYFS_COMMIT" \
 && cd /opt/lazyfs/libs/libpcache && ./build.sh \
 && cd /opt/lazyfs/lazyfs && ./build.sh \
 && test -x /opt/lazyfs/lazyfs/build/lazyfs

RUN sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf \
 || echo user_allow_other >> /etc/fuse.conf

ENTRYPOINT []
CMD ["bash"]
