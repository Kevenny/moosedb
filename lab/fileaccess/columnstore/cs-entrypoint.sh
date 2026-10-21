#!/bin/bash
# Sobe os daemons do ColumnStore sem systemd (ordem de mariadb-columnstore-start.sh), roda o
# entrypoint oficial do MariaDB e, no SIGTERM (docker stop/restart), para tudo na ordem de
# mariadb-columnstore-stop.sh e salva o BRM (mcs-savebrm.py), como o ExecStopPost do systemd.
set -e
JE=$(ldconfig -p | grep -m1 libjemalloc | awk '{print $4}')
LOG=/var/log/mariadb/columnstore/daemons.log
mkdir -p /var/log/mariadb/columnstore/install /var/lib/columnstore
# volume vazio: recria a árvore que o postinst do pacote deixou na imagem
[ -d /var/lib/columnstore/data1 ] || cp -a /opt/cs-seed/. /var/lib/columnstore/
chown -R mysql:mysql /var/lib/columnstore /var/log/mariadb /etc/columnstore
run() { su -s /bin/bash mysql -c "MALLOC_CONF='' ${2:+LD_PRELOAD=$JE} exec $1" >>$LOG 2>&1 & sleep 1; }
/usr/bin/mcs-loadbrm.py no >>$LOG 2>&1 || true
run "/usr/bin/workernode DBRM_Worker1"
run /usr/bin/controllernode
run /usr/bin/PrimProc je
run /usr/bin/WriteEngineServer je
sleep 2
run /usr/bin/DMLProc
run /usr/bin/DDLProc
sleep 3
su -s /bin/sh -c '/usr/bin/dbbuilder 7' mysql >/var/log/mariadb/columnstore/install/dbbuilder.log 2>&1 || true

stopx() { pkill -TERM -x "$1" || true; for _ in $(seq 60); do pgrep -x "$1" >/dev/null || return 0; sleep 1; done; pkill -KILL -x "$1" || true; }
term() {
  kill -TERM "$MPID" 2>/dev/null; wait "$MPID" || true
  for p in DMLProc DDLProc WriteEngineServ PrimProc controllernode workernode; do stopx $p; done
  /usr/bin/mcs-savebrm.py >>$LOG 2>&1 || echo "mcs-savebrm.py falhou" >>$LOG
  clearShm >/dev/null 2>&1 || true
  exit 0
}
trap term TERM INT
docker-entrypoint.sh "$@" & MPID=$!
wait "$MPID"
