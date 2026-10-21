#!/usr/bin/env bash
# Onde o tempo vai: engine (leitura) x executor do MariaDB. Melhor de RUNS (servidor, NOW(6)).
#   ENGINES="innodb moose moose_h" bash scripts/breakdown.sh | tee results/breakdown_20m.txt
set -uo pipefail
export MSYS_NO_PATHCONV=1
cd "$(dirname "$0")/.."
RUNS=${RUNS:-3}; ENGINES=${ENGINES:-"innodb moose moose_h"}; PW="${ROOT_PASSWORD:-fileacc}"
DAY="ts >= '2026-10-15 00:00:00' AND ts < '2026-10-16 00:00:00'"
M() { docker exec -i fileacc-mariadb mariadb -uroot -p"$PW" -N -B fileacc "$@"; }
for e in $ENGINES; do
  t=evento_$e
  echo "## $t"
  grep -E '^b[0-9]+ ' sql/breakdown.sql | while IFS='|' read -r lab sqltxt; do
    sqltxt=${sqltxt//\{\{T\}\}/$t}; sqltxt=${sqltxt//\{\{DAY\}\}/$DAY}
    best=""
    for ((i = 0; i <= RUNS; i++)); do   # a 1ª execução aquece
      s=$(M -e "SET @t0=NOW(6); $sqltxt; SELECT TIMESTAMPDIFF(MICROSECOND,@t0,NOW(6))/1e6" 2>&1 </dev/null | tail -1)
      [ $i -gt 0 ] && best=$(awk -v a="${best:-$s}" -v c="$s" 'BEGIN{print (c<a)?c:a}')
    done
    echo "$lab	$best"
  done
done
