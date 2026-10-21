-- Decomposição do custo (usada por scripts/breakdown.sh): separa a leitura da engine do executor do MariaDB.
-- {{T}} = tabela; {{DAY}} = predicado do dia. Uma instrução por linha; "rótulo | SQL".
b1 scan_dia_count_star     | SELECT COUNT(*) FROM {{T}} WHERE {{DAY}}
b2 scan_dia_soma_1col_int  | SELECT SUM(latency_us) FROM {{T}} WHERE {{DAY}}
b3 scan_dia_soma_bytes     | SELECT SUM(bytes) FROM {{T}} WHERE {{DAY}}
b4 scan_dia_le_file        | SELECT SUM(LENGTH(file)) FROM {{T}} WHERE {{DAY}}
b5 scan_dia_le_file_folder | SELECT SUM(LENGTH(file)+LENGTH(folder)) FROM {{T}} WHERE {{DAY}}
b6 scan_dia_group_host     | SELECT host, COUNT(*) FROM {{T}} WHERE {{DAY}} GROUP BY host
b7 scan_dia_group_file     | SELECT COUNT(*) FROM (SELECT file, COUNT(*) n FROM {{T}} WHERE {{DAY}} GROUP BY file) x
b8 full_7d_soma_1col       | SELECT SUM(latency_us) FROM {{T}}
b9 full_7d_ignore_ix_ts_dia| SELECT SUM(latency_us) FROM {{T}} IGNORE INDEX (ix_ts) WHERE {{DAY}}
b10 full_7d_le_file        | SELECT SUM(LENGTH(file)) FROM {{T}}
