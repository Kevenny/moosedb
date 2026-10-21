-- Top 20 processos no ambiente numa hora, com latência (AVG = lat_soma/n; p95 não é trivial no MariaDB, ver README)
SELECT process, COUNT(*) AS n, SUM(latency_us) AS lat_soma, MAX(latency_us) AS lat_max FROM {{T}} WHERE ts >= '{{HOUR0}}' AND ts < '{{HOUR1}}' GROUP BY process ORDER BY n DESC, process LIMIT 20;
