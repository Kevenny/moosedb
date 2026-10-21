-- Top 20 processos de uma máquina no dia
SELECT process, COUNT(*) AS n, SUM(bytes) AS total_bytes FROM {{T}} WHERE host = '{{HOST0}}' AND ts >= '{{DAY0}}' AND ts < '{{DAY1}}' GROUP BY process ORDER BY n DESC, process LIMIT 20;
