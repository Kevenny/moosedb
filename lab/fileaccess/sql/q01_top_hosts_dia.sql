-- Top 10 máquinas por volume (SUM(bytes)) no dia
SELECT host, SUM(bytes) AS total_bytes FROM {{T}} WHERE ts >= '{{DAY0}}' AND ts < '{{DAY1}}' GROUP BY host ORDER BY total_bytes DESC, host LIMIT 10;
