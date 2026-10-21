-- Série por minuto de uma máquina no dia (1440 pontos)
SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i') AS minuto, COUNT(*) AS n, SUM(bytes) AS total_bytes FROM {{T}} WHERE host = '{{HOST0}}' AND ts >= '{{DAY0}}' AND ts < '{{DAY1}}' GROUP BY minuto ORDER BY minuto;
