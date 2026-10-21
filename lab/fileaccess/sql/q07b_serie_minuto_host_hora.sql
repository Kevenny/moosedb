-- Série por minuto de uma máquina numa hora (60 pontos)
SELECT DATE_FORMAT(ts, '%Y-%m-%d %H:%i') AS minuto, COUNT(*) AS n, SUM(bytes) AS total_bytes FROM {{T}} WHERE host = '{{HOST0}}' AND ts >= '{{HOUR0}}' AND ts < '{{HOUR1}}' GROUP BY minuto ORDER BY minuto;
