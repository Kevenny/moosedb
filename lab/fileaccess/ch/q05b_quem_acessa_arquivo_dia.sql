-- Quem acessa um arquivo (máquina e processo), no dia
SELECT host, process, COUNT(*) AS n FROM {{T}} WHERE file = '{{FILE0}}' AND ts >= '{{DAY0}}' AND ts < '{{DAY1}}' GROUP BY host, process ORDER BY n DESC, host, process LIMIT 50;
