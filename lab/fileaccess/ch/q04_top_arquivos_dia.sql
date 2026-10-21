-- Top 20 arquivos mais acessados no dia
SELECT file, COUNT(*) AS n FROM {{T}} WHERE ts >= '{{DAY0}}' AND ts < '{{DAY1}}' GROUP BY file ORDER BY n DESC, file LIMIT 20;
