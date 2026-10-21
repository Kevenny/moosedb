-- Quem acessa um arquivo (máquina e processo), 7 dias (sem predicado de tempo)
SELECT host, process, COUNT(*) AS n FROM {{T}} WHERE file = '{{FILE0}}' GROUP BY host, process ORDER BY n DESC, host, process LIMIT 50;
