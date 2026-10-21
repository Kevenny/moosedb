-- Top 20 pastas mais lidas numa hora (COUNT de op='read')
SELECT folder, COUNT(*) AS n FROM {{T}} WHERE op = 'read' AND ts >= '{{HOUR0}}' AND ts < '{{HOUR1}}' GROUP BY folder ORDER BY n DESC, folder LIMIT 20;
