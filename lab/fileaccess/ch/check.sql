SELECT concat(toString(count()), ' linhas sum(bytes)=', toString(sum(bytes)), ' sum(lat)=', toString(sum(latency_us))) FROM fileacc.evento
