# MariaDB 11.4.13 (mesma versão do contêiner da MooseDB) + ColumnStore do repositório oficial do 11.4.13.
FROM mariadb:11.4.13
RUN apt-get update && apt-get install -y --no-install-recommends mariadb-plugin-columnstore libjemalloc2 \
 && rm -rf /var/lib/apt/lists/* \
 && cp -a /var/lib/columnstore /opt/cs-seed \
 # O PrimProc dimensiona memória pela RAM do host (30 GiB), não pelo limite do contêiner (10 GiB):
 # cache de blocos 13% (~4 GiB) e memória de join/agregação fixa em 3 GiB cabem nos 10 GiB.
 && mcsSetConfig DBBC NumBlocksPct 13 \
 && mcsSetConfig HashJoin TotalUmMemory 3G
COPY cs-entrypoint.sh /usr/local/bin/
ENTRYPOINT ["bash", "/usr/local/bin/cs-entrypoint.sh"]
CMD ["mariadbd"]
