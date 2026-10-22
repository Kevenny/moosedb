#
# Runs the statement $q ({T} = table) on the MooseDB table m and on its InnoDB
# mirror i, each on freshly reloaded rows, and prints the errors, warnings and
# row count of both: the two blocks must be identical.
#
--disable_query_log
--disable_abort_on_error
--echo # $label
TRUNCATE m;
TRUNCATE i;
INSERT INTO m SELECT '2026-03-01 00:00:00' + INTERVAL seq MINUTE, CONCAT('h', seq MOD 3), seq FROM seq_0_to_59;
INSERT INTO i SELECT '2026-03-01 00:00:00' + INTERVAL seq MINUTE, CONCAT('h', seq MOD 3), seq FROM seq_0_to_59;
eval SET @q = "$q";
SET @s = REPLACE(@q, '{T}', 'm');
PREPARE st FROM @s;
--echo # MooseDB
EXECUTE st;
SHOW WARNINGS;
SELECT ROW_COUNT();
DEALLOCATE PREPARE st;
SET @s = REPLACE(@q, '{T}', 'i');
PREPARE st FROM @s;
--echo # InnoDB
EXECUTE st;
SHOW WARNINGS;
SELECT ROW_COUNT();
DEALLOCATE PREPARE st;
SELECT (SELECT COUNT(*) FROM m) AS rows_m, (SELECT COUNT(*) FROM i) AS rows_i;
--enable_abort_on_error
--enable_query_log
