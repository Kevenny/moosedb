#
# Runs one query on a MooseDB table and on its InnoDB mirror and compares the
# complete answers, row by row, in result order.
#
# In : $q       query text; {T} stands for the table (it is replaced by $mt,
#               then by $it). It must give a deterministic row order.
#      $label   text echoed above the verdict
#      $mt/$it  MooseDB table / mirror table names
#      $show    1 = also print the answer of the MooseDB run
#      $status  1 = print the Handler_read_* counters of the MooseDB run, to
#               show how many rows and index calls it took
#
# The answers are stored in two temporary InnoDB tables with an AUTO_INCREMENT
# column _n, so the comparison (EXCEPT both ways, _n included) is sensitive to
# the order of the rows too.
#
--disable_query_log
--disable_warnings
--echo # $label
eval SET @q = "$q";
eval SET @mt = '$mt';
eval SET @it = '$it';
DROP TEMPORARY TABLE IF EXISTS r_m, r_i;
SET @s = CONCAT('CREATE TEMPORARY TABLE r_m (_n INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AS ', REPLACE(@q, '{T}', @mt));
PREPARE st FROM @s;
FLUSH STATUS;
EXECUTE st;
DEALLOCATE PREPARE st;
if ($status)
{
  SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_first', 'Handler_read_key', 'Handler_read_last', 'Handler_read_next', 'Handler_read_prev', 'Handler_read_rnd_next');
}
SET @s = CONCAT('CREATE TEMPORARY TABLE r_i (_n INT AUTO_INCREMENT PRIMARY KEY) ENGINE=InnoDB AS ', REPLACE(@q, '{T}', @it));
PREPARE st FROM @s;
EXECUTE st;
DEALLOCATE PREPARE st;
SELECT COUNT(*) INTO @n FROM r_m;
SELECT COUNT(*) INTO @d FROM ((SELECT * FROM r_m EXCEPT SELECT * FROM r_i) UNION ALL (SELECT * FROM r_i EXCEPT SELECT * FROM r_m)) x;
if ($show)
{
  SELECT * FROM r_m ORDER BY _n;
}
SELECT @n AS result_rows, IF(@d = 0, 'SAME', 'DIFFERENT') AS vs_innodb;
DROP TEMPORARY TABLE r_m, r_i;
--enable_warnings
--enable_query_log
