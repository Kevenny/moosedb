#
# Runs one query shape against a MooseDB table and its InnoDB mirror and
# reports whether the two answers are identical.
#
# In : $mt           MooseDB table name
#      $it           InnoDB mirror table name
#      $sel          three comma separated expressions (count, a sum, a checksum)
#      $tail         everything after "FROM <table> AS tt" (joins, WHERE ...)
#      $show_status  1 = also show Handler_read_rnd_next of the MooseDB run
#                    (rows handed to the server: matching series only when the
#                    predicate was pushed down, +1 for the end of scan)
#      $quiet        1 = do not print; the verdict goes to the table `verdicts`
#                    (tag VARCHAR, same INT) instead
#
--disable_query_log
--disable_warnings
if (!$quiet)
{
  --echo # $tail
}
FLUSH STATUS;
eval SELECT $sel INTO @a1, @b1, @c1 FROM $mt AS tt $tail;
if ($show_status)
{
  SHOW SESSION STATUS LIKE 'Handler_read_rnd_next';
}
eval SELECT $sel INTO @a2, @b2, @c2 FROM $it AS tt $tail;
if ($quiet)
{
  eval INSERT INTO verdicts VALUES ('$tail', @a1 <=> @a2 AND @b1 <=> @b2 AND @c1 <=> @c2);
}
if (!$quiet)
{
  SELECT @a1 AS a, @b1 AS b, @c1 AS c, IF(@a1 <=> @a2 AND @b1 <=> @b2 AND @c1 <=> @c2, 'SAME', 'DIFFERENT') AS vs_innodb;
}
--enable_warnings
--enable_query_log
