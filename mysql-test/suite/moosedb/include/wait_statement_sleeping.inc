#
# Waits until $sleeping_n statements whose text matches the LIKE pattern
# $sleeping_like are inside SLEEP() ("User sleep"): the rows they wrote before
# the SLEEP are written, and the series they hold are claimed.
#
--let $wait_condition= SELECT COUNT(*) = $sleeping_n FROM information_schema.PROCESSLIST WHERE INFO LIKE '$sleeping_like' AND STATE = 'User sleep'
--source include/wait_condition.inc
