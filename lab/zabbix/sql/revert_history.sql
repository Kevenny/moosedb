-- Desfaz sql/adapted_history.sql: as tabelas InnoDB originais (<t>_innodb)
-- voltam ao nome oficial; as MooseDB ficam como <t>_moose (apague à mão se quiser).
-- Uso: USE zabbix; SOURCE /lab/sql/revert_history.sql;   (zabbix_server parado)
RENAME TABLE history      TO history_moose,      history_innodb      TO history,
             history_uint TO history_uint_moose, history_uint_innodb TO history_uint,
             history_str  TO history_str_moose,  history_str_innodb  TO history_str,
             history_text TO history_text_moose, history_text_innodb TO history_text,
             history_log  TO history_log_moose,  history_log_innodb  TO history_log,
             history_bin  TO history_bin_moose,  history_bin_innodb  TO history_bin;
