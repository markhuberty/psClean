-- This runs a set of scripts to load a database table
--
--
-- Version March2012       Created by Mark Huberty    on 05/03/2012    
--

alter table triadic_core disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/triadic/201206_TPF_CORE.txt' into table triadic_core fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table triadic_core enable keys;

alter table triadic_inventors disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/triadic/201206_TPF_INVENTORS.txt' into table triadic_inventors fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table triadic_inventors enable keys;

alter table triadic_priority disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/triadic/201206_TPF_PRIORITY.txt' into table triadic_priority fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table triadic_priority enable keys;

