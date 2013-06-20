-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--

alter table tls207_pers_appln disable keys;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls207_part01.txt' into table tls207_pers_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls207_part02.txt' into table tls207_pers_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls207_part03.txt' into table tls207_pers_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls207_part04.txt' into table tls207_pers_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls207_part05.txt' into table tls207_pers_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
alter table tls207_pers_appln enable keys;
SHOW WARNINGS;
