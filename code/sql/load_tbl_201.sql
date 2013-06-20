-- This runs a set of scripts to load a database table
--
--
-- Version March2012       Created by Mark Huberty    on 05/03/2012    
--

alter table tls201_appln disable keys;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls201_part01.txt' into table tls201_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls201_part02.txt' into table tls201_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls201_part03.txt' into table tls201_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls201_part04.txt' into table tls201_appln fields terminated by ',' optionally enclosed by '"' lines terminated by '\n' ignore 1 lines;
SHOW WARNINGS;
alter table tls201_appln enable keys;

