-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--

alter table tls209_appln_ipc disable keys;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part01.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part02.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part03.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part04.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part05.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part06.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1//tls209_part07.txt' into table tls209_appln_ipc fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
alter table tls209_appln_ipc enable keys;
