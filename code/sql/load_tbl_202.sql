-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--


load data local infile '/mnt/db_master/patstat_raw/dvd1/tls202_part01.txt' into table tls202_appln_title fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls202_part02.txt' into table tls202_appln_title fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls202_part03.txt' into table tls202_appln_title fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls202_part04.txt' into table tls202_appln_title fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;

