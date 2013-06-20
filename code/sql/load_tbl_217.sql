-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--


load data local infile '/mnt/db_master/patstat_raw/dvd2/tls217_part01.txt' into table tls217_appln_ecla fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls217_part02.txt' into table tls217_appln_ecla fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls217_part03.txt' into table tls217_appln_ecla fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls217_part04.txt' into table tls217_appln_ecla fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
