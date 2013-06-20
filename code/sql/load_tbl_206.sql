-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--
-- SET sql_mode='NO_BACKSLASH_ESCAPES';
alter table tls206_person disable keys;
-- load data local infile '/mnt/db_master/patstat_raw/dvd1/tls206_part01_clean.txt' into table tls206_person fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
-- SHOW WARNINGS;
-- load data local infile '/mnt/db_master/patstat_raw/dvd1/tls206_part02_clean.txt' into table tls206_person fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
-- SHOW WARNINGS;
-- load data local infile '/mnt/db_master/patstat_raw/dvd1/tls206_part03_clean.txt' into table tls206_person fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
-- SHOW WARNINGS;
-- load data local infile '/mnt/db_master/patstat_raw/dvd1/tls206_part04_clean.txt' into table tls206_person fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls206_part05_clean.txt' into table tls206_person fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table tls206_person enable keys;
