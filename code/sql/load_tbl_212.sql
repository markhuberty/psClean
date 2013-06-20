-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--

alter table tls212_citation disable keys;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part01.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part02.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part03.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part04.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part05.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part06.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
load data local infile '/mnt/db_master/patstat_raw/dvd2/tls212_part07.txt' into table tls212_citation fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table tls212_citation enable keys;
