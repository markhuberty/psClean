-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--


load data local infile '/mnt/db_master/patstat_raw/dvd2/tls218_part01.txt' into table tls218_docdb_fam fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;