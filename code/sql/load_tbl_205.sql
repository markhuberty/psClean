-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--


load data local infile '/mnt/db_master/patstat_raw/dvd1/tls205_part01.txt' into table tls205_tech_rel fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
