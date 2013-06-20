-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--


load data local infile '/mnt/db_master/patstat_raw/dvd2/tls214_part01.txt' into table tls214_npl_publn fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
