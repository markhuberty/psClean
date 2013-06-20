-- This runs a set of scripts to load a database table
--
--
-- Version March2008    Created by Tony Teculescu    on 04/03/2008    
--

alter table tls204_appln_prior disable keys;
load data local infile '/mnt/db_master/patstat_raw/dvd1/tls204_part01.txt' into table tls204_appln_prior fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
alter table tls204_appln_prior enable keys;
