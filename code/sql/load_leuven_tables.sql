-- This runs a set of scripts to load a database table
--
--
-- Version March2012       Created by Mark Huberty    on 05/03/2012    
--

alter table leuven_name disable keys;
load data local infile '/mnt/db_master/patstat_raw/leuven/EEE_PPAT_Oct2011.csv' into table leuven_name fields terminated by ',' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table leuven_name enable keys;
