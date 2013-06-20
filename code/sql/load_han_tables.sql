-- This runs a set of scripts to load a database table
--
--
-- Version March2012       Created by Mark Huberty    on 05/03/2012    
--

alter table han_name disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/han/201205_HAN_NAME.txt' into table han_name fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table han_name enable keys;

alter table han_person disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/han/201205_HAN_PERSON.txt' into table han_person fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table han_person enable keys;

alter table han_patent disable keys;
load data local infile '/mnt/db_master/patstat_raw/oecd/han/201205_HAN_PATENTS.txt' into table han_patent fields terminated by '|' optionally enclosed by '"' lines terminated by '\r\n' ignore 1 lines;
SHOW WARNINGS;
alter table han_patent enable keys;

