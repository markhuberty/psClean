-- This script inserts new tables into the patstatOct2011 db
-- for holding the Leuven disambiguated data




-- ---------------------------
-- leuven_name
-- Holds the name table from the KU-Leuven dataset
-- ---------------------------

DROP TABLE IF EXISTS `leuven_name`;

CREATE TABLE `leuven_name` (

  person_id int(8) NOT NULL default '0',

  person_ctry_code varchar(2) NOT NULL default '',

  person_name text NOT NULL,

  htm_l1 text NOT NULL,

  hrm_l2 text NOT NULL,

  hrm_level int(1),
  
  hrm_l2_id int(8),     

  sector char(50),
 
  person_address text NOT NULL,

  doc_std_name_id int(8),

  PRIMARY KEY  (person_id),

  INDEX (person_ctry_code)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;

