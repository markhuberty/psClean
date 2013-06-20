-- This script inserts new tables into the patstatOct2011 db
-- for holding the OECD / HAN data




-- Version December2012     
-- Author Mark Huberty
-- Date 10 December 2012

-- NOTE: These scripts work for Unix-like systems where table names
-- are case sensitive. 

-- Recommend to run the CREATE syntax in mysql directly.
-- The rest of the script runs as:
-- mysql -vvv -h localhost -u root -p patstatOct2011 < create_tables.sql > -- create_tables_report.txt mysql -vvv -h localhost -u root -p patstatOct20-- 11 < create_tables.sql > create_tables_report.txt 

-- CREATE DATABASE `patstatOct2011`; /*!40100 DEFAULT CHARACTER SET latin1 */
-- USE `patstatOct2011`;



--

-- Table `tls201_appln`

--


-- ---------------------------
-- han_name
-- Holds the HAN name table from the OECD
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

