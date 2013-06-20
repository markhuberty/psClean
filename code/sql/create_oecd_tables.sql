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

DROP TABLE IF EXISTS `han_name`;

CREATE TABLE `han_name` (


`HAN_ID` int(10) NOT NULL default '0',

`Person_name_clean` text NOT NULL,

`Person_ctry_code` varchar(2) NOT NULL default '',

`Matched` int(1) NOT NULL default '0',

PRIMARY KEY (`HAN_ID`)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;


-- ---------------------------
-- han_person
-- Holds the HAN person table from the OECD
-- ---------------------------

DROP TABLE IF EXISTS `han_person`;

CREATE TABLE `han_person` (

HAN_ID int(10) NOT NULL default '0',

Apr12_Person_id int(10) NOT NULL default '0',

OCT11_Person_id int(10) NOT NULL default '0',

Person_name_clean text NOT NULL default '',

Person_ctry_code varchar(2) NOT NULL default '',

PRIMARY KEY (HAN_ID),

INDEX (OCT11_Person_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;


-- ---------------------------
-- han_patent
-- Holds the HAN person:patent correspondence from the OECD
-- ---------------------------

DROP TABLE IF EXISTS `han_patent`;

CREATE TABLE `han_patent` (

`HAN_ID` int(10) NOT NULL default '0',

`Appln_id` int(10) NOT NULL default '0',

`Version` int(6) NOT NULL default '0',

`Publn_auth` varchar(2) NOT NULL default '',

`Patent_number` text NOT NULL default '',

PRIMARY KEY (`HAN_ID`),

INDEX (Appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ---------------------------
-- triadic_patent
-- Holds the OECD triadic patent families core table
-- ---------------------------
DROP TABLE IF EXISTS triadic_core;
CREATE TABLE `triadic_core` (

Family_id int(10) NOT NULL,
Count_prio int(10) NOT NULL default '0',
First_Prio date default NULL,
Last_Prio date default NULL,
USPTO_App date default NULL,
USPTO_app_last date default NULL,
USPTO_grant date default NULL,
Count_USPTO int(10) NOT NULL default '0',
EPO_App date default NULL,
EPO_App_last date default NULL,
EPO_Grant date default NULL,
Count_EPO int(10) NOT NULL default '0',
JPO_App date default NULL,
JPO_App_last date default NULL,
JPO_Grant date default NULL,
Count_JPO int(10) NOT NULL default '0',
PCT_App date default NULL,
Count_PCT int(10) NOT NULL default '0',

PRIMARY KEY (Family_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ---------------------------
-- triadic_inventor
-- Holds the triadic patent: inventor correspondence table
-- ---------------------------
DROP TABLE IF EXISTS triadic_inventors;
CREATE TABLE `triadic_inventors` (

Family_id int(10) NOT NULL,
Inventor text NOT NULL,
Address text NOT NULL,
Country varchar(2) NOT NULL default '',
Inventor_count int(3) NOT NULL default '0',
Inventor_share decimal(6) NOT NULL default '0.0',

PRIMARY KEY (Family_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ---------------------------
-- triadic_priority
-- Holds the triadic patent:priority listing correspondence table
-- ---------------------------
DROP TABLE IF EXISTS triadic_priority;
CREATE TABLE `triadic_priority` (

Family_id int(10) NOT NULL,
Patent_Nbr text default '',
Parent_Type varchar(8) NOT NULL default '',
Appln_id int(10) NOT NULL default '0',

PRIMARY KEY (Family_id),

INDEX (Appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;
