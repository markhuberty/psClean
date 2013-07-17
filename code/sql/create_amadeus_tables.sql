-- This script inserts new tables into the patstatOct2011 db
-- for holding the Leuven disambiguated data




-- ---------------------------
-- amadeus_parent_child
-- Holds the parent-child subsidiary data and NAICS sector codes for the Amadeus firm financials data
-- bvdep_id: the Amadeus unique firm ID
-- country: country of register
-- company_name: the Amadeus firm name
-- naics_2007: the 2007 NAICS core code
-- is_bvdep_id: the Amadeus ID of the firm's immediate shareholder
-- d_uo_bvdep_id: the Amadeus ID of the firm's ultimate domestic shareholder
-- g_uo_bvdep_id: the Amadeus ID of the firm's ultimate global shareholder
-- lat: geocoded latitude of firm's listed address
-- long: geocoded longitude of firm's listed address
-- ---------------------------

DROP TABLE IF EXISTS `amadeus_parent_child`;

CREATE TABLE `amadeus_parent_child` (

  bvdep_id varchar(10) NOT NULL default '0',

  country varchar(2) NOT NULL default '',

  company_name text NOT NULL,

  lat float(8),

  long float(8),

  naics_2007 int(4),
  
  is_bvdep_id varchar(10),
  
  d_uo_bvdep_id varchar(10),

  g_uo_bvdep_id varchar(10),

  PRIMARY KEY  (bvdep_id),

  INDEX (is_bvdep_id), 

  KEY (is_bvdep_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;

