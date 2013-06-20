-- This script creates a MySQL database named patstatOct2011

-- by defining a new set of tables and indexes.  


-- Version March2012     
-- Author Mark Huberty
-- Date 6 March 2012

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



CREATE TABLE  `tls201_appln` (
  

`appln_id` int(10) NOT NULL default '0',
  

`appln_auth` char(2) NOT NULL default '',
  

`appln_nr` char(15) NOT NULL default '',
  

`appln_kind` char(2) NOT NULL default '00',
  

`appln_filing_date_str` char(10),--  date default NULL,
  

`ipr_type` char(2) NOT NULL default '',
  

`appln_title_lg` char(2) NOT NULL default '',
  

`appln_abstract_lg` char(2) NOT NULL default '',

`internat_appln_id` int(10) NOT NULL default '0',

PRIMARY KEY  (`appln_id`),
  

KEY `internat_appln_id` (`internat_appln_id`),
  

KEY `appln_auth` (`appln_auth`,`appln_nr`,`appln_kind`),
  

KEY `appln_filing_date` (`appln_filing_date`)
) 

ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls202_appln_title`

-- 



CREATE TABLE `tls202_appln_title` (

  appln_id int(10) NOT NULL default '0',

  appln_title text NOT NULL,

  PRIMARY KEY  (appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls203_appln_abstr`

--



CREATE TABLE `tls203_appln_abstr` (

  appln_id int(10) NOT NULL default '0',

  appln_abstract text NOT NULL,

  PRIMARY KEY  (appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls204_appln_prior`

-- 



CREATE TABLE `tls204_appln_prior` (

  appln_id int(10) NOT NULL default '0',

  prior_appln_id int(10) NOT NULL default '0',

  prior_appln_seq_nr smallint(4) NOT NULL default '0',

  PRIMARY KEY  (appln_id,prior_appln_id),

  INDEX (prior_appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls205_tech_rel`

-- 



CREATE TABLE `tls205_tech_rel` (

  appln_id int(10) NOT NULL default '0',

  tech_rel_appln_id int(10) NOT NULL default '0',

  PRIMARY KEY  (appln_id,tech_rel_appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls206_person`

-- 
-- NOTE: there are backslashed escapes in person_name
-- Result: need to load the table with sql_mode='NO_BACKSLASH_ESCAPES'


CREATE TABLE `tls206_person` (

  person_id int(10) NOT NULL default '0',

  person_ctry_code varchar(2) NOT NULL default '',

  doc_std_name_id int(10) NOT NULL default '0',

  person_name text NOT NULL,

  person_address text NOT NULL,

  PRIMARY KEY  (person_id),

  INDEX (person_ctry_code)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls207_pers_appln`

-- 



CREATE TABLE `tls207_pers_appln` (

  person_id int(10) NOT NULL default '0',

  appln_id int(10) NOT NULL default '0',

  applt_seq_nr smallint(4) NOT NULL default '0',

  invt_seq_nr smallint(4) NOT NULL default '0',

  PRIMARY KEY  (person_id,appln_id),

  INDEX (person_id),

  INDEX (appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls208_doc_std_nms`

-- 



CREATE TABLE `tls208_doc_std_nms` (

  doc_std_name_id int(10) NOT NULL default '0',

  doc_std_name varchar(150) NOT NULL default '',

  PRIMARY KEY  (doc_std_name_id),

  INDEX (doc_std_name)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls209_appln_ipc`

-- 



CREATE TABLE  `tls209_appln_ipc` (
  

`appln_id` int(10) NOT NULL default '0',
  

`ipc_class_symbol` char(15) NOT NULL default '',
  

`ipc_class_level` char(1) NOT NULL default '',
  

`ipc_version` date default NULL,
  

`ipc_value` char(1) NOT NULL default '',
  

`ipc_position` char(1) NOT NULL default '',
  

`ipc_gener_auth` char(2) NOT NULL default '',
  

PRIMARY KEY  (`appln_id`,`ipc_class_symbol`,`ipc_class_level`),
  

KEY `ipc_class_symbol` (`ipc_class_symbol`)
) 

ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls210_appln_n_cls`

-- 



CREATE TABLE `tls210_appln_n_cls` (

  appln_id int(10) NOT NULL default '0',

  nat_class_symbol char(15) NOT NULL default '',

  PRIMARY KEY  (appln_id,nat_class_symbol)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls211_pat_publn`

-- 



CREATE TABLE  `tls211_pat_publn` (
  

`pat_publn_id` int(10) NOT NULL default '0',
  

`publn_auth` char(2) NOT NULL default '',
  

`publn_nr` char(15) NOT NULL default '',
  

`publn_kind` char(2) NOT NULL default '',
  

`appln_id` int(10) NOT NULL default '0',
  

`publn_date` date default NULL,
  

`publn_lg` char(2) NOT NULL default '',


`publn_first_grant` int(1) NOT NULL default '0', 


`publn_claims` smallint NOT NULL default '0',
    

PRIMARY KEY  (`pat_publn_id`),
  

KEY `publn_auth` (`publn_auth`,`publn_nr`,`publn_kind`),
  

KEY `appln_id` (`appln_id`),
  

KEY `publn_date` (`publn_date`)
) 

ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls212_citation`

-- 


CREATE TABLE `tls212_citation` (

  pat_publn_id int(10) NOT NULL default '0',

  citn_id smallint(4) NOT NULL default '0',

  cited_pat_publn_id int(10) NOT NULL default '0',

  npl_publn_id int(10) NOT NULL default '0',

  pat_citn_seq_nr smallint(4) NOT NULL default '0',

  npl_citn_seq_nr smallint(4) NOT NULL default '0',

  citn_origin char(5) NOT NULL default '',

  cited_appln_id int(10) NOT NULL default '0',

  cited_gener_auth char(2) NOT NULL default '',

  PRIMARY KEY  (pat_publn_id, citn_id),

  INDEX (cited_pat_publn_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8 PACK_KEYS=0;



-- -----------------------------------------------------------



-- 

-- Table `tls214_npl_publn`

-- 



CREATE TABLE `tls214_npl_publn` (

  npl_publn_id int(10) NOT NULL default '0',
  
  npl_biblio text NOT NULL,
  
  PRIMARY KEY  (npl_publn_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls215_citn_categ`

-- 



CREATE TABLE `tls215_citn_categ` (

  pat_publn_id int(10) NOT NULL default '0',

  citn_id smallint(4) NOT NULL default '0',

  citn_categ char(1) NOT NULL default '',

  PRIMARY KEY  (pat_publn_id,citn_id,citn_categ)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls216_appln_contn`

-- 



CREATE TABLE `tls216_appln_contn` (

  appln_id int(10) NOT NULL default '0',

  parent_appln_id int(10) NOT NULL default '0',

  contn_type char(3) NOT NULL default '',

  PRIMARY KEY  (appln_id,parent_appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;





-- -----------------------------------------------------------

-- 

-- Table `tls217_appln_i_cls`

-- 




CREATE TABLE `tls217_appln_ecla` (

  appln_id int(10) NOT NULL default '0',

  epo_class_auth char(2) NOT NULL,

  epo_class_scheme char(4) NOT NULL,

  epo_class_symbol char(50) NOT NULL,

  PRIMARY KEY  (appln_id,epo_class_auth,epo_class_scheme, epo_class_symbol)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;




-- -----------------------------------------------------------

-- 

-- Table `tls218_docdb_fam`

-- 



CREATE TABLE `tls218_docdb_fam` (

  appln_id int(10) NOT NULL default '0',

  docdb_family_id int(10) NOT NULL default '0',

  PRIMARY KEY  (appln_id,docdb_family_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;



-- -----------------------------------------------------------



-- 

-- Table `tls219_inpadoc_fam`

-- 



CREATE TABLE `tls219_inpadoc_fam` (

  appln_id int(10) NOT NULL default '0',

  inpadoc_family_id int(10) NOT NULL default '0',

  PRIMARY KEY  (appln_id)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;






-- -- NOTE: INCLUDED HERE FOR PLACEHOLDER. NOT IN OCT2011 DVD dump

-- -- Table `tls221_inpadoc_prs`

-- -- 



-- CREATE TABLE `tls219_inpadoc_fam` (

--   appln_id int(10) NOT NULL default '0',

--   prs_event_seq_n smallint NOT NULL default '0',

--   prs_agzette_date date NOT NULL default '9999-12-31',

--   prs_code char(4) NOT NULL default '',
--   inpadoc_family_id int(10) NOT NULL default '0',

--   PRIMARY KEY  (appln_id)

-- ) ENGINE=MyISAM DEFAULT CHARSET=utf8;


-- -----------------------------------------------------------



-- 

-- Table `tls222_apln_jp_class`

-- 



CREATE TABLE `tls222_appln_jp_class` (

  appln_id int(10) NOT NULL default '0',

  jp_class_scheme char(5) NOT NULL,
  
  jp_class_symbol char(50) NOT NULL,

  PRIMARY KEY  (appln_id,jp_class_scheme,jp_class_symbol)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;


-- -----------------------------------------------------------



-- 

-- Table `tls223_appln_docus`

-- 



CREATE TABLE `tls223_appln_docus` (

  appln_id int(10) NOT NULL default '0',

  docus_class_symbol char(50) NOT NULL,

  PRIMARY KEY  (appln_id,docus_class_symbol)

) ENGINE=MyISAM DEFAULT CHARSET=utf8;
