README
=================

Introduction
---------------

This directory contains the routines to extract, clean, and disambiguate inventor data in the PATSTAT database. 


Order of operations
-------------------

`psCleanup` assumes that you have PATSTAT loaded into a SQL database. The *sql* folder provides the scripts used to create and load tables for the October 2011 PATSTAT release. It may or may not work for future releases, depending on changes applied by the European Patent Office. 

We assume the following order of operations:

1. `./extract/extract_patstat_data.py`
Queries the PATSTAT SQL database for inventors and inventor characteristics; performs preliminary cleaning and standardization.

2. `./clean/prepare_dedupe_input.py`
Formats extracted PATSTAT data for use in the dedupe process.

3. `./dedupe/<country_code>_weighted.py`
Disambiguates country-level PATSTAT inventor files and returns a disambiguated record.

4. `./analyze/compute_precision_recall.py`
Computes the precision and recall data for each country file and returns useful tables.

Note that the present configuration of `psCleanup` does not write back into the SQL database; this is a relic of debugging and should be fixed in the future. 



Dependencies
--------------------

All code here has been tested on a Linux system (specifically
Ubuntu 10.04 or higher). We make no promises about how the default
configurations will work in other environments.

### Universal dependencies
- pandas
- numpy

### Data extract dependencies
- iPython v0.13 or higher (for parallelization of cleaning on multi-core machines)

### Data cleaning and formatting dependencies
- unidecode

### Disambiguation dependencies
- [dedupe](https://github.com/open-city/dedupe) and its dependencies


Data
---------------
The raw PATSTAT data can be obtained from the European Patent Office. Data for the Community Independent Transaction Log (CITL) can be obtained from the [European Commission](http://ec.europa.eu/environment/ets/). Company financial and subsidiary data can be obtained from the [Amadeus](https://amadeus.bvdinfo.com/version-2013617/home.serv?product=amadeusneo) database. 

We make use of the following data:

1. PATSTAT: `person_name`, `person_address`, `person_id`, `person_ctry_code`, `ipc_class_symbol`, `appln_id`, `appln_filing_date`

2. CITL: `accountholder`, 

Other logic
------------------------

1. The `citl` folder
