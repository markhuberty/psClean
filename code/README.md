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

4. `./postprocess/map_dedupe_han_leuven.py`
Takes as input the disambiguated PATSTAT data and merges it with the HAN and Leuven disambiguated datasets for comparison.

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
The raw PATSTAT data can be obtained from the European Patent Office. Data for the Community Independent Transaction Log (CITL) can be obtained from the [European Commission](http://ec.europa.eu/environment/ets/). 
We make use of the following data:

1. PATSTAT: `person_name`, `person_address`, `person_id`, `person_ctry_code`, `ipc_class_symbol`, `appln_id`, `appln_filing_date`

2. Leuven: `person_id`, `hrm_level`, `hrm_l2_id`, `person_name`, `person_ctry_code`

The KU Leuven [harmonized names data](http://www.ecoom.be/en/EEE-PPAT) builds on HAN with additional automated and manual disambiguation. We use the KU-Leuven hand-disambiguated data as a comparison set for computing precision and recall data for our methods. 

3. HAN: `HAN_ID`, `OCT11_Person_id`, `Person_name_clean`, `Person_ctry_code`

The OECD Harmonized Applicants' Name ([HAN](http://www.oecd.org/sti/inno/oecdpatentdatabases.htm)) database contains supplemental tables for PATSTAT which attempt a straightforward machine-based disambiguation.

4. CITL: `accountholder`, 

The [Community Independent Transaction Log](http://ec.europa.eu/environment/ets/) reports regulated firms under the European Emissions Trading Scheme. We match our unique PATSTAT inventors to regulated CITL firms. 

5. Amadeus

The [Amadeus](https://amadeus.bvdinfo.com/version-2013617/home.serv?product=amadeusneo) database provides financial and corporate structure data for many European firms. We match Amadeus firm data to both PATSTAT and CITL. 


Other logic
------------------------

We ultimately produce a PATSTAT-Amadeus-CITL lookup table. Two additional directories contain the logic used to do so.

1. The `citl` folder contains all logic used to match CITL inventors to the Amadeus database

2. The `amadeus` folder contains all logic used to match PATSTAT unique inventors to the Amadeus financial database
