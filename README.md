psClean
=======

Python library and associated code for preparing PATSTAT inventor-patent data for disambiguation with either the [Torvik-Smallheiser](https://github.com/markhuberty/fung_disambiguator) or [Open City Dedupe](https://github.com/markhuberty/dedupe) algorithms. 

[PATSTAT](http://www.epo.org/searching/subscription/raw/product-14-24.html) provides multicountry patent data files. However, the inventor and patent owner data requires significant cleaning and disambiguation. psClean will provide open source tools to standardize and geocode inputs via the following workflow:

1. Loading the PATSTAT data to a MySQL database
2. Querying patent-inventor records
3. Cleaning those records, including case standardization, diacritic removal, and unicode handling
4. Geocoding inventor locales
5. Consolidating inventor records for inventor disambiguation

This work is part of a broader project, funded in part by the European Union FP7 Programme. 

*NOTE*: The `devel` branch is research code at present. Adapt at your own risk. We will eventually publish a more coherent workflow for moving from a clean PATSTAT MySQL database to input files suitable for the disambiguation algorithm. 
