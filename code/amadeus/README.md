Amadeus analysis code
===============


The file is specific to the `./code/amadeus` directory as contained in the *amadeus_prob_dist* branch of the psClean repository. That branch is still research in progress and hence is far less formalized than the CITL and PATSTAT clean/disambiguate workflows. This README servers to document that code as it currently exists. 


Code to write the Amadeus data into a SQL table in the PATSTAT database
---------------

1. `write_amadeus_sql_table.py`. This will write the Amadeus data into the PATSTAT database; it assumes the table already exists. See the `./code/sql` folder for the necessary SQL code to create the table.

Code to consolidate the firm ownership hierarchy
-----------

1. `assign_parent_child_naics.py`: recursively walks through the Amadeus parent:child relationships and accumulates all Amadeus IDs tied to the same firm structure into a group. Counts the NAICS codes as assigned to those IDs. Assigns the most frequent NAICS code to the firms, and writes it back into the Amadeus table in the database. 

Code to map from IPC to NAICS sectors
-------------------


1. TBD

Code to construct the coauthor network and propagate sector labels
---------------

1. `count_nl_coauthors.py`: queries the coauthor structure from the database, using IDs as assigned by dedupe, and outputs a table of ID:ID matches between coauthors 

2. `build_nl_coauthor_network.py`: Takes a coauthor edgelist, defined as a ID:ID map between coauthors, and builds an undirected graph from the output. It then does a variety of operations on that graph, including community detection and analysis of the largest connected component of the graph

3. `nl_coauthor_network_label_propagation.py`: attempts to assign NAICS codes to NL firms with a simple label propagation algorithm, starting from a small set of labeled nodes. 


Code to map Amadeus to PATSTAT using only name and geo data
---------------------

1. `cat_amadeus_patstat_levenstrein.py`: Performs the same function as the `citl_levenstein.py` code: reads in country-specific files for both the Amadeus data and the PATSTAT data, filters the PATSTAT data for likely firms, and then finds the top 3 most likely Amadeus matches for each PATSTAT firm. Outputs results as a single file for all countries. 

2. `company_legal_ids.csv`: a table of company legal IDs and their abbreviations, for use in filtering for likely firms in PATSTAT

3. `classify_amadeus_patstat_matches.py`: takes the output from the `cat...` code. Requests that the user hand-label a set of possible matches. Then trains a SVC classifier on the labeled data and applies that classifier to all matches. Outputs the set of actual matches from the large set of possible matches. 


Other
-----------------

1. `parse_legal_ids.py`: reads a list of firm legal identifiers from a website, and parses the XML output to return a set of actual IDs and their abbreviations
