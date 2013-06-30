CITL output data formats
===========


`citl_patstat_matches.csv` contains the output of the name matching classifier. The output fields are defined as follows:

citl_name,citl_id,patstat_name,patstat_id,lev_name_dist,jac_name_dist,geo_dist,country

- `citl_name`: the name given to the firm in the CITL registry
- `citl_id`: the accountholder ID corresponding to that name
- `patstat_name`: the standardized PATSTAT name assigned in the disambiguation process
- `patstat_id`: the unique PATSTAT cluster ID for this name
- `lev_name_dist`: the Levenshtein distance between the CITL and PATSTAT names
- `jac_name_dist`: the Jaccard token distance between the CITL and PATSTAT names
- `geo_dist`: the computed Haversine great-circle distance between the PATSTAT and CITL records; available if both had valid latitude and longitude data
- `country`: the country of origin for the PATSTAT and CITL records

