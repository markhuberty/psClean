`dedupe` process logic
======================


`generate_bash_dedupe.py` will take as input the directory
containing the input files for dedupe, and output a bash script to run
the disambiguation engine on each. See `patstat_dedupe.py` for specific
documentation of the disambiguation process itself.

The resulting bash file will invoke the training / blocking /
disambiguation for each country in turn. If one country fails for any
reason, it does not prevent processing on subsequent countries from proceeding.

