eu27 <- c('at',
          'bg',
          'be',
          'it',
          'gb',
          'fr',
          'de',
          'sk',
          'se',
          'pt',
          'pl',
          'hu',
          'ie',
          'ee',
          'es',
          'cy',
          'cz',
          'nl',
          'si',
          'ro',
          'dk',
          'lt',
          'lu',
          'lv',
          'mt',
          'fi',
          'el'
          )

input.dir <- "../../data/dedupe_script_post/"
input.root <- "dedupe_leuven_patent_counts"
iter <- 0
n.rows <- 10000

for(cdx in 1:length(eu27)){
  country = eu27[cdx]
  country.input <- paste(input.dir,
                         country,
                         "_",
                         input.root,
                         sep=""
                         )
  print(country.input)

  input.file <- paste(country.input, ".csv", sep="")
  print(input.file)
  if(file.exists(input.file)){
    df = read.csv(input.file, stringsAsFactors=FALSE)
    df.sub <- df[df$ref_ct > 1, 4:ncol(df)]
    
    ## Down-sample to 10k rows if necessary
    if(nrow(df.sub) > n.rows){
      
      df.sub <- df.sub[sample(1:nrow(df.sub), n.rows,replace=FALSE), ]
      
    }
    
    df.sub$country <- country
    

    if(cdx == 1){
      df.out <- df.sub

    }else{
      df.out <- rbind(df.out, df.sub)
    }

  }else{
    next
  }

}



write.csv(df.out, file="../../data/sampled_dedupe_leuven_patent_counts.csv", row.names=FALSE)
