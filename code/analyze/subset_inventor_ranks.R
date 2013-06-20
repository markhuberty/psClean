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

input.dir <- "./dedupe/"
input.root <- "dedupe_leuven_patent_counts_r"
iter <- 0
n.rows <- 10000

for(cdx in 1:length(eu27)){
  country = eu27[cdx]
  country.input <- paste(input.dir,
                         country,
                         "_weighted/",
                         input.root,
                         sep=""
                         )
  print(country.input)
  for(i in 1:1){

    input.file <- paste(country.input, i, ".csv", sep="")
    print(input.file)
    if(file.exists(input.file)){
      df = read.csv(input.file, stringsAsFactors=FALSE)
      df.sub <- df[df$ref_ct > 1, 4:ncol(df)]

      ## Down-sample to 10k rows if necessary
      if(nrow(df.sub) > n.rows){

        df.sub <- df.sub[sample(1:nrow(df.sub), n.rows,replace=FALSE), ]

      }

      df.sub$country <- country
      df.sub$round <- i

      if(iter == 0){
        df.out <- df.sub
        iter <- iter + 1
      }else{
        df.out <- rbind(df.out, df.sub)
      }

    }else{
      next
    }

  }

}

write.csv(df.out, file="../data/sampled_dedupe_leuven_patent_counts.csv", row.names=FALSE)
