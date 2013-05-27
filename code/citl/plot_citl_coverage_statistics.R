library(ggplot2)
library(xtable)
library(plyr)

to.numeric <- function(x) as.numeric(as.character(x))
compute.coverage <- function(df){

  ct.match <- table(df$country.y)
  ct.all <- table(df$country.x)
  df.ct.match <- data.frame(ct.match)
  names(df.ct.match) <- c("country", "ct.match")
  df.ct.all <- data.frame(ct.all)
  names(df.ct.all) <- c("country", "ct.all")
  df.pct <- merge(df.ct.match, df.ct.all, by="country", all=TRUE)
  df.pct$pct.match <- to.numeric(df.pct$ct.match) / to.numeric(df.pct$ct.all)
  return(df.pct)

}

setwd("../../")
citl.candidates <- read.csv("./code/citl/citl_patstat_matches.csv")
citl.matches <- read.csv("./code/citl/predicted_citl_matches.csv")
citl.raw <- read.csv("./data/citl/citl_eu27.csv")
citl.raw$countrycode <- tolower(citl.raw$countrycode)

levels(citl.raw$mainactivitytypecodelookup) <- c("Aircraft",
                                                 "Coke ovens",
                                                 "Combustion, > 20MW",
                                                 "Combustion",
                                                 "Pulp and paper",
                                                 "Raw ceramics and brick",
                                                 "Glass and glass fibre",
                                                 "Cement clinker",
                                                 "Pig iron and steel",
                                                 "Ceramic products",
                                                 "Metal ore",
                                                 "Oil refineries",
                                                 "Other",
                                                 "Bulk chemicals",
                                                 "Carbon black",
                                                 "Ferrous metals",
                                                 "Gypsum and plaster"
                                                 )

unique.candidates <- unique(citl.candidates[,c("citl_name", "citl_id", "country")])
unique.matches <- unique(citl.matches[,c("citl_name", "citl_id", "country", "patstat_name", "patstat_id")])

all.citl <- merge(unique.candidates, unique.matches,
                  by=c("citl_name", "citl_id"), all.x=TRUE, all.y=TRUE
                  )

all.citl <- merge(all.citl, citl.raw,
                  by.x=c("citl_id", "country.x"), by.y=c("installationidentifier", "countrycode"),
                  all.x=TRUE,
                  all.y=FALSE
                  )


## Compute the overall match percentage by country
df.pct.match <- compute.coverage(all.citl)

# df.pct.match <- data.frame(pct.match.bycountry)
# names(df.pct.match) <- c("country", "pct.match")
df.pct.match$pct.match[is.na(df.pct.match$pct.match)] <- 0
df.pct.match$country <- toupper(df.pct.match$country)
df.pct.match$country <- factor(df.pct.match$country,
                               levels=df.pct.match$country[order(df.pct.match$pct.match)]
                               )

plot.pct.match <- ggplot(df.pct.match,
                         aes(x=pct.match,
                             y=country
                             )
                         ) +
  geom_point() +
  scale_y_discrete("Country") +
  scale_x_continuous("Proportion of CITL names w. PATSTAT match")
print(plot.pct.match)
ggsave("./figures/citl_match_pct_overall.pdf",
       width=7,
       height=7
       )

## Then compute overall match percentage by country and installation
## type


pct.match.bytype <- ddply(.data=all.citl, .variables=c("country.x", "mainactivitytypecodelookup"),
              .fun=compute.coverage
              )
pct.match.bytype$country <- toupper(pct.match.bytype$country)

plot.pct.match.bytype <- ggplot(pct.match.bytype,
                                aes(y=mainactivitytypecodelookup,
                                    x=pct.match
                                    )
                                ) +
  geom_point() +
  facet_wrap(~ country) +
  scale_x_continuous("Proportion of CITL names w. PATSTAT match") +
  scale_y_discrete("CITL activity description") +
  opts(axis.text.y=theme_text(size=5), axis.text.x=theme_text(size=8, angle=-90, vjust=0))
print(plot.pct.match.bytype)
ggsave(plot.pct.match.bytype,
       file="./figures/citl_match_pct_bytype.pdf",
       width=7,
       height=7
       )

mean.pct.match.bytype <- tapply(pct.match.bytype$pct.match,
                                pct.match.bytype$mainactivitytypecodelookup,
                                mean,
                                na.rm=TRUE
                                )
df.pct.match.bytype <- data.frame(mean.pct.match.bytype)
df.pct.match.bytype$Sector <- rownames(df.pct.match.bytype)
df.pct.match.bytype <- df.pct.match.bytype[,2:1]
names(df.pct.match.bytype) <- c("Sector", "Pct. Match to PATSTAT")
tab.pct.match.bytype <- xtable(df.pct.match.bytype,
                               label="tab:pct-match-bytype",
                               caption="Percentatge of CITL installations by sector with positive PATSTAT matches. Results shown for the EU-25, Cyprus and Malta omitted",
                               digits=2,
                               )

print.xtable(tab.pct.match.bytype,
             file="./tables/pct_patstat_citl_match_bytype.tex",
             include.rownames=FALSE,
             NA.string="0"
             )
