library(ggplot2)
library(xtable)
library(reshape)

df <- read.csv("../data/eu27_dedupe_leuven_patent_ct_corr.csv",
               stringsAsFactors=FALSE
               )
pearson <- strsplit(df$pearson, ", ")
pearson <- sapply(pearson, function(x) as.numeric(gsub("\\(", "", x[1])))
spearman <- strsplit(df$spearman, ", ")
spearman <- sapply(spearman, function(x) as.numeric(gsub("\\(", "", x[1])))

df$pearson <- pearson
df$spearman <- spearman

tab.corr <- df[,3:ncol(df)]
names(tab.corr) <- c("Country", "Pearson", "Spearman")
tab.corr$Country <- toupper(tab.corr$Country)
tab.corr <- tab.corr[order(tab.corr$Country),]
xtab.corr <- xtable(tab.corr,
                    label="tab:patent_count_corr",
                    caption="Correlation between patent counts assigned to matching Leuven and Dedupe IDs."
                    )
print.xtable(xtab.corr,
             digits=2,
             file="../tables/tab_corr_leuven_dedupe_patent_counts.tex",
             include.rownames=FALSE
             )


df.melt <- melt(df[,2:ncol(df)])

levels(df.melt$variable) <- c("Pearson", "Spearman")
#levels(df.melt$cluster) <- c("Round 1", "Round 2")

plot.corr <- ggplot(df.melt,
                    aes(y=country,
                        x=value,
                        shape=variable
                        )
                    ) +
  geom_point()
print(plot.corr)
## plot.corr <- ggplot(df.melt,
##                     aes(y=country,
##                         x=value,
##                         shape=cluster
##                         )
##                     ) +
##   geom_point() +
##   facet_wrap(~ variable) +
##   scale_shape("Disambiguation round") +
##   scale_x_continuous("Correlation with Leuven patent counts per ID") +
##   scale_y_discrete("Country")
## print(plot.corr)
## ggsave(plot.corr,
##        file="../figures/plot_dedupe_leuven_count_correlation.pdf",
##        width=7,
##        height=7
##        )
