library(ggplot2)
library(xtable)
library(reshape)

df <- read.csv("../data/eu27_dedupe_leuven_patent_ct_corr_float.csv")
df.melt <- melt(df)

levels(df.melt$variable) <- c("Pearson", "Spearman")
levels(df.melt$cluster) <- c("Round 1", "Round 2")

plot.corr <- ggplot(df.melt,
                    aes(y=country,
                        x=value,
                        shape=cluster
                        )
                    ) +
  geom_point() +
  facet_wrap(~ variable) +
  scale_shape("Disambiguation round") +
  scale_x_continuous("Correlation with Leuven patent counts per ID") +
  scale_y_discrete("Country")
print(plot.corr)
ggsave(plot.corr,
       file="../figures/plot_dedupe_leuven_count_correlation.pdf",
       width=7,
       height=7
       )
