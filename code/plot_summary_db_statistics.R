library(ggplot2)
library(xtable)
library(reshape)

df <- read.csv("../data//patstat_dedupe_summary_statistics_eu27.csv")

# Plot the addresses
plot.addr.ratio <- ggplot(df,
                          aes(x=address_ratio,
                              y=dedupe_addr_ct,
                              label=country
                              )
                          ) +
  geom_text() +
  geom_abline(intercept=0, slope=1, alpha=0.5, linetype=2) +
  coord_cartesian(xlim=c(0,1), ylim=c(0,1)) +
  scale_x_continuous("Share of records with addresses") +
  scale_y_continuous("Share of records with geo-coded addresses")
print(plot.addr.ratio)
ggsave(plot.addr.ratio,
       file="../figures/patstat_dedupe_address_ratio.pdf",
       width=7,
       height=7
       )

df.dedupe <- df[, grepl('dedupe', names(df))]
df.dedupe$country <- df$country
df.dedupe <- df.dedupe[,names(df.dedupe) != "dedupe_record_ct"]

df.dedupe.melt <- melt(df.dedupe)
levels(df.dedupe.melt$variable) <- c("Geo-coded address", "Coauthors", "IPC Codes", "Name")

plot.ratio.line <- ggplot(df.dedupe.melt,
                          aes(x=country,
                              y=value,
                              group=variable,
                              linetype=variable
                              )
                          ) +
  geom_line() +
  scale_x_discrete("Country") +
  scale_y_continuous("Percent complete records") +
  scale_linetype("Field")
print(plot.ratio.line)
ggsave(plot.ratio.line,
       filename="../figures/dedupe_data_summary.pdf",
       width=7,
       height=7
       )
