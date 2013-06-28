library(ggplot2)
df <- read.csv("../data/sampled_dedupe_leuven_patent_counts.csv")


plot.summary <- ggplot(df,
                       aes(x=log10(ref_ct),
                           y=log10(dedupe_ct)
                           )
                       ) +
  geom_abline(intercept=0, slope=1, linetype=2, alpha=0.5) +
  geom_point(size=1, alpha=0.5) +
  geom_smooth(method="lm") +
  facet_wrap(~ country) +
  scale_x_continuous("Patents per Leuven ID (log scale)") +
  scale_y_continuous("Patents per Dedupe ID (log scale)")
print(plot.summary)
ggsave(plot.summary,
       file="../figures/plot_patents_per_id.pdf",
       width=7,
       height=7
       )
