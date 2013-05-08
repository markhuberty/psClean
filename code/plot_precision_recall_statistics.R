library(ggplot2)
library(reshape)
setwd("~/projects/psClean")
df.pr <- read.csv("./data/patstat_country_precision_recall.csv")
df.id <- read.csv("./data/patstat_dedupe_id_counts.csv")
df.id <- unique(df.id[,c("X", "patstat", "round1", "round2")])
df.names <- read.csv("./data/patent_inventor_shares.csv")

## Compute summary stats on the name data
name.max <- apply(df.names, 2, max, na.rm=TRUE)
df.name.max <- data.frame(names(name.max), name.max)
names(df.name.max) <- c("country", "max.share")
df.name.max$country <- factor(df.name.max$country,
                               levels=df.name.max$country[order(df.name.max$max.share)]
                               )

plot.name.max <- ggplot(df.name.max,
                         aes(x=country,
                             y=max.share
                             )
                         ) +
  geom_point() +
  scale_x_discrete("Country") +
  scale_y_continuous("Max patent share per person_id")
print(plot.name.max)
ggsave(plot.name.max,
       file="./figures/patstat_max_name_share.pdf",
       height=7,
       width=7
       )

## Bootstrap the mean / variance patent share data
bs.mean <- function(vec, n.draws=1000){
  vec = sort(vec, decreasing=TRUE)[1:100]
  means <- sapply(1:n.draws, function(x){

    sample.vec <- vec[sample(1:length(vec), length(vec), replace=TRUE)]
    out <- mean(sample.vec, na.rm=TRUE)

  })

  out <- c(mean(means), quantile(means, probs=c(0.025, 0.975)))
  return(out)

}

name.means <- apply(df.names, 2, function(x) bs.mean(x))

df.name.means <- data.frame(t(name.means))
colnames(df.name.means) <- c("mean.share", "ci2.5", "ci97.5")
df.name.means$country <- rownames(df.name.means)
df.name.means$country <- factor(df.name.means$country,
                                levels=df.name.means$country[order(df.name.means$mean.share)])

plot.name.means <- ggplot(df.name.means,
                          aes(x=country,
                              y=mean.share,
                              ymin=ci2.5,
                              ymax=ci97.5
                              )
                          ) +
  geom_pointrange() +
  scale_x_discrete("Country") +
  scale_y_continuous("Mean share of national patents for the top 100 inventors")
print(plot.name.means)
ggsave(plot.name.means,
       file="./figures/patstat_mean_top_innovator_share.pdf",
       height=7,
       width=7
       )

## Generate tables and plots for the unique ID reduction data
df.id$round1 <- df.id$round1 / df.id$patstat
df.id$round2 <- df.id$round2 / df.id$patstat
df.id$patstat <- df.id$patstat / df.id$patstat
names(df.id) <- c("Country", "PATSTAT", "Dedupe level 1", "Dedupe level 2")

df.id <- melt(df.id, id.vars="Country")
df.id$label <- ifelse(df.id$variable == "Dedupe level 2",
                      as.character(df.id$Country),
                      ""
                      )


plot.id.reduction <- ggplot(df.id,
                            aes(x=variable,
                                y=value,
                                group=Country,
                                label=label
                                )
                            ) +
  geom_line(alpha=0.5, linetype=2) +
  geom_text(vjust=1) +
  scale_x_discrete("Disambiguation step") +
  scale_y_continuous("Unique ID count as pct. of PATSTAT")
print(plot.id.reduction)
ggsave(plot.id.reduction,
       file="./figures/dedupe_id_reduction.pdf",
       width=7,
       height=7
       )

## Generate tables and plots for the precision/recall data
library(xtable)

df.pr.melt <- melt(df.pr, id.vars=c("X", "cluster_label"))
df.pr.melt$type <- ifelse(grepl("recall", df.pr.melt$variable),
                          "recall",
                          "precision"
                          )
df.pr.melt$val <- gsub("_[a-z]+$", "", df.pr.melt$variable)
df.pr.cast <- cast(df.pr.melt, X + val + cluster_label ~ type)
df.pr.cast$val <- factor(df.pr.cast$val)
levels(df.pr.cast$val) <- c("Person ID - Leuven level 2", "Patent - Leuven level 1", "Patent - Leuven level 2")


df.pr.cast$group <- paste(df.pr.cast$X, df.pr.cast$val, sep=".")
levels(df.pr.cast$cluster_label) <- c("Round 1", "Round 2")
df.pr.cast$country.label <- ifelse(df.pr.cast$cluster_label == "Round 2",
                                   as.character(df.pr.cast$X),
                                   ""
                                   )

plot.pr <- ggplot(df.pr.cast,
                  aes(y=precision,
                      x=recall,
                      label=country.label,
                      group=group
                      )
                  ) +
  geom_line(alpha=0.5, linetype=2) +
  geom_point(aes(shape=cluster_label)) +
  geom_text(size=4, hjust=-0.75, vjust=0.75) +
  scale_shape("Disambiguation stage", guide=guide_legend(direction="horizontal")) +
  facet_grid(val ~ ., scales="free") +
  scale_y_continuous("Precision") +
  scale_x_continuous("Recall") +
  theme(legend.position="top",
        strip.text.y=element_text(angle=0)
        )
print(plot.pr)
ggsave(plot.pr,
       file="./figures/dedupe_precision_recall.pdf",
       width=7,
       height=8
       )


df.pr <- df.pr[df.pr$cluster_label == "cluster_id_r1",]
df.pr <- df.pr[,-2]

# assumes be, it, fr, es, nl, dk, fi
r.weight <- "(1, 2.5)"
p.weight <- "(0.5, 1.5)"
weights.vec <- c(r.weight, p.weight, p.weight, p.weight, r.weight, r.weight, r.weight)

df.pr$pr.wt <- weights.vec
names(df.pr) <- c("Country", "ID precision", "ID recall", "L1 patent precision",
                  "L1 patent recall", "L2 patent precision", "L2 patent recall", "Precision-Recall weights"
                  )

tab.pr <- xtable(df.pr,
                 label="tab:pr",
                 caption="Precision and recall results by country for the \\texttt{dedupe} output. ID values measure the person-level performance, relative to hand-matched Leuven Level 2 results. Patent data measure the accuracy of assignment of patents to unique individuals. Results are shown for comparison with both the Leuven level 1 (L1) and hand-matched level 2 (L2) datasets. Precision-recall weights refer to the settings used for rounds 1 and 2 of disambiguation for each country."
                 )
print(tab.pr,
      file="./tables/tab_precision_recall.tex",
      size="footnotesize"
      )
