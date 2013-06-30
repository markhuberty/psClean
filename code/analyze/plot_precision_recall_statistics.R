library(ggplot2)
library(reshape)
library(xtable)

setwd("~/projects/psClean")
df.pr <- read.csv("./data/patstat_country_precision_recall.csv")
df.id <- read.csv("./data/patstat_dedupe_id_counts.csv")
df.id <- unique(df.id[,c("X", "patstat", "round1")])
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
df.id$patstat <- df.id$patstat / df.id$patstat
names(df.id) <- c("Country", "PATSTAT", "Dedupe")

## Tabulate the pct reduction
tab.id <- df.id[c("Country", "Dedupe")]
tab.id$Country = toupper(tab.id$Country)
tab.id$Dedupe = (1 - tab.id$Dedupe) * 100
names(tab.id) <- c("Country", "Pct reduction in unique individuals")

xtab.id <- xtable(tab.id,
                  label="tab:id_pct_reduction",
                  caption="Percentage reduction in unique IDs by country."
                  )
print(xtab.id, digits=1, file="./tables/tab_id_pct_reduction.tex")

## Generate tables and plots for the precision/recall data
df.pr.melt <- melt(df.pr, id.vars=c("X", "cluster_label"))
df.pr.melt$type <- ifelse(grepl("recall", df.pr.melt$variable),
                          "recall",
                          "precision"
                          )
df.pr.melt$val <- gsub("_[a-z]+$", "", df.pr.melt$variable)
df.pr.cast <- cast(df.pr.melt, X + val + cluster_label ~ type)
df.pr.cast$val <- factor(df.pr.cast$val)
levels(df.pr.cast$val) <- c("Person ID - Leuven level 2", "Patent - Leuven level 1", "Patent - Leuven level 2")

plot.pr <- ggplot(df.pr.cast,
                  aes(y=precision,
                      x=recall,
                      group=val,
                      label=X
                      )
                  ) +
  geom_text() +
  scale_x_continuous("Recall") +
  scale_y_continuous("Precision") +
  scale_colour_discrete("Reference unit and dataset") +
  facet_wrap(~ val, scales="fixed")
print(plot.pr)
ggsave(plot.pr,
       file="./figures/dedupe_precision_recall.pdf",
       width=7,
       height=8
       )

df.pr.melt$level <- ifelse(grepl("id", df.pr.melt$variable),
                           "ID",
                           ifelse(grepl("l1", df.pr.melt$variable),
                                  "L1",
                                  "L2"
                                  )
                           )
df.pr.melt$val <- ifelse(grepl("patent", df.pr.melt$val), "patent", "id")
df.pr.cast <- cast(df.pr.melt, X + val + cluster_label + type ~ level)

df.pr.cast$type <- ifelse(df.pr.cast$type=="precision", "Precision", "Recall")

plot.pr.l1.l2 <- ggplot(df.pr.cast,
                        aes(x=L1,
                            y=L2,
                            label=X
                            )
                        ) +
  geom_text(size=4) +
  facet_wrap(~ type, scales="free") +
  scale_x_continuous("Leuven Level 1 Patent") +
  scale_y_continuous("Leuven Level 2 Patent")
print(plot.pr.l1.l2)
ggsave(plot.pr.l1.l2,
       file="./figures/level1_level2_precision_recall.pdf"
       )

df.pr <- df.pr[df.pr$cluster_label == "cluster_id",]
df.pr <- df.pr[,-2]

# assumes be, it, fr, es, nl, dk, fi
# weights.vec <- c(1.5, 4, 1.5, 1.5, 3, 1.5, 1.5)
weights <- c('at'=3,
             'be'=1.25,
             'bg'=1.5,
             'cy'= 1.5,
             'cz'= 1.5,
             'de'=1.5,
             'dk'=1.5,
             'ee'=1.5,
             'el'=1.5,
             'es'=1.5,
             'fi'=1.5,
             'fr'=1.5,
             'gb'=2,
             'hu'=5,
             'ie'=3,
             'it'=4.5,
             'lt'=3,
             'lu'=1,
             'lv'=1.5,
             'mt'=2.0,
             'nl'=3.5,
             'pl'=6,
             'pt'=1.5,
             'ro'=1.5,
             'se'=1.0,
             'si'=1.5,
             'sk'=1.5
             )

df.weights <- data.frame(names(weights), weights)
df.pr <- merge(df.pr, df.weights, by.x="X", by.y="names.weights.")
names(df.pr) <- c("Country", "ID precision", "ID recall", "L1 patent precision",
                  "L1 patent recall", "L2 patent precision", "L2 patent recall", "Precision-Recall weights"
                  )
df.pr$Country <- toupper(df.pr$Country)

tab.pr <- xtable(df.pr,
                 label="tab:pr",
                 caption="Precision and recall results by country for the \\texttt{dedupe} output. ID values measure the person-level performance, relative to hand-matched Leuven Level 2 results. Patent data measure the accuracy of assignment of patents to unique individuals. Results are shown for comparison with both the Leuven level 1 (L1) and hand-matched level 2 (L2) datasets. Countries missing precision and recall data had no corresponding Leuven Level 2 IDs in the dataset."
                 )
print(tab.pr,
      file="./tables/tab_precision_recall.tex",
      size="footnotesize"
      )
