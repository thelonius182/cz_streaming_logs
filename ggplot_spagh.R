# Libraries
library(tidyverse)
library(hrbrthemes)
library(kableExtra)
options(knitr.table.format = "html")
library(babynames)
library(streamgraph)
library(viridis)
library(DT)
library(plotly)

# Load dataset from github
data <- babynames %>% 
  filter(name %in% c("Mary","Emma", "Ida", "Ashley", "Amanda", "Jessica",    "Patricia", "Linda", "Deborah",   "Dorothy", "Betty", "Helen")) %>%
  filter(sex=="F")

# Plot
data %>%
  ggplot( aes(x=year, y=n, group=name, color=name)) +
  geom_line() +
  scale_color_viridis(discrete = TRUE) +
  theme(
    legend.position="none",
    plot.title = element_text(size=14)
  ) +
  ggtitle("A spaghetti chart of baby names popularity") +
  theme_ipsum()

data %>%
  mutate( highlight=ifelse(name=="Amanda", "Amanda", "Other")) %>%
  ggplot( aes(x=year, y=n, group=name, color=highlight, size=highlight)) +
  geom_line() +
  scale_color_manual(values = c("#69b3a2", "lightgrey")) +
  scale_size_manual(values=c(1.5,0.2)) +
  theme(legend.position="none") +
  ggtitle("Popularity of American names in the previous 30 years") +
  theme_ipsum() +
  geom_label( x=1990, y=55000, label="Amanda reached 3550\nbabies in 1970", size=4, color="#69b3a2") +
  theme(
    legend.position="none",
    plot.title = element_text(size=14)
  )

data %>%
  ggplot( aes(x=year, y=n, group=name, fill=name)) +
  geom_area() +
  scale_fill_viridis(discrete = TRUE) +
  theme(legend.position="none") +
  ggtitle("Popularity of American names in the previous 30 years") +
  theme_ipsum() +
  theme(
    legend.position="none",
    panel.spacing = unit(0.1, "lines"),
    strip.text.x = element_text(size = 8),
    plot.title = element_text(size=14)
  ) +
  facet_wrap(~name)

tmp <- data %>%
  mutate(name2=name)

tmp %>%
  ggplot( aes(x=year, y=n)) +
  geom_line( data=tmp %>% dplyr::select(-name), aes(group=name2), color="grey", size=0.5, alpha=0.5) +
  geom_line( aes(color=name), color="#69b3a2", size=1.2 )+
  scale_color_viridis(discrete = TRUE) +
  theme_ipsum() +
  theme(
    legend.position="none",
    plot.title = element_text(size=14),
    panel.grid = element_blank()
  ) +
  ggtitle("A spaghetti chart of baby names popularity") +
  facet_wrap(~name)
