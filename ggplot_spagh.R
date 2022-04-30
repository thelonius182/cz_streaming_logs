# Libraries
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(hrbrthemes))
suppressPackageStartupMessages(library(kableExtra))
suppressPackageStartupMessages(library(streamgraph))
suppressPackageStartupMessages(library(viridis))
suppressPackageStartupMessages(library(DT))
suppressPackageStartupMessages(library(plotly))
suppressPackageStartupMessages(library(yaml))

options(knitr.table.format = "html")
cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# CZ stats ----
cz_stats_report.4f <- read_rds(file = paste0(stats_data_flr(), "cz_stats_report.4f.RDS"))
cz_stats_report.4c <- read_rds(file = paste0(stats_data_flr(), "cz_stats_report.4c.RDS"))

cz_stats_hours <- cz_stats_report.4f %>% bind_rows(cz_stats_report.4c)%>% 
  select(cha_name, hour_of_day, n_dev) %>% 
  filter(!cha_name %in% c("TOTALEN")) %>% 
  ungroup()

tmp <- cz_stats_hours %>%
  mutate(cha_name = if_else(cha_name == "RoD", "Radio on Demand", cha_name),
         cha_name2 = cha_name) %>%
  filter(
    cha_name %in% c(
      "Concertzender Oude Muziek",
      "Geen dag zonder Bach",
      "Live",
      "Radio on Demand",
      "Concertzender Jazz",
      "Barok"
    )
  )

# Luisteraars verspreid over de dag ----
cz_plot <- paste0("Luisteraars verspreid over de dag (", 
                  cz_stats_cfg$`current-month` %>% str_sub(1, 7),
                  ")")

tmp %>%
  ggplot(aes(x = hour_of_day, y = n_dev)) +
  ggtitle(cz_plot) +
  geom_line(
    data = tmp %>% dplyr::select(-cha_name),
    aes(group = cha_name2),
    color = "grey",
    size = 0.5,
    alpha = 0.5
  ) +
  geom_line(aes(color = cha_name), color = "#69b3a2", size = 1.2) +
  scale_color_viridis(discrete = TRUE) +
  theme_ipsum() +
  theme(
    legend.position = "none",
    plot.title = element_text(size = 14),
    panel.grid = element_blank(),
  ) +
  xlab(NULL) +
  ylab(NULL) +
  scale_x_continuous(breaks = c(0, 6, 12, 18)) +
  facet_wrap( ~ cha_name)

