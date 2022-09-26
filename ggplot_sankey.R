suppressPackageStartupMessages(library(networkD3))
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(viridis))
suppressPackageStartupMessages(library(patchwork))
suppressPackageStartupMessages(library(hrbrthemes))
suppressPackageStartupMessages(library(circlize))
suppressPackageStartupMessages(library(yaml))

suppressMessages( hrbrthemes::import_roboto_condensed())
cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_joined_04 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS"))

cz_stats_report.1 <- cz_stats_joined_04 %>% filter(!is.na(cha_name)) %>% 
  mutate(hour_of_day = hour(cz_ts_local),
         pct_pgm_coverage = case_when(cz_length >= 2700 ~ 100,
                                      cz_length >= 1800 ~ 75,
                                      cz_length >= 900 ~ 50,
                                      cz_length >= 450 ~ 25,
                                      T ~ 5),
         part_of_day = case_when(hour_of_day >= 18 ~ "E",
                                 hour_of_day >= 12 ~ "A",
                                 hour_of_day >= 6 ~ "M",
                                 T ~ "N")
  ) 

# uren per land per kanaal
sankey_data.1.uren <- cz_stats_report.1 %>% 
  filter(country_name != "" & country_name != "Netherlands") %>% 
  select(cha_name, country_name, cz_length) %>% 
  group_by(cha_name, country_name) %>% 
  summarise(.groups = "keep", uren = round(sum(cz_length) / 3600)) %>% 
  filter(uren > 0) %>% 
  arrange(cha_name, desc(uren)) %>% 
  ungroup()

# top-15 landen obv uren
sankey_data.land_rang <- sankey_data.1.uren %>% 
  group_by(country_name) %>% 
  summarise(.groups = "keep", uren_totaal = sum(uren)) %>%
  ungroup() %>% 
  arrange(desc(uren_totaal)) %>% 
  mutate(uren_per_land_rang = row_number()) %>% 
  select(-uren_totaal) %>% 
  filter(uren_per_land_rang <= 15L)

# top-15 kanalen obv uren
sankey_data.kanaal_rang <- sankey_data.1.uren %>% 
  group_by(cha_name) %>% 
  summarise(.groups = "keep", uren_totaal = sum(uren)) %>%
  ungroup() %>% 
  arrange(desc(uren_totaal)) %>% 
  mutate(uren_per_kanaal_rang = row_number()) %>% 
  select(-uren_totaal) %>% 
  filter(uren_per_kanaal_rang <= 15L)

# filter om te plotten
sankey_data.2.uren <- sankey_data.1.uren %>% 
  filter(cha_name %in% sankey_data.kanaal_rang$cha_name 
         & country_name %in% sankey_data.land_rang$country_name) %>% 
  inner_join(sankey_data.kanaal_rang) %>% 
  inner_join(sankey_data.land_rang) %>% 
  arrange(uren_per_kanaal_rang, uren_per_land_rang) %>% 
  select(source = cha_name, target = country_name, value = uren)
  
nodes <-
  data.frame(name = c(
    as.character(sankey_data.2.uren$source),
    as.character(sankey_data.2.uren$target)
  ) %>% unique())

# With networkD3, connection must be provided using id, not using real name like in the links dataframe.. So we need to reformat it.
sankey_data.2.uren$IDsource = match(sankey_data.2.uren$source, nodes$name) - 1

sankey_data.2.uren$IDtarget = match(sankey_data.2.uren$target, nodes$name) - 1

# prepare colour scale
viridis_colors = 'd3.scaleOrdinal() .range([
  "#440154FF",
  "#481567FF",
  "#482677FF",
  "#453781FF",
  "#404788FF",
  "#39568CFF",
  "#33638DFF",
  "#2D708EFF",
  "#287D8EFF",
  "#238A8DFF",
  "#1F968BFF",
  "#20A387FF",
  "#29AF7FFF",
  "#3CBB75FF",
  "#55C667FF",
  "#73D055FF",
  "#95D840FF",
  "#B8DE29FF",
  "#DCE319FF",
  "#FDE725FF"
])'

# Make the Network
p <- sankeyNetwork(
  Links = sankey_data.2.uren,
  Nodes = nodes,
  Source = "IDsource",
  Target = "IDtarget",
  Value = "value",
  NodeID = "name",
  sinksRight = FALSE,
  nodeWidth = 40,
  fontSize = 14,
  nodePadding = 20,
  fontFamily = "sans",
  iterations = 0, 
  units = "uur",
  colourScale = viridis_colors
  # width = 1200,
  # height = 1000
)

# htmlwidgets::prependContent(p, htmltools::tags$h3("Top-15 kanalen en landen"))

htmlwidgets::onRender(p, '
  function(el) { 
    var cols_x = this.sankey.nodes().map(d => d.x).filter((v, i, a) => a.indexOf(v) === i);
    var labels = ["Top-15 Kanalen", "Top-15 Internationaal"];
    cols_x.forEach((d, i) => {
      d3.select(el).select("svg")
        .append("text")
        .attr("x", d)
        .attr("y", 12)
        .text(labels[i]);
    })
  }
')
