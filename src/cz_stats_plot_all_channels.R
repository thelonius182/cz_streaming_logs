suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(viridis))
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(hrbrthemes))
suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(sitools))
suppressPackageStartupMessages(library(fs))

suppressMessages( hrbrthemes::import_roboto_condensed())
cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_joined_04 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS"))
cz_stats_report_03 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_report.3.RDS")) 

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
                                 T ~ "N"),
         cha_name = str_replace(cha_name, "RoD", "Radio-on-demand")
  ) 

# total number of devices this month (= number of unique listeners)
n_devices_month <-  cz_stats_report.1 %>% 
  group_by(cz_ipa, cz_dev_type) %>% 
  summarise(n_sess = n()) %>% 
  ungroup() %>% nrow()

# Luisteraars per kanaal
lolli.1a <- cz_stats_report.1 %>% 
  group_by(cha_name) %>% 
  summarise(cz_totaal = round(sum(cz_length) / 3600L)) %>% 
  arrange(cz_totaal) %>% 
  mutate(cha_name = factor(cha_name, unique(cha_name)),
         cz_group = factor("Uren"))

lolli.1b <- cz_stats_report.1 %>% 
  group_by(cha_name) %>% 
  summarise(cz_totaal = n()) %>% 
  arrange(cz_totaal) %>% 
  mutate(cha_name = factor(cha_name, unique(cha_name)),
         cz_group = factor("Sessies"))

lolli.1c <- cz_stats_report.1 %>% 
  select(cha_name, cz_ipa) %>% distinct() %>% 
  group_by(cha_name) %>% 
  summarise(cz_totaal = n()) %>% 
  arrange(cz_totaal) %>% 
  mutate(cha_name = factor(cha_name, unique(cha_name)),
         cz_group = factor("Luisteraars"))

lolli.1 <- lolli.1a %>% bind_rows(lolli.1b) %>% bind_rows(lolli.1c) %>% 
  filter(cz_totaal > 750) 

lolli.2 <- NULL

for (a_name in lolli.1$cha_name) {
  ### TEST
  # a_name <- "Live"
  ### TEST
  tmp_lolli.a <- lolli.1 %>% filter(cha_name == a_name & cz_group == "Uren") 
  tmp_lolli.b <- tmp_lolli.a %>% mutate(cz_totaal_si = f2si2(cz_totaal, T))
  
  if (is.null(lolli.2)) {
    lolli.2 <- tmp_lolli.b
  } else {
    lolli.2 %<>% bind_rows(tmp_lolli.b)
  }

  tmp_lolli.a <- lolli.1 %>% filter(cha_name == a_name & cz_group == "Sessies") 
  tmp_lolli.b <- tmp_lolli.a %>% mutate(cz_totaal_si = f2si2(cz_totaal, T))
  
  if (is.null(lolli.2)) {
    lolli.2 <- tmp_lolli.b
  } else {
    lolli.2 %<>% bind_rows(tmp_lolli.b)
  }
  
  tmp_lolli.a <- lolli.1 %>% filter(cha_name == a_name & cz_group == "Luisteraars") 
  
  if (nrow(tmp_lolli.a) > 0) {
    tmp_lolli.b <- tmp_lolli.a %>% mutate(cz_totaal_si = f2si2(cz_totaal, T))
    
    if (is.null(lolli.2)) {
      lolli.2 <- tmp_lolli.b
    } else {
      lolli.2 %<>% bind_rows(tmp_lolli.b)
    }
  }
  
}

lolli.2 %<>% distinct() 

# plot olot: Overzicht Live Ondemand Themakanalen
plot_cur_month <- cz_stats_cfg$`current-month` %>% str_sub(6, 7)

olot <- ggplot( data = lolli.2, 
        aes(fill = cz_group, color = cz_group)) +
  geom_segment( aes(x = cha_name, xend = cha_name,
                    y = 0, yend = cz_totaal),
                color = "grey") +
  geom_point( aes(x = cha_name, y = cz_totaal, shape = cz_group),
              size = 3, alpha = 0.8, stroke = 2, fill = 2) +
  # geom_text( aes(x = cha_name, y = cz_totaal, label = format(cz_totaal, big.mark = ".", decimal.mark = ",")), 
  geom_text( aes(x = cha_name, y = cz_totaal, label = cz_totaal_si), 
             hjust = -0.35,
             size = 5.1,
             nudge_x = case_when(lolli.2$cz_group == "Uren" ~ 0.4,
                                 lolli.2$cz_group == "Luisteraars" ~ 0.4, 
                                 T ~ 0.0),
             check_overlap = T,
             colour = "black") +
  theme_ipsum() +
  coord_flip() +
  theme(
    panel.grid.major.y = element_blank(),
    panel.border = element_blank(),
    axis.ticks.y = element_blank(),
    axis.text.x = element_text(color = 'black', size = 18),
    # axis.text.y = element_text(color = 'black', size = 18, hjust = 1, margin = margin(r = -3.0, unit = "cm")),
    axis.text.y = element_text(color = 'black', size = 18, hjust = 1),
    legend.title = element_blank(),
    legend.text = element_text(size = 26),
    legend.justification = c(0.45, 0.4), legend.position = c(0.6, 0.4),
    plot.title = element_text(size=30),
    plot.subtitle = element_text(size=24)
  ) +
  xlab(NULL) +
  ylab(NULL) +
  labs(title = paste0("Overzicht Live, OnDemand en Themakanalen (",
                      case_when(plot_cur_month == "01" ~ "Jan. ",
                                plot_cur_month == "02" ~ "Feb. ",
                                plot_cur_month == "03" ~ "Mrt. ",
                                plot_cur_month == "04" ~ "Apr. ",
                                plot_cur_month == "05" ~ "Mei ",
                                plot_cur_month == "06" ~ "Jun. ",
                                plot_cur_month == "07" ~ "Jul. ",
                                plot_cur_month == "08" ~ "Aug. ",
                                plot_cur_month == "09" ~ "Sep. ",
                                plot_cur_month == "10" ~ "Okt. ",
                                plot_cur_month == "11" ~ "Nov. ",
                                TRUE ~ "Dec. "),
                      cz_stats_cfg$`current-month` %>% str_sub(1, 4),
                      ")"),
       subtitle = paste0("unieke luisteraars (totaal) = ",
                         format(round(n_devices_month, digits = -2), big.mark = ".", decimal.mark = ","))
  ) +
  scale_color_manual(values = c('#D55E00FF', '#0072B2FF', "#F0E442FF"))
  # scale_color_manual(values = c('#E69F00FF', '#0072B2FF', "#CC79A7FF")) 
  # c("#999999", "#E69F00", "#56B4E9", "#009E73", 
  #   "#F0E442", "#0072B2", "#D55E00", "#CC79A7")

ggsave(
  plot = olot,
  device = "png",
  width = 500,
  height = 350,
  units = "mm",
  bg = "white",
  filename = "CZ-luistercijfers, alle kanalen.png",
  path = paste0(stats_data_flr(), "diagrams/")
)
  # scale_x_continuous(expand = expansion(mult = 0.1)) 

  # geom_hline(aes(yintercept = n_devices_month), size = 0.6, colour = "red") +
  # annotate(
  #   "text",
  #   x = 3,
  #   y = 33000,
  #   label = paste0(
  #     "Luisteraars over alle kanalen samen = ",
  #     format(
  #       n_devices_month,
  #       big.mark = ".",
  #       decimal.mark = ","
  #     )
  #   ),
  #   colour = "red",
  #   size = 6
  # )

# 
# ggplot(data = lolli.1,
#        aes(x = cha_name, y = luisteraars_per_kanaal)) +
#   geom_segment(aes(
#     x = cha_name,
#     xend = cha_name,
#     y = 0,
#     yend = luisteraars_per_kanaal
#   ),
#   color = "grey") +
#   geom_point(color = "orange",
#              size = 4,
#              alpha = 0.6) +
#   geom_text(aes(label = luisteraars_per_kanaal), hjust = -0.5) +
#   theme_ipsum() +
#   coord_flip() +
#   theme(
#     panel.grid.major.y = element_blank(),
#     panel.border = element_blank(),
#     axis.ticks.y = element_blank(),
#     plot.margin = margin(1, 1, 4, 1.1, "cm"),
#     axis.text.y = element_text(color = 'black', size = 14, hjust = 1)
#   ) +
#   xlab(NULL) +
#   ylab(NULL) +
#   labs(title = "Luisteraars per kanaal", subtitle = "obv sessies") +
#   scale_y_continuous(breaks = c(0, 20000, 40000, 60000))
