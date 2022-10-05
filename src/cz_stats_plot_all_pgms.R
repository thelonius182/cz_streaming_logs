suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(viridis))
suppressPackageStartupMessages(library(ggplot2))
suppressPackageStartupMessages(library(hrbrthemes))
suppressPackageStartupMessages(library(fmsb))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(fs))

cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

suppressMessages( hrbrthemes::import_roboto_condensed())
options(knitr.table.format = "html")

cz_stats_pgm_report.2 <- read_rds(file = paste0(stats_data_flr(), "cz_licharod_stats_pgm_report.2.RDS"))

cz_pgm_radar.1 <- cz_stats_pgm_report.2 %>% 
  mutate(pgm_id = row_number(),
         sess_mobile = sess_mobile_web + sess_mobile_app) %>% 
  select(
    pgm_id,
    pgm_title,
    hours_ROD,
    hours_LIVE,
    hours_TH_CHA,
    sess_tot,
    n_unique_dev,
    pct_abroad,
    n_sess_P5,
    n_sess_P25,
    n_sess_P50,
    n_sess_P75,
    n_sess_P100,
    sess_desktop,
    sess_mobile,
    sess_webradio,
    sess_overig
  ) 

cz_pgm_radar.max <- cz_pgm_radar.1 %>% 
  select(-pgm_id, -pgm_title) %>% 
  mutate_all(~max(., na.rm = F)) %>% 
  distinct()

cz_pgm_radar.mean <- cz_pgm_radar.1 %>% 
  select(-pgm_id, -pgm_title) %>% 
  mutate_all(~na_if(., 0)) %>% 
  mutate_all(~mean(., na.rm = T)) %>% 
  distinct()

cz_pgm_radar.min <- rep(0, 15)
names(cz_pgm_radar.min) <- names(cz_pgm_radar.max)

colors_border=c( rgb(0.2,0.5,0.5,0.9), rgb(0.8,0.2,0.5,0.9) , rgb(0.7,0.5,0.1,0.9) )
colors_in=c( rgb(0.2,0.5,0.5,0.4), rgb(0.8,0.2,0.5,0.4) , rgb(0.7,0.5,0.1,0.4) )

# prep title list ----
cz_stats_verzendlijst.1 <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - verzendlijst.tsv",
                                      delim = "\t", escape_double = FALSE,
                                      trim_ws = TRUE,
                                      quote = "",
                                      show_col_types = FALSE) %>% 
  filter(deelnemer_actief == "j")

# get pgm details ----
cz_stats_by_pgm.1 <- read_rds(file = paste0(stats_data_flr(), "cz_licharod_stats_pgm_report.2.RDS")) %>% 
  filter(!is.na(hours_tot) & pgm_title %in% cz_stats_verzendlijst.1$titel_stats)

cz_stats_titles <- cz_stats_by_pgm.1 %>% select(pgm_title) %>% distinct()

# prep diagram folder ----
dir_create(paste0(stats_data_flr(), "diagrams/"))

# create diagrams ----
plot_cur_month <- cz_stats_cfg$`current-month` %>% str_sub(6, 7)
plot_cur_period <- paste0(" (",
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
                          ")")

for (wrk_title in cz_stats_titles$pgm_title) {
  cz_pgm_radar.a <- cz_pgm_radar.1 %>% filter(pgm_title == wrk_title)
  
  # get proper pgm title
  proper_pgm_title <- cz_stats_verzendlijst.1 %>% filter(titel_stats == wrk_title) %>% select(titel_gids) %>% distinct()
  
  # plot_title <-paste0(cz_pgm_radar.a$pgm_title, " (", stats_data_flr() %>% str_extract(pattern = "\\d{4}-\\d{2}"), ")")
  # plot_title <- paste0(proper_pgm_title$titel_gids[[1]], " (", stats_data_flr() %>% str_extract(pattern = "\\d{4}-\\d{2}"), ")")
  plot_title <- paste0(proper_pgm_title$titel_gids[[1]], plot_cur_period)
  cz_pgm_radar.a %<>% select(-pgm_id, -pgm_title)
  
  cz_pgm_radar.b <- cz_pgm_radar.max %>% 
    bind_rows(cz_pgm_radar.min) %>% 
    bind_rows(cz_pgm_radar.a) %>% 
    bind_rows(cz_pgm_radar.mean) 
  
  # rownames(cz_pgm_radar.b) <- c("max", "min", "pgm", "mean")
  # format(n_devices_month, big.mark = ".", decimal.mark = ",")
  names(cz_pgm_radar.b) <- c(
    paste0("On-demand: ", format(cz_pgm_radar.b$hours_ROD[[3]], big.mark = ".", decimal.mark = ","), " uur"),
    paste0("Live: ", format(cz_pgm_radar.b$hours_LIVE[[3]], big.mark = ".", decimal.mark = ","), " uur"),
    paste0("Themakanalen: ", format(cz_pgm_radar.b$hours_TH_CHA[[3]], big.mark = ".", decimal.mark = ","), " uur"),
    paste0("Sessies: ", format(cz_pgm_radar.b$sess_tot[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("Luisteraars: ", format(cz_pgm_radar.b$n_unique_dev[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("Buitenlands: ", cz_pgm_radar.b$pct_abroad[[3]], "%"),
    paste0("5% gehoord: ", format(cz_pgm_radar.b$n_sess_P5[[3]], big.mark = ".", decimal.mark = ","), " sessies"),
    paste0("25%: ", format(cz_pgm_radar.b$n_sess_P25[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("50%: ", format(cz_pgm_radar.b$n_sess_P50[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("75%: ", format(cz_pgm_radar.b$n_sess_P75[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("100%: ", format(cz_pgm_radar.b$n_sess_P100[[3]], big.mark = ".", decimal.mark = ","), " sessies"),
    paste0("via Desktop: ", format(cz_pgm_radar.b$sess_desktop[[3]], big.mark = ".", decimal.mark = ","), " sessies"),
    paste0("via Mobile: ", format(cz_pgm_radar.b$sess_mobile[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("via Webradio: ", format(cz_pgm_radar.b$sess_webradio[[3]], big.mark = ".", decimal.mark = ",")),
    paste0("via Overig: ", format(cz_pgm_radar.b$sess_overig[[3]], big.mark = ".", decimal.mark = ","), " sessies")
  )
  
  # png(paste0(stats_data_flr(), "diagrams/", wrk_title, ".png"), width = 1177, height = 800, units = "px")
  png(paste0(stats_data_flr(), "diagrams/", proper_pgm_title$titel_gids[[1]], ".png"), width = 1177, height = 800, units = "px")
  
  # show plot
  radarchart(cz_pgm_radar.b,
             axistype = 1,
             pcol = colors_border, pfcol = colors_in, plwd = 4, plty = 1,
             cglcol = "grey", cglty = 1, axislabcol = "grey", cglwd = 0.8,
             title = plot_title, 
             vlcex = 1.5,
             cex.main = 3) 
  legend(
    x = -2.1,
    y = 1.26,
    x.intersp = 0.9,
    legend = c("dit programma", "gemiddelde CZ"),
    bty = "n",
    pch = 20 ,
    col = colors_in ,
    text.col = "black",
    cex = 1.65,
    pt.cex = 5
  )
  
  dev.off()
}
