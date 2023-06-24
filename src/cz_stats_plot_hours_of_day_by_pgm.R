pacman::p_load(magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, curlconverter,
               jsonlite, httr, yaml, ssh, googledrive, viridis, ggplot2, hrbrthemes, yaml, sitools,
               fmsb, kableExtra, DT, plotly)

options(knitr.table.format = "html")
cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# load verzendlijst ----
source("src/get_google_czdata_stats.R", encoding = "UTF-8")

# prep title list ----
# cz_stats_verzendlijst.tpt <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - themakanalen-per-titel.tsv",
#                                         delim = "\t", escape_double = FALSE,
#                                         trim_ws = TRUE,
#                                         quote = "",
#                                         show_col_types = FALSE) 

tpt_dft.1 <- tbl_stats_vzl_tpt %>% 
  select(titel_gids, themakanaal) %>% distinct() 

rod_all <- tbl_stats_vzl_lst %>% 
# rod_all <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - verzendlijst.tsv",
#                       delim = "\t", escape_double = FALSE,
#                       trim_ws = TRUE,
#                       quote = "",
#                       show_col_types = FALSE) %>% 
  filter(deelnemer_actief) %>% 
  select(titel_gids) %>% distinct() %>% 
  mutate(themakanaal = "Radio on Demand")

live_stream_tonen <- tbl_stats_vzl_lst %>% 
# live_stream_tonen <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - verzendlijst.tsv",
#                                 delim = "\t", escape_double = FALSE,
#                                 trim_ws = TRUE,
#                                 quote = "",
#                                 show_col_types = FALSE) %>% 
  filter(deelnemer_actief & live_stream_tonen) %>% 
  select(titel_gids) %>% arrange(titel_gids) %>% distinct()

tpt_dft.2 <- rod_all %>% 
  filter(titel_gids %in% live_stream_tonen$titel_gids) %>% 
  select(titel_gids) %>% distinct() %>% 
  mutate(themakanaal = "Live")

cz_stats_verzendlijst.tpt.1 <-
  tbl_stats_vzl_tpt %>% 
  bind_rows(tpt_dft.1) %>% 
  bind_rows(rod_all) %>% 
  bind_rows(tpt_dft.2) %>% 
  arrange(titel_gids, themakanaal) %>% 
  filter(!(titel_gids == "Concertzender Live" & themakanaal == "Radio on Demand")) %>% 
  select(-greenhost_stream) %>% distinct()

# CZ stats ----
cz_stats_report.4f <- read_rds(file = paste0(stats_data_flr(), "cz_stats_report.4f.RDS"))
cz_stats_report.4c <- read_rds(file = paste0(stats_data_flr(), "cz_stats_report.4c.RDS"))

cz_stats_hours <- cz_stats_report.4f %>% bind_rows(cz_stats_report.4c)%>% 
  select(cha_name, hour_of_day, n_dev) %>% 
  filter(!cha_name %in% c("TOTALEN")) %>% 
  ungroup()

chas_by_pgm <- cz_stats_verzendlijst.tpt.1 %>% group_by(titel_gids) %>% 
  mutate(cha_filter = paste0("c('", str_flatten(themakanaal, collapse = "', '"), "')"),
         cha_filter = str_replace_all(cha_filter, "'", '"')) %>% 
  select(-themakanaal) %>% distinct() %>% ungroup()

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

for (titel in chas_by_pgm$titel_gids) {
  print(titel)
  cha_names <- cz_stats_verzendlijst.tpt.1 %>% filter(titel_gids == titel) %>% select(themakanaal)
  
  tmp <- cz_stats_hours %>%
    mutate(cha_name = if_else(cha_name == "RoD", "Radio on Demand", cha_name),
           cha_name2 = cha_name) %>%
    filter(cha_name %in% cha_names$themakanaal)

  # Luisteraars verspreid over de dag ----
  cz_plot <- paste0("Luisteraars verspreid over de dag", plot_cur_period)
  
  png(paste0(stats_data_flr(), 
             "diagrams/Streams waar ",
             titel,
             " in opgenomen is.png"), width = 0.7*1177, height = 0.7*800, units = "px")
  
  print(ggplot(data = tmp, aes(x = hour_of_day, y = n_dev)) +
          ggtitle(cz_plot) +
          geom_line(
            data = tmp %>% dplyr::select(-cha_name),
            aes(group = cha_name2),
            color = "grey",
            size = 0.5,
            alpha = 0.5
          ) +
          geom_line(aes(color = cha_name), color = "#0072B2", size = 1.2) +
          scale_color_viridis(discrete = TRUE) +
          theme_ipsum() +
          theme(
            legend.position = "none",
            plot.title = element_text(size = 28),
            panel.grid = element_blank(),
            strip.text = element_text(size = 18)
          ) +
          xlab(NULL) +
          ylab(NULL) +
          scale_x_continuous(breaks = c(0, 6, 12, 18)) +
          facet_wrap( ~ cha_name))
  
  dev.off()
}
