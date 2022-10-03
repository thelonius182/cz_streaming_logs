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

# get start from config file----
fmt_cz_month <- stamp("202112", orders = "%y%0m", quiet = T)
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_rep_month_start <- paste0(fmt_cz_month(ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")), "00")
cz_rep_month_stop <- paste0(fmt_cz_month(ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")), "00")

salsa_stats_all_pgms_w_editor <-
  read_delim(
    "~/Downloads/salsa_stats_all_pgms.txt",
    delim = "\t",
    escape_double = FALSE,
    lazy = F,
    quote = "",
    trim_ws = TRUE,
    show_col_types = FALSE
  )

cz_live.1 <- salsa_stats_all_pgms_w_editor %>% 
  filter(pgmStart > cz_rep_month_start &
           pgmStart < cz_rep_month_stop & 
           post_editor != "CZ" & 
           !is.na(post_editor)) %>% 
  select(pgmTitle, post_editor) %>% distinct() %>% arrange(pgmTitle, post_editor) %>% 
  mutate(pgmTitle = case_when(pgmTitle == "Bach &amp; Co" ~ "Bach en Co",
                              pgmTitle == "Groove &amp; Grease" ~ "Groove and Grease",
                              pgmTitle == "JazzNotJazz Music &amp; Politics" ~ "JazzNotJazz Music and Politics",
                              pgmTitle == "Framework 1" ~ "Framework",
                              pgmTitle == "Framework 2" ~ "Framework",
                              TRUE ~ pgmTitle),
         post_editor = case_when(post_editor == "Framework" ~ "Hessel Veldman",
                                 TRUE ~ post_editor))

cz_live.coli.1 <- salsa_stats_all_pgms_w_editor %>% 
  filter(pgmStart > cz_rep_month_start &
           pgmStart < cz_rep_month_stop & 
           post_editor != "CZ" & 
           !is.na(post_editor) &
           pgmTitle == "Concertzender Live") %>% 
  select(post_editor, pgmTitle)

n_coli_rows = nrow(cz_live.coli.1)

cz_live.coli.2 <- cz_live.coli.1 %>% 
  group_by(post_editor) %>% 
  summarise(n_editors = n()) %>% 
  mutate(editor_pct = round(100.0 * n_editors / n_coli_rows, digits = 2))

cz_live_multiple_editors_by_pgm <- cz_live.1 %>%
  group_by(pgmTitle) %>%
  summarise(n = n()) %>% ungroup() %>% filter(n > 1) %>% select(pgmTitle)

cz_live.2 <- cz_live.1 %>% filter(pgmTitle %in% cz_live_multiple_editors_by_pgm$pgmTitle)

pgm2cha <- read_rds(file = paste0(stats_data_flr(), "caroussel_7.RDS")) %>% 
  ungroup() %>% select(pgm_title, cha_name) %>% distinct()

# wpgi_raw <- read_delim("~/Downloads/Wordpress gids-info - gids-info.tsv",
#                        delim = "\t", escape_double = FALSE, quote = "", show_col_types = F,
#                        trim_ws = TRUE)
# 
# wpgi.1 <- wpgi_raw %>% 
#   select(titel_gids = `titel-NL`, titel_moro = `key-modelrooster`, redacteur = `productie-1-mdw`) %>% 
#   arrange(titel_gids, titel_moro, redacteur)

cz_stats_verzendlijst.1 <- read_delim("~/Downloads/Luistercijfers verzendlijst - ovz.tsv",
                                      delim = "\t", 
                                      escape_double = FALSE, 
                                      quote = "",
                                      trim_ws = TRUE,
                                      show_col_types = FALSE) %>% 
  filter(actief_jn == "j") %>% 
  mutate(titel_wpgidsinfo = case_when(titel_wpgidsinfo == "Bach & Co" ~ "Bach en Co",
                                      titel_wpgidsinfo == "Groove & Grease" ~ "Groove and Grease",
                                      titel_wpgidsinfo == "JazzNotJazz Music & Politics" ~ "JazzNotJazz Music and Politics",
                                      titel_wpgidsinfo == "Cinemathèque" ~ "Cinematheque",
                                      titel_wpgidsinfo == "De Wandeling" ~ "De wandeling",
                                      titel_wpgidsinfo == "Geen Dag zonder Bach" ~ "Geen dag zonder Bach",
                                      titel_wpgidsinfo == "Het Pianoatelier" ~ "Het pianoatelier",
                                      titel_wpgidsinfo == "Missa Etcetera" ~ "Missa etcetera",
                                      titel_wpgidsinfo == "Omzwervingen in de Joodse Muziek" ~ "Omzwervingen in de Joodse muziek",
                                      titel_wpgidsinfo == "Tussen Droom en Daad" ~ "Tussen droom en daad",
                                      TRUE ~ titel_wpgidsinfo))

cz_stats_pgm_report <- read_rds(file = paste0(stats_data_flr(), "cz_licharod_stats_pgm_report.2.RDS")) %>% 
  select(pgm_title, hours_tot)

cz_stats_verzendlijst.2a <- cz_live.1 %>% 
  left_join(pgm2cha, by = c("pgmTitle" = "pgm_title")) 

cz_stats_verzendlijst.2b <- cz_stats_verzendlijst.2a %>% 
  left_join(cz_stats_verzendlijst.1, by = c("pgmTitle" = "titel_wpgidsinfo"))

cz_stats_verzendlijst.2c <- cz_stats_verzendlijst.2b %>% 
  left_join(cz_stats_pgm_report, by = c("titel_stats" = "pgm_title")) %>% 
  filter(!is.na(hours_tot)) %>% 
  select(-hours_tot)

cz_stats_verzendlijst.2d <- cz_stats_verzendlijst.2c %>% 
  filter(post_editor == wie) %>% 
  select(titel_gids = pgmTitle, everything(), -post_editor)

write_delim(cz_stats_verzendlijst.2d, 
            file = paste0(stats_data_flr(), "luistercijfers_verzendlijst.tsv"), 
            delim = "\t", 
            na = "")  
