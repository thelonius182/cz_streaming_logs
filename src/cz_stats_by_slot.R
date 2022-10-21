suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
# suppressPackageStartupMessages(library(curlconverter))
# suppressPackageStartupMessages(library(jsonlite))
# suppressPackageStartupMessages(library(httr))
suppressPackageStartupMessages(library(yaml))
# suppressPackageStartupMessages(library(ssh))

# init logger ----
# fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

# load functions ----
# has func defs only
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

# flog.info(paste0("selecting logs for ", cz_reporting_day_one_chr), name = "cz_stats_proc_log")

# kalender
kal_date_fmt <- stamp("Sun 2022-10-16", orders = "%a %Y-%0m-%d", quiet = T)
stats_date_fmt <- stamp("Sun 2022-10-16 00", orders = "%a %Y-%0m-%d %H", quiet = T)

stats_kal.1 <- seq(cz_reporting_start, cz_reporting_stop, "days") %>% 
  kal_date_fmt() %>% as_tibble() 

rmv_months <- stats_kal.1 %>% head(1) %>% add_row(stats_kal.1 %>% tail(1))

stats_kal <- stats_kal.1 %>% 
  filter(!value %in% rmv_months$value) %>% 
  separate(col = value, into = c("weekday", "kal_ymd"), sep = " ") %>% 
  group_by(weekday) %>% mutate(cz_week = paste0("week_", row_number())) %>% ungroup()

# zenderschema erbij
zenderschema <- read_rds(paste0(cz_stats_cfg$cz_rds_store_ubu, "zenderschema.RDS"))

# Hedendaags
kal_hedendaags <- zenderschema %>% filter(titel == "CZ-Live hedendaags") %>% select(-titel) %>% 
  mutate(dag = str_sub(slot, 1, 2),
         weekday = case_when(dag == "ma" ~ "Mon",
                             dag == "di" ~ "Tue",
                             dag == "wo" ~ "Wed",
                             dag == "do" ~ "Thu",
                             dag == "vr" ~ "Fri",
                             dag == "za" ~ "Sat",
                             T ~ "Sun"
                             ),
         kal_hr.1 = as.numeric(str_sub(slot, 3, 4)),
         kal_hr.2 = kal_hr.1 + 1,
         kal.hr.3 = kal_hr.1 + 2,
         hh_offset = as.numeric(str_sub(hh_formule, 1, 2)),
         hh_hr.1 = as.numeric(str_sub(hh_formule, 5, 6)),
         hh_hr.2 = hh_hr.1 + 1,
         hh_hr.3 = hh_hr.1 + 2) %>% 
  inner_join(stats_kal, by = c("wanneer" = "cz_week", "weekday" = "weekday")) %>% 
  mutate(kal_ymd = ymd(kal_ymd, quiet = T),
         hh_ymd = kal_ymd + ddays(hh_offset))

kal_hedendaags.1 <- kal_hedendaags %>% select(contains("ymd") | contains("hr."))

kal_hedendaags_kal <- kal_hedendaags.1 %>% select(contains("kal")) %>% 
  pivot_longer(cols = contains("hr"), names_to = "kal_hr_nam", values_to = "kal_hr_val") %>% 
  select(-kal_hr_nam) %>% 
  mutate(kal_hr_val = str_pad(as.character(kal_hr_val), width = 2, side = "left", pad = "0")) 
  
kal_hedendaags.2 <- kal_hedendaags.1 %>% select(contains("hh")) %>% 
  pivot_longer(cols = contains("hr"), names_to = "hh_hr_nam", values_to = "hh_hr_val") %>% 
  select(-hh_hr_nam) %>% 
  mutate(hh_hr_val = str_pad(as.character(hh_hr_val), width = 2, side = "left", pad = "0")) %>% 
  rename(kal_ymd = hh_ymd, kal_hr_val = hh_hr_val) %>% 
  add_row(kal_hedendaags_kal) %>% 
  mutate(marker = "CZ-Live Hedendaags")

# init stats directory
stats_flr <- paste0(cz_stats_cfg$stats_data_home, str_sub(cz_reporting_day_one_chr, 1, 7))

# alle CZ-live stats via Live-stream (alle redacties)
stats_czlive <- read_rds(file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS")) %>% 
  filter(tolower(cha_name) == "live" & tolower(pgm_title_fixed) == "concertzender live") %>% 
  select(cz_ipa, cz_ts, cz_length) %>% 
  mutate(cz_ts_kal = stats_date_fmt(cz_ts)) %>% 
  separate(col = cz_ts_kal, into = c("weekday", "stats_ymd", "stats_hr"), sep = " ") %>% 
  select(-cz_ts) %>% 
  inner_join(stats_kal, by = c("stats_ymd" = "kal_ymd")) %>% 
  select(-weekday.x, weekday = weekday.y) %>% 
  mutate(stats_ymd = ymd(stats_ymd, quiet = T)) %>% 
  arrange(stats_ymd, stats_hr, cz_ipa)

# filter CZ-live Hedendaags
stats_czlive_hedendaags <- stats_czlive %>% 
  left_join(kal_hedendaags.2, 
            by = c("stats_ymd" = "kal_ymd", "stats_hr" = "kal_hr_val")) %>% 
  filter(!is.na(marker))

# kentallen
czlive_hedendaags_uren <- round(sum(stats_czlive_hedendaags$cz_length) / 3600, digits = 0)
czlive_hedendaags_sessies <- stats_czlive_hedendaags %>% nrow()
czlive_hedendaags_luisteraars <- stats_czlive_hedendaags %>% select(cz_ipa) %>% distinct() %>% nrow()
