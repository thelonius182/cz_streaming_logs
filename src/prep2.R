library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_ana.log"), "cz_ana_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/")

ana_full <- readRDS(file = "ana_full.RDS") %>% 
  filter(title != "eggs")

feb_2021 <- interval(ymd("2021-02-01"), rollback(ymd("2021-03-01")))

channel <- ana_full %>%
  filter(title == "oudemuziek" &
           cz_ts_ymd %within% feb_2021 &
           cz_seconds >= 300L) %>% 
  arrange(cz_ts_ymd, cz_ts_h, ip)

channel.listeners_by_day_by_hour <- oudemuziek %>% 
  select(cz_ts_ymd, ip) %>% 
  group_by(cz_ts_ymd) %>%
  distinct(ip) %>% 
  summarize(n_ips = n()) %>% 
  mutate(n_ips_roll = cumsum(n_ips))

live <- ana_full %>% filter(title == "live" & cz_ts_ymd %within% feb_2021) %>% arrange(cz_ts_ymd, cz_ts_h, ip)
