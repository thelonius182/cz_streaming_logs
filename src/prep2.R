library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_ana.log"), "cz_ana_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/")

ana_full <- readRDS(file = "ana_full.RDS") 

feb_2021 <- interval(ymd("2021-02-01"), rollback(ymd("2021-03-01")))

# feb 2021 only
ana_2021.02 <- ana_full %>%
  filter(lg_cz_ts %within% feb_2021) %>% 
  # split http-reg
  separate(lg_http_req, into = c("lg_http_cmd", "lg_cz_channel", "lg_http_protocol"), sep = " ") %>% 
  # http-200 only
  filter(lg_http_resp_sts == "200")

cz_bots.1 <- ana_2021.02 %>% 
  filter(str_detect(lg_usr_agt, pattern = "bot[\\s_ :,.;/-]"))

cz_bots.2 <- ana_2021.02 %>% 
  filter(is.na(lg_usr_agt))

cz_bots.3 <- ana_2021.02 %>% 
  filter(str_detect(lg_usr_agt, pattern = "dalvik/"))

cz_bots.4 <- ana_2021.02 %>% 
  filter(str_detect(lg_usr_agt, pattern = "bingbot/"))

cz_bots.5 <- ana_2021.02 %>% 
  filter(str_detect(lg_usr_agt, pattern = "core"))

cz_n_ips <- ana_2021.02 %>% select(lg_ip) %>% distinct()

cz_n_usr_agts <- ana_2021.02 %>% select(lg_usr_agt) %>% distinct()

cz_stats.1 <- ana_2021.02 %>%
  group_by(lg_ip, lg_usr_agt) %>%
  summarise(n_visits = n(),
            bandwidth_MB = round(sum(as.integer(lg_n_bytes))/1024/1024, digits = 2)) %>% 
  mutate(avg_MB_by_visit = round(bandwidth_MB/n_visits, digits = 2))

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
