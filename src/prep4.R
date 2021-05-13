library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)
library(curlconverter)
library(jsonlite)
library(httr)
library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (!exists(x = "cz_stats_cha.03")) {
  cz_stats_cha.03 <- readRDS(file = "cz_stats_cha.03.RDS")
}

# prep current month
config <- read_yaml("config.yaml")
cur_month <-
  interval(
    ymd_hms(config$`current-month`, tz = "Europe/Amsterdam"),
    ymd_hms(config$`current-month`, tz = "Europe/Amsterdam") + months(1L),
    tzone = "Europe/Amsterdam"
  )

cz_stats_cha.04 <- cz_stats_cha.03 %>% 
  # + filter: TD-3.1 ----
  filter(is.na(lg_referrer) | !str_starts(lg_referrer, "^192.168")
  ) %>% 
  # + filter: TD-3.2 ----
  filter(lg_session_length >= 60L
  ) %>% 
  # + filter: TD-3.4 & 3.5 ----
  filter(!str_detect(lg_usr_agt,
                     pattern = "(bot|crawler|spider|checker|scanner|grabber|getter|\\(null\\))[\\s_:,.;/)-]?")
  ) %>% 
  # + filter: TD-3.8 ----
  mutate(lg_session_length = if_else(lg_session_length > 86400L, 86400L, lg_session_length),
         lg_session_start = lg_cz_ts,
         lg_session_stop = lg_cz_ts + dseconds(lg_session_length)
  ) %>% 
  # current month only ----
  filter(lg_session_start %within% cur_month
         | lg_session_stop %within% cur_month
  ) %>% 
  select(lg_ip,
         lg_device_type,
         lg_channel_id = lg_channel_idx,
         lg_session_start,
         lg_session_stop,
         lg_session_length,
         lg_referrer
  ) %>% 
  arrange(lg_ip,
          lg_device_type,
          lg_channel_id,
          lg_session_start,
          lg_session_stop
  )

rm(cz_stats_cha.03)
rm(config)

saveRDS(cz_stats_cha.04, file = "cz_stats_cha.04.RDS")
