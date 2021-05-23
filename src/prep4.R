# library(tidyr)
# library(dplyr)
# library(stringr)
# library(readr)
# library(lubridate)
# library(fs)
# library(futile.logger)
# library(curlconverter)
# library(jsonlite)
# library(httr)
# library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (!exists(x = "cz_stats_cha.03")) {
  cz_stats_cha.03 <- readRDS(file = "cz_stats_cha.03.RDS")
}

# prep current month
tc_interval_ts <- tc_cur_pgms$cp_snap_ts[[1]]
day(tc_interval_ts) <- 1L
hour(tc_interval_ts) <- 0L
minute(tc_interval_ts) <- 0L
second(tc_interval_ts) <- 0L
tc_interval_start <- tc_interval_ts - days(1)
tc_interval_stop <- tc_interval_ts + months(1) + days(1)
cur_month <- interval(tc_interval_start, tc_interval_stop, tzone = "Europe/Amsterdam")

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
         lg_session_stop = lg_cz_ts + seconds(lg_session_length),
         # if a pgm stops TOTH, make it stop 1 second earlier to facilitate splitting by hour later on
         stop_toth = if_else(minute(lg_session_stop) == 0 & second(lg_session_stop) == 0, T, F),
         lg_session_stop = if_else(stop_toth, lg_session_stop - seconds(1L), lg_session_stop),
         lg_session_length = if_else(stop_toth, 
                                     as.integer(lg_session_length - seconds(1L)), 
                                     as.integer(lg_session_length))
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

saveRDS(cz_stats_cha.04, file = "cz_stats_cha.04.RDS")
