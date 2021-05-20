library(magrittr)
library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)
library(ssh)
library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

config <- read_yaml("config.yaml")

source("src/prep_funcs.R", encoding = "UTF-8")

caroussel <- NULL

if (file_exists("caroussel.RDS")) {
  caroussel <- readRDS("caroussel.RDS")
} else {
  caroussel <- stage_caroussel()
}

# adjust for test: set snap_ts to feb '21
month(caroussel$cp_snap_ts) <- 2

cha_cur_pgms <- caroussel %>% 
  group_by(cha_id) %>% 
  mutate(cha_idx_max = max(cha_idx)) %>% 
  filter(!is.na(cp_snap_ts))

tc_interval_ym <- cha_cur_pgms$cp_snap_ts[1]
day(tc_interval_ym) <- 1L
hour(tc_interval_ym) <- 0L
minute(tc_interval_ym) <- 0L
second(tc_interval_ym) <- 0L
tc_interval_start <- tc_interval_ym - days(1)
tc_interval_stop <- tc_interval_ym + months(1) + days(1)

cur_cha_new <-  NULL

# infer intervals V2 ----
for (a_cur_cha_id in cha_cur_pgms$cha_id) {
  
  # init ----
  # a_cur_cha_id <- 21L
  cur_cha <- cha_cur_pgms %>% filter(cha_id == a_cur_cha_id) %>% 
    mutate(track_start = cp_snap_ts, 
           track_stop = cp_snap_ts + seconds(pgm_secs))
  cur_cha_new %<>% bind_rows(cur_cha)
  
  # fill backwards ----
  running_ymd <- cur_cha$track_start
  cur_cha_idx <- cur_cha$cha_idx
  cur_cha_idx_max <- cur_cha$cha_idx_max
  
  while (running_ymd > tc_interval_start) {
    
    cur_cha_idx <- cur_cha_idx - 1L
    
    if (cur_cha_idx == 0) {
      cur_cha_idx <- cur_cha_idx_max
    }
    
    cur_cha <- caroussel %>% filter(cha_id == a_cur_cha_id & cha_idx == cur_cha_idx) 
    next_stop <- running_ymd
    running_ymd <- running_ymd - seconds(cur_cha$pgm_secs)
    cur_cha %<>% mutate(track_start = running_ymd, 
                        track_stop = next_stop)
    cur_cha_new %<>% bind_rows(cur_cha)
  }
  
  # re-init ----
  cur_cha <- cha_cur_pgms %>% filter(cha_id == a_cur_cha_id) %>% 
    mutate(track_start = cp_snap_ts, 
           track_stop = cp_snap_ts + seconds(pgm_secs))
  
  # fill forward ----
  running_ymd = cur_cha$track_stop
  cur_cha_idx <- cur_cha$cha_idx

  while (running_ymd < tc_interval_stop) {
    
    cur_cha_idx <- cur_cha_idx + 1L
    
    if (cur_cha_idx > cur_cha_idx_max) {
      cur_cha_idx <- 1L
    }
    
    cur_cha <- caroussel %>% filter(cha_id == a_cur_cha_id & cha_idx == cur_cha_idx) 
    next_start <- running_ymd
    running_ymd <- running_ymd + seconds(cur_cha$pgm_secs)
    cur_cha %<>% mutate(track_start = next_start, 
                        track_stop = running_ymd)
    cur_cha_new %<>% bind_rows(cur_cha)
  }
}

caroussel.7 <- cur_cha_new %>% arrange(cha_id, track_start)

saveRDS(caroussel.7, "caroussel_7.RDS")

