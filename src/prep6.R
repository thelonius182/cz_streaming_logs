# library(magrittr)
# library(tidyr)
# library(dplyr)
# library(stringr)
# library(readr)
# library(lubridate)
# library(fs)
# library(futile.logger)
# library(ssh)
# library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (file_exists("caroussel.RDS")) {
  caroussel <- readRDS(file = "caroussel.RDS")
} else {
  caroussel <- stage_caroussel()
}

cha_cur_pgms <- caroussel %>% 
  group_by(cha_id) %>% 
  mutate(cha_idx_max = max(cha_idx)) %>% 
  filter(!is.na(cp_snap_ts))


# # # # # # #   T E S T   O N L Y   # # # # # # # 
# adjust for test: set snap_ts to feb '21
month(cha_cur_pgms$cp_snap_ts) <- month(ymd(cz_reporting_day_one, tz = "Europe/Amsterdam"))
# # # # # # #   T E S T   O N L Y   # # # # # # # 


cur_cha_new <-  NULL

# infer intervals ----
for (a_cur_cha_id in cha_cur_pgms$cha_id) {
  
  # init ----
  # a_cur_cha_id <- 21L
  cur_cha <- cha_cur_pgms %>% filter(cha_id == a_cur_cha_id) %>% 
    mutate(track_start = cp_snap_ts, 
           track_stop = cp_snap_ts + seconds(pgm_secs))
  
  if (is.null(cur_cha_new)){
    cur_cha_new <- cur_cha
  } else {
    cur_cha_new %<>% bind_rows(cur_cha)
  }
  
  # fill backwards ----
  running_ymd <- cur_cha$track_start
  cur_cha_idx <- cur_cha$cha_idx
  cur_cha_idx_max <- cur_cha$cha_idx_max
  
  while (running_ymd > cz_reporting_start) {
    
    cur_cha_idx <- cur_cha_idx - 1L
    
    if (cur_cha_idx == 0) {
      cur_cha_idx <- cur_cha_idx_max
    }
    
    cur_cha <- caroussel %>% filter(cha_id == a_cur_cha_id & cha_idx == cur_cha_idx) 
    next_stop <- running_ymd
    running_ymd <- running_ymd - seconds(cur_cha$pgm_secs)
    cur_cha %<>% mutate(track_start = running_ymd, 
                        track_stop = next_stop)
    
    if (is.null(cur_cha_new)){
      cur_cha_new <- cur_cha
    } else {
      cur_cha_new %<>% bind_rows(cur_cha)
    }
  }
  
  # re-init ----
  cur_cha <- cha_cur_pgms %>% filter(cha_id == a_cur_cha_id) %>% 
    mutate(track_start = cp_snap_ts, 
           track_stop = cp_snap_ts + seconds(pgm_secs))
  
  # fill forward ----
  running_ymd = cur_cha$track_stop
  cur_cha_idx <- cur_cha$cha_idx

  while (running_ymd < cz_reporting_stop) {
    
    cur_cha_idx <- cur_cha_idx + 1L
    
    if (cur_cha_idx > cur_cha_idx_max) {
      cur_cha_idx <- 1L
    }
    
    cur_cha <- caroussel %>% filter(cha_id == a_cur_cha_id & cha_idx == cur_cha_idx) 
    next_start <- running_ymd
    running_ymd <- running_ymd + seconds(cur_cha$pgm_secs)
    cur_cha %<>% mutate(track_start = next_start, 
                        track_stop = running_ymd)
    
    if (is.null(cur_cha_new)){
      cur_cha_new <- cur_cha
    } else {
      cur_cha_new %<>% bind_rows(cur_cha)
    }
  }
}

# adjust stop time ----
# if a pgm stops TOTH, make it stop 1 second earlier to facilitate splitting by hour later on
caroussel.7 <- cur_cha_new %>% 
  mutate(stop_toth = if_else(minute(track_stop) == 0 & second(track_stop) == 0, T, F),
         track_stop = if_else(stop_toth, track_stop - seconds(1L), track_stop),
         pgm_secs = if_else(stop_toth, pgm_secs - 1L, pgm_secs)
  ) %>% 
  arrange(cha_id, track_start)

saveRDS(caroussel.7, paste0(stats_data_flr(), "caroussel_7.RDS"))

rm(caroussel, cur_cha_new)
