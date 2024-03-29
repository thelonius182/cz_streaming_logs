# library(magrittr)
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

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (!exists(x = "cz_stats_cha.04")) {
  cz_stats_cha.04 <- readRDS(file = paste0(stats_data_flr(), "cz_stats_cha.04.RDS"))
}

# create clean frgments ----
# itvl: "interval"
itvl01 <- cz_stats_cha.04 %>% 
  group_by(lg_ip, lg_device_type,lg_channel_id) %>% 
  mutate(lg_grp_idx = row_number()) %>% 
  ungroup() %>% 
  arrange(lg_ip, lg_device_type,lg_channel_id, lg_session_start) %>% 
  mutate(lg_cur_iv = interval(lg_session_start, lg_session_stop),
         lg_prv_iv = interval(lag(lg_session_start), lag(lg_session_stop)),
         lg_within_prv = if_else(lg_grp_idx > 1 & lg_cur_iv %within% lg_prv_iv, T, F)) %>% 
  filter(!lg_within_prv) %>% 
  select(-lg_within_prv) %>% 
  mutate(lg_overlap_prv = if_else(lg_grp_idx > 1 & int_overlaps(lg_cur_iv, lg_prv_iv), T, F))
  
itvl02 <- itvl01 %>% 
  # link fragments of same session
  mutate(lg_ext_session = if_else(lg_overlap_prv, NA_integer_, row_number())) %>% 
  fill(lg_ext_session, .direction = "down") %>% 
  # extract start/stop by linked fragment group
  group_by(lg_ext_session) %>% 
  mutate(lg_ext_sess_start = min(lg_session_start),
         lg_ext_sess_stop = max(lg_session_stop)) %>% 
  # remove partial fragments
  filter(!lg_overlap_prv)

itvl03 <- itvl02 %>% 
  ungroup() %>% 
  # recalculate session lengths & apply 24 hour rule again
  mutate(lg_session_length = as.integer(int_length(interval(lg_ext_sess_start, lg_ext_sess_stop))),
         lg_session_length = if_else(lg_session_length > 86400L, 86400L, lg_session_length))

itvl04 <- itvl03 %>% 
  select(lg_ip, lg_device_type, lg_channel_id, lg_sess_start = lg_ext_sess_start, lg_sess_stop = lg_ext_sess_stop,
         lg_session_length, lg_referrer)

# assign an id to each session
itvl05 <- itvl04 %>% 
  mutate(lg_sess_id = row_number()) %>% 
  select(lg_sess_id, everything())

# split in hourly segments ----
sessions_by_hour <- tibble(
  sbh.id = 0L,
  sbh.ts = ymd_hms("1970-01-01 00:00:00", tz = "Europe/Amsterdam"),
  sbh.length = 0L
)

for (sid in itvl05$lg_sess_id) {
  
  cur_sess <- itvl05 %>% filter(lg_sess_id == sid)
  fd_start <- floor_date(cur_sess$lg_sess_start, unit = "hour")
  fd_stop <- floor_date(cur_sess$lg_sess_stop, unit = "hour")
  
  if (fd_start == fd_stop) {
    cur_sbh <- tibble(
      sbh.id = cur_sess$lg_sess_id,
      sbh.ts = fd_start,
      sbh.length = cur_sess$lg_session_length
    )
    
    sessions_by_hour %<>% add_row(cur_sbh)
    
  } else {
    breaks = seq(fd_start, fd_stop, by = "1 hour")
    sess_hours <- fd_start + hours(0: length(breaks)) 
    sess_itvls <- int_diff(sess_hours)
    last_iter <- length(breaks)
    
    for (i1 in 1:last_iter) {
    
      if (i1 == 1) {
        
        cur_sbh <- tibble(
          sbh.id = cur_sess$lg_sess_id,
          sbh.ts = fd_start,
          sbh.length = int_length(interval(cur_sess$lg_sess_start, int_end(sess_itvls[[1]])))
        )
        
      } else if (i1 == last_iter) {
        
        cur_sbh <- tibble(
          sbh.id = cur_sess$lg_sess_id,
          sbh.ts = fd_stop,
          sbh.length = int_length(interval(int_start(sess_itvls[[last_iter]]), cur_sess$lg_sess_stop))
        )
        
      } else {
        
        cur_sbh <- tibble(
          sbh.id = cur_sess$lg_sess_id,
          sbh.ts = int_start(sess_itvls[[i1]]),
          sbh.length = 3600L
        )
        
      }
      
      sessions_by_hour %<>% add_row(cur_sbh)
    }
  }
}

# join segments to pgm details ----
cz_stats_cha_05 <- sessions_by_hour %>%
  inner_join(itvl05, by = c("sbh.id" = "lg_sess_id")) %>%
  mutate(cz_row_id = row_number()) %>% 
  select(
    cz_row_id,
    cz_id = sbh.id,
    cz_ipa = lg_ip,
    cz_dev_type = lg_device_type,
    cz_cha_id = lg_channel_id,
    cz_ts = sbh.ts,
    cz_length = sbh.length,
    cz_ref = lg_referrer
  )

rm(itvl01, itvl02, itvl03, itvl04, itvl05, sessions_by_hour, cz_stats_cha.04)
