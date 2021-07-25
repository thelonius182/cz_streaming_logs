suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(ssh))

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_rod.log"), "cz_stats_rod_log")

if (!exists("rod_logs")) {
  flog.info("loading DF rod_logs from RDS", name = "cz_stats_rod_log")
  rod_logs <- read_rds(file = "rod_logs_2.RDS")
} else {
  flog.info("reusing DF rod_logs", name = "cz_stats_rod_log")
}

if (!exists("cz_gids")) {
  flog.info("loading DF cz_gids from RDS", name = "cz_stats_rod_log")
  cz_gids <- read_rds(file = "cz_gids_1.RDS")
} else {
  flog.info("reusing DF cz_gids", name = "cz_stats_rod_log")
}

if (!exists("cz_audio")) {
  flog.info("loading DF cz_audio from RDS", name = "cz_stats_rod_log")
  cz_audio <- read_rds(file = "cz_rod_audio_1.RDS")
} else {
  flog.info("reusing DF cz_audio.1", name = "cz_stats_rod_log")
}

cz_stats_rod.1 <- rod_logs %>% 
  inner_join(cz_audio, by = c("lg_pgmkey" = "key_tk_ymd")) %>% 
  select(lg_ipa, lg_usr_agt, lg_pgmkey, lg_ts, lg_sts, lg_size, audio_size, everything()) 

cz_stats_rod.2 <- cz_stats_rod.1 %>% 
  inner_join(cz_gids, by = c("lg_pgmkey" = "pgm_start")) %>% 
  mutate(pgm_seconds = as.numeric(interval(lg_pgmkey, pgm_stop), "seconds"))

cz_stats_rod.3 <- cz_stats_rod.2 %>% 
  mutate(tmp_bps = audio_size / pgm_seconds,
         tmp_frag_seconds = lg_size / tmp_bps) 

cz_stats_rod.4 <- cz_stats_rod.3 %>% 
  group_by(lg_ipa, lg_usr_agt, lg_pgmkey) %>% 
  summarize(bc_key = row_number(),
            tmp_total_seconds = sum(tmp_frag_seconds)) %>% 
  mutate(tmp_total_seconds = round(tmp_total_seconds, 0)) %>% 
  select(-bc_key) %>% 
  ungroup() %>% 
  distinct()

cz_stats_rod.5 <- cz_stats_rod.3 %>% 
  inner_join(cz_stats_rod.4) 

cz_stats_rod.6 <- cz_stats_rod.5 %>% 
  select(-tmp_frag_seconds, -tmp_bps, -lg_size, -key_rowNum, -lg_sts) %>% 
  distinct()

cz_stats_rod.7 <- cz_stats_rod.6 %>% 
  mutate(lg_listened_seconds = if_else(tmp_total_seconds <= pgm_seconds,
                                       tmp_total_seconds,
                                       pgm_seconds)) %>% 
  group_by(lg_ipa, lg_usr_agt, lg_pgmkey) %>% 
  mutate(bc_key = row_number()) %>% 
  filter(bc_key == 1) %>% 
  ungroup()

cz_stats_rod.8 <- cz_stats_rod.7 %>% 
  select(-audio_size, -audio_file, -key_tk_dir, -pgm_stop, -pgm_seconds, -tmp_total_seconds, -bc_key) %>% 
  filter(lg_listened_seconds >= 120)

rm(cz_stats_rod.1,
   cz_stats_rod.2,
   cz_stats_rod.3,
   cz_stats_rod.4,
   cz_stats_rod.5,
   cz_stats_rod.6,
   cz_stats_rod.7)

write_rds(x = cz_stats_rod.8,
          file = "cz_stats_rod_8.RDS",
          compress = "gz")

cz_stats_rod.8 <- read_rds(file = "cz_stats_rod_8.RDS")
cz_stats_cha_08a <- read_rds(file = "cz_stats_cha_08.RDS") 

cz_stats_cha_08 <- cz_stats_cha_08a %>% 
  inner_join(channels, by = c("cz_cha_id" = "id")) 

cz_stats_rod.9 <- cz_stats_rod.8 %>% 
  rename(lg_sess_start = lg_ts,
         lg_session_length = lg_listened_seconds) %>% 
  mutate(lg_sess_stop = lg_sess_start + seconds(lg_session_length)) %>% 
  arrange(lg_ipa, lg_usr_agt, lg_sess_start)

# assign an id to each session. itvl: "interval"
itvl01 <- cz_stats_rod.9 %>% 
  mutate(lg_sess_id = row_number()) %>% 
  select(lg_sess_id, everything())

# split in hourly segments ----
sessions_by_hour <- tibble(
  sbh.id = 0L,
  sbh.ts = ymd_hms("1970-01-01 00:00:00", tz = "Europe/Amsterdam"),
  sbh.length = 0L
)

for (sid in itvl01$lg_sess_id) {
  
  cur_sess <- itvl01 %>% filter(lg_sess_id == sid)
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
cz_stats_rod.10 <- sessions_by_hour %>%
  inner_join(itvl01, by = c("sbh.id" = "lg_sess_id")) %>%
  mutate(cz_row_id = row_number(),
         cz_cha_id = 99L,
         cha_name = "RoD") %>% 
  select(
    cz_row_id,
    cz_id = sbh.id,
    cz_ipa = lg_ipa,
    cz_dev_type = lg_device_type,
    cz_cha_id,
    cz_ts = sbh.ts,
    cz_length = sbh.length,
    cz_ref = lg_referrer,
    cha_name,
    pgm_title
  )
