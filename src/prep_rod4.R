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

log_audio.1 <- rod_logs %>% 
  inner_join(cz_audio, by = c("lg_pgmkey" = "key_tk_ymd")) %>% 
  select(lg_ipa, lg_usr_agt, lg_pgmkey, lg_ts, lg_sts, lg_size, audio_size, everything()) 

log_audio.2 <- log_audio.1 %>% 
  inner_join(cz_gids, by = c("lg_pgmkey" = "pgm_start")) %>% 
  mutate(pgm_minutes = as.numeric(interval(lg_pgmkey, pgm_stop), "minutes"))

log_audio.3 <- log_audio.2 %>% 
  mutate(tmp_bytes_per_minute = audio_size / pgm_minutes,
         tmp_frag_minutes = lg_size / tmp_bytes_per_minute) 

log_audio.4 <- log_audio.3 %>% 
  group_by(lg_ipa, lg_usr_agt, lg_pgmkey) %>% 
  summarize(bc_key = row_number(),
            tmp_total_minutes = sum(tmp_frag_minutes)) %>% 
  mutate(tmp_total_minutes = round(tmp_total_minutes, 0)) %>% 
  select(-bc_key) %>% 
  ungroup() %>% 
  distinct()

log_audio.5 <- log_audio.3 %>% 
  inner_join(log_audio.4) 

log_audio.6 <- log_audio.5 %>% 
  select(-tmp_frag_minutes, -tmp_bytes_per_minute, -lg_size, -key_rowNum, -lg_sts) %>% 
  distinct()

log_audio.7 <- log_audio.6 %>% 
  mutate(lg_listened_minutes = if_else(tmp_total_minutes <= pgm_minutes,
                                       tmp_total_minutes,
                                       pgm_minutes)) %>% 
  group_by(lg_ipa, lg_usr_agt, lg_pgmkey) %>% 
  mutate(bc_key = row_number()) %>% 
  filter(bc_key == 1)

log_audio.8 <- log_audio.7 %>% 
  select(-audio_size, -audio_file, -key_tk_dir, -pgm_stop, -pgm_minutes, -tmp_total_minutes, -bc_key) %>% 
  filter(lg_listened_minutes >= 2)
