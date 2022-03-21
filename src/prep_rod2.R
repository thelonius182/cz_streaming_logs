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
suppressPackageStartupMessages(library(yaml))

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - days(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + days(1), unit = "months")

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS")

# get list of log file names to process ----
cz_log_rod_list <- cz_log_limits %>% 
  filter(str_detect(cz_log_dir, "logs/R_") 
         & cz_ts_log >= cz_reporting_start
         & cz_ts_log <= cz_reporting_stop) %>% 
  mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) %>% 
  select(cz_log_list_path, cz_ts_log)

# analyze rod-loga arrived ----
  
cz_stats_rod.01 = NULL

for (cur_file in cz_log_rod_list$cz_log_list_path) {
  ana_single <- analyze_rod_log(cur_file)
  
  if (is.null(cz_stats_rod.01)) {
    cz_stats_rod.01 <- ana_single
  } else {
    cz_stats_rod.01 %<>% bind_rows(ana_single)
  }
}

rod_logs.1 <- cz_stats_rod.01 %>%
  mutate(
    lg_audio_dir = path_dir(lg_audio_file),
    lg_audio_file = path_file(lg_audio_file),
    pgmkey_prep = if_else(
      !str_ends(lg_audio_dir, pattern = "/cp") &
        str_starts(lg_audio_file, pattern = "\\d{8}-\\d{4}"),
      str_replace(lg_audio_file, pattern = "\\.mp3", replacement = ""),
      NA_character_
    ),
    lg_pgmkey = round_date(ymd_hm(pgmkey_prep, tz = "Europe/Amsterdam"), unit = "30 minutes")
  ) %>%
  select(lg_ipa,
         lg_usr_agt,
         everything(),
         -lg_audio_dir,
         -lg_audio_file,
         -pgmkey_prep) %>%
  arrange(lg_ipa, lg_usr_agt, lg_ts)

rod_logs.2 <- rod_logs.1 %>%
  mutate(
    clean_referrer = sub("https?://(www\\.)?(.*?/|.*?:|.*?\n)", "\\2", lg_ref, perl =
                           TRUE),
    clean_referrer = gsub("(.*?/|.*?:).*", "\\1", clean_referrer, perl =
                            TRUE),
    clean_referrer = str_replace_all(clean_referrer, ":\\d*|/|\\.$", ""),
    ua_type = case_when(
      str_detect(lg_usr_agt, "iphone|android|mobile|ipad")
      &
        str_starts(lg_usr_agt, "mozilla|opera") ~ "mobile-web",
      str_detect(lg_usr_agt, "iphone|android|mobile|ipad") ~ "mobile-app",
      str_detect(
        lg_usr_agt,
        "windows|darwin|macintosh|linux|curl|wget|python|java|gvfs"
      )
      &
        str_starts(lg_usr_agt, "mozilla|opera") ~ "desktop",
      str_detect(lg_usr_agt, "nsplayer|winamp|vlc") ~ "desktop",
      str_detect(
        lg_usr_agt,
        "play|itunes|vlc|foobar|lavf|winamp|radio|stream|apple tv|hd|tv|sonos|bose|ipod"
      )
      |
        !str_starts(lg_usr_agt, "mozilla|opera|internet|ices|curl|wget") ~ "smart-speaker",
      T ~ "overig"
    )
  ) %>%
  rename(lg_referrer = clean_referrer,
         lg_device_type = ua_type) %>%
  select(-lg_ref)

# write_rds(x = cz_rod_done,
#           file = cz_rod_done_path,
#           compress = "gz")
# 
rm(ana_single)

flog.info(paste0("log line count = ", nrow(rod_logs.2)), name = "cz_stats_proc_log")

write_rds(x = rod_logs.2,
          file = paste0(stats_data_flr(), "rod_logs_2.RDS"),
          compress = "gz")
