# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#
# Concertzender Monthly Webcast Metrics
# Version 0.1 - 2021-05-01, SJ/LA
# Version 0.2 - 2021-08-19, SJ/LA
# Version 1.0 - 2022-03-21, SJ/LA
# Version 2.0 - 2022-10-22, SJ/LA
#
# Docs: docs.google.com/document/d/1vrwVwDFrxYJvcjXKxJkF7_R1uNgIE7T2KDOfPx_YIn8
#
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

pacman::p_load(magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, curlconverter,
               jsonlite, httr, yaml, ssh, googledrive)

# init logger ----
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

# load functions ----
# has func defs only
source("src/prep_funcs.R", encoding = "UTF-8")

# download wp-files from GD
source("src/download_wpfiles.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

flog.info(paste0("selecting logs for ", cz_reporting_day_one_chr), name = "cz_stats_proc_log")

# init stats directory
stats_flr <- paste0(cz_stats_cfg$stats_data_home, str_sub(cz_reporting_day_one_chr, 1, 7))

if (!dir_exists(stats_flr)) {
  dir_create(stats_flr)
  dir_create(paste0(stats_flr, "/diagrams"))
}

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS") # |> filter(!str_detect(cz_log_dir, "20231001"))

# get list of which log files to process ----
cz_log_list <- cz_log_limits %>% 
  filter(str_detect(cz_log_dir, "logs/S_") 
         & cz_ts_log >= cz_reporting_start
         & cz_ts_log <= cz_reporting_stop) %>% 
  mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) %>% 
  select(cz_log_list_path, cz_ts_log)

# Get gids pgms ----
# Built by a query on Nipper-pc, exported as .txt/tsv
# - C:\Users\nipper\Documents\cz_queries\salsa_stats_cz_gids.sql
# - C:\Users\nipper\Downloads\salsa_stats_all_pgms.txt
# copy to ubu_vm via Z370: /home/lon/Downloads/salsa_stats_all_pgms.txt
salsa_stats_all_pgms_raw <-
  read_delim("/mnt/muw/cz_stats_wpdata/salsa_stats_all_pgms.txt",
    delim = "\t",
    escape_double = FALSE,
    col_types = cols(pgmLang = col_skip()),
    trim_ws = TRUE,
    quote = ""
  ) %>% filter(pgmTitle != "NULL")

salsa_stats_all_pgms.1 <- salsa_stats_all_pgms_raw %>%
  mutate(
    tbh.id = row_number(),
    tbh.cha_id = 0,
    tbh.cha_name = "Live-stream",
    tbh.start = ymd_h(pgmStart, tz = "Europe/Amsterdam"),
    tbh.stop = ymd_h(pgmStop, tz = "Europe/Amsterdam"),
    tbh.secs = int_length(interval(tbh.start, tbh.stop)),
    tbh.title = str_replace(pgmTitle, "&amp;", "&"),
    tbh.editor = post_editor
  ) %>%
  select(starts_with("tbh.")) %>% 
  filter(tbh.secs > 0)

rm(salsa_stats_all_pgms_raw)
rds_file <- paste0(stats_data_flr(), "salsa_stats_all_pgms.1.RDS")
dir_create(path_dir(rds_file))
write_rds(x = salsa_stats_all_pgms.1,
          file = rds_file,
          compress = "gz")

# get theme channel (TC) playlists ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\themakanalen_2.sql
# themakanalen_listed_raw <- read_csv("~/Downloads/themakanalen_listed.csv")
  
themakanalen_listed_raw <-
  read_delim("/mnt/muw/cz_stats_wpdata/themakanalen_listed.txt",
    delim = "\t",
    escape_double = FALSE,
    locale = locale(encoding = "ISO-8859-1"),
    trim_ws = TRUE,
    quote = "",
    show_col_types = F,
    lazy = F
  )

# get current TC-programs ----
# Built by query on Nipper-pc, exported as themakanalen_current_pgms.csv
# C:\Users\nipper\Documents\cz_queries\themakanalen.sql
# "current" means "for this reporting period"!
cur_pgms_snapshot_filename <- "/mnt/muw/cz_stats_wpdata/themakanalen_current_pgms.txt"
cur_pgms_snapshot.1 <-
  read_delim(cur_pgms_snapshot_filename, 
             delim = "\t",escape_double = FALSE,
             trim_ws = TRUE, quote = "", show_col_types = F)
pgms_snapshot_info <- file_info(cur_pgms_snapshot_filename)
cur_pgms_snapshot <- cur_pgms_snapshot.1 %>% 
  mutate(ts_snapshot = pgms_snapshot_info$modification_time)

tc_cur_pgms <- cur_pgms_snapshot %>% 
  select(channel, current_program, cp_snap_ts = ts_snapshot)

# gather streaming log files ----
# stored in /home/lon/Documents/cz_streaming_logs/L_* ("logs Themakanalen + Live-stream")
# result: cz_stats_cha
# tibble(
#   lg_data_src: data lineage (current log file name)
#   lg_ip: client IP-address 
#   lg_usr_agt: client user agent 
#   lg_cz_ts = ymd_hms(..., tz = "Europe/Amsterdam"): timestamp log message 
#   lg_http_req: what the client requested
#   lg_http_resp_sts: server response 
#   lg_referrer: client request was mediated by this website
#   lg_n_bytes: bytes received by client
#   lg_session_length: sic, in seconds
# )
source("src/prep1.R", encoding = "UTF-8")

# apply Triton rules (1) ----
# result: cz_stats_cha.01a
source("src/prep2.R", encoding = "UTF-8")

# apply Triton rules (2) ----
# decode channel names in http-request
# result: cz_stats_cha.03
source("src/prep3.R", encoding = "UTF-8")

# apply Triton rules (3) ----
# decode device types in user agent
# result: cz_stats_cha.04
source("src/prep4.R", encoding = "UTF-8")

# link fragments of same session ----
# extract start/stop by linked fragment group
# remove partial fragments
# split in hourly segments
# result: cz_stats_cha.05 
source("src/prep5.R", encoding = "UTF-8")

# sync Caroussel to current reporting month ----
# NBNBNBNB remove test-value for FEB '21 NBNBNBNBNB
source("src/prep6.R", encoding = "UTF-8")

# split caroussel by hours ----
# join stats info and caroussel
# result: cz_stats_cha.07
source("src/prep7.R", encoding = "UTF-8")

# join live-pgms ----
# result: cz_stats_cha.08
source("src/prep8.R", encoding = "UTF-8")

# gather RoD log files ----
# result: rod_logs.2
source("src/prep_rod2.R", encoding = "UTF-8")

# select gh audio dirs ----
# collect mp3 details 
# result: cz_rod_audio_1.RDS
source("src/prep_rod3.R", encoding = "UTF-8")

# split in hourly segments ----
# result: cz_stats_rod.10.RDS
source("src/prep_rod4.R", encoding = "UTF-8")

# tmp_titles <- cz_stats_rod.10 %>% select(cz_title) %>% distinct()
# 
# tmp_titles_cha <- cz_stats_cha_08 %>% select(pgm_title, cz_cha_id, cha_name, cz_length) %>%
#   group_by(pgm_title, cz_cha_id, cha_name) %>%
#   summarise(sum_seconds = sum(cz_length)) %>%
#   mutate(sum_hours = round(sum_seconds / 3600, 1))
# 
# tmp_channels <- cz_stats_cha_08 %>% select(cz_cha_id, cha_name) %>% filter(is.na(cha_name)) %>% distinct()

# collect geo-data ----
source("src/ws_geodata.R", encoding = "UTF-8")

# convert times to country local A ----
source("src/prep_stats_df_03A.R", encoding = "UTF-8")

if(nrow(cz_stats_joined_04_missing) == 0) {

  # convert times to country local B ----
  source("src/prep_stats_df_03B.R", encoding = "UTF-8")
  
  # reports ----
  source("src/prep_stats_df_05.R", encoding = "UTF-8")
  source("src/cz_stats_plot_all_pgms.R", encoding = "UTF-8")
  source("src/cz_stats_plot_all_channels.R", encoding = "UTF-8")
  source("src/cz_stats_plot_hours_of_day_by_pgm.R", encoding = "UTF-8")
  # source("src/cz_stats_by_slot.R", encoding = "UTF-8")
  
  print("------------------ job completed successfully")
  
} else {
  
  print("------------------ job aborted - one or more titles are missing. Check tibble cz_stats_joined_04_missing")
  # vereiste match: GD-spreadsheet Luistercijfers verzendlijst 2.0 > verzendlijst > titel_stats 
  #            met: pgm_title_cleaner.tsv > pgmTtle_clean
  #
  # Na wijzigingen: herdraai het laatste stuk van main, dwz alles vanaf prep_stats_df_03A.R
  
}
