# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#
# Concertzender Monthly Webcast Metrics
# Version 0.1 - 2021-05-01, SJ/LA
# Version 0.2 - 2021-08-19, SJ/LA
#
# Docs: docs.google.com/document/d/1vrwVwDFrxYJvcjXKxJkF7_R1uNgIE7T2KDOfPx_YIn8
#
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(curlconverter))
suppressPackageStartupMessages(library(jsonlite))
suppressPackageStartupMessages(library(httr))
suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(ssh))

# init logger ----
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

flog.info("selecting this monhts logs", name = "cz_stats_proc_log")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - days(1)
cz_reporting_stop <- cz_reporting_day_one + months(1) 

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS")

# get list of log file names to process ----
cz_log_list <- cz_log_limits %>% 
  filter(str_detect(cz_log_dir, "logs/S_") 
         & cz_ts_log >= cz_reporting_start
         & cz_ts_log <= cz_reporting_stop) %>% 
  mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) %>% 
  select(cz_log_list_path, cz_ts_log)

# Get gids pgms ----
# Built by a query on Nipper-pc, exported as .txt/tsv
# - C:\Users\nipper\Documents\cz_queries\salsa_stats_cz_gids.sql
# - C:\Users\nipper\Downloads\cz_downloads\salsa_stats_all_pgms.txt
# copy to ubu_vm via Z370: /home/lon/Downloads/salsa_stats_all_pgms.txt
salsa_stats_all_pgms_raw <-
  read_delim(
    "~/Downloads/salsa_stats_all_pgms.txt",
    delim = "\t",
    escape_double = FALSE,
    col_types = cols(pgmLang = col_skip()),
    trim_ws = TRUE
  )

salsa_stats_all_pgms.1 <- salsa_stats_all_pgms_raw %>%
  mutate(
    tbh.id = row_number(),
    tbh.cha_id = 0,
    tbh.cha_name = "Live-stream",
    tbh.start = ymd_h(pgmStart, tz = "Europe/Amsterdam"),
    tbh.stop = ymd_h(pgmStop, tz = "Europe/Amsterdam"),
    tbh.secs = int_length(interval(tbh.start, tbh.stop)),
    tbh.title = str_replace(pgmTitle, "&amp;", "&")
  ) %>%
  select(starts_with("tbh.")) %>% 
  filter(tbh.secs > 0)

rm(salsa_stats_all_pgms_raw)

# get theme channel (TC) playlists ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\themakanalen_2.sql
suppressWarnings(
  themakanalen_listed_raw <- read_csv("~/Downloads/themakanalen_listed.csv")
)

# get current TC-programs ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\themakanalen.sql
# "current" means "for this reporting period"!
cur_pgms_snapshot_filename <- "~/Downloads/themakanalen_current_pgms_20210517.csv"
cur_pgms_snapshot <- read_delim(cur_pgms_snapshot_filename, delim = ",") %>% 
  mutate(ts_snapshot = ymd_hms("2021-05-17 11:22:09", tz = "Europe/Amsterdam"))
# cur_pgms_snapshot_filename <- "~/Downloads/themakanalen_current_pgms_20210820.txt"
# cur_pgms_snapshot <- read_delim(cur_pgms_snapshot_filename, delim = "\t")

tc_cur_pgms <- cur_pgms_snapshot %>% 
  select(channel, current_program, cp_snap_ts = ts_snapshot)

# adjust month for this report
# month(tc_cur_pgms$cp_snap_ts) <- 5


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

# apply Triton rules (1)
# result: cz_stats_cha.01a
source("src/prep2.R", encoding = "UTF-8")

# apply Triton rules (2)
# decode channel names in http-request
# result: cz_stats_cha.03
source("src/prep3.R", encoding = "UTF-8")

# apply Triton rules (3)
# decode device types in user agent
# result: cz_stats_cha.04
source("src/prep4.R", encoding = "UTF-8")

# link fragments of same session
# extract start/stop by linked fragment group
# remove partial fragments
# split in hourly segments
# result: cz_stats_cha.05 
source("src/prep5.R", encoding = "UTF-8")

# sync Caroussel to current reporting month
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
# result: cz_stats_rod.01
source("src/prep_rod1.R", encoding = "UTF-8")

tmp_titles <- cz_stats_rod.10 %>% select(cz_title) %>% distinct()

tmp_titles_cha <- cz_stats_cha_08 %>% select(pgm_title, cz_cha_id, cha_name, cz_length) %>% 
  group_by(pgm_title, cz_cha_id, cha_name) %>% 
  summarise(sum_seconds = sum(cz_length)) %>% 
  mutate(sum_hours = round(sum_seconds / 3600, 1))

tmp_channels <- cz_stats_cha_08 %>% select(cz_cha_id, cha_name) %>% filter(is.na(cha_name)) %>% distinct()
