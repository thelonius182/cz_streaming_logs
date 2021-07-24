# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#
# Concertzender Monthly Webcast Metrics
# Version 0.1 - 2021-05-01, SJ/LA
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
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# get live pgms ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\oorboekje.sql
live_stream_pgms_raw <- read_csv("~/Downloads/live_stream_pgms.csv",
                                 col_types = cols(cz_id = col_integer(),
                                                  pgm_dtm = col_character(),
                                                  herh_van = col_skip(),
                                                  cz_id_herh = col_skip()
                                 )
)

# get theme channel (TC) playlists ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\themakanalen_2.sql
suppressWarnings(
  themakanalen_listed_raw <- read_csv("~/Downloads/themakanalen_listed.csv")
)

# get current TC-programs ----
# Built by query on Nipper-pc, exported as .csv
# C:\Users\nipper\Documents\cz_queries\themakanalen.sql
cur_pgms_snapshot_filename <- "~/Downloads/themakanalen_current_pgms.csv"
cur_pgms_snapshot <- read_csv(cur_pgms_snapshot_filename)

tc_cur_pgms <- cur_pgms_snapshot %>% 
  select(channel, current_program) %>% 
  mutate(cp_snap_ts = file_info(cur_pgms_snapshot_filename)$modification_time)


# # # # # # #   T E S T   O N L Y   # # # # # # # 
# adjust for test: set snap_ts to feb '21
month(tc_cur_pgms$cp_snap_ts) <- 2
# # # # # # #   T E S T   O N L Y   # # # # # # # 


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
