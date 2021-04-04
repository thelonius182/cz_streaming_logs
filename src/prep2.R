library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)
library(curlconverter)
library(jsonlite)
library(httr)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_ana.log"), "cz_ana_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/")

if (!exists(x = "ana_full")) {
  ana_full <- readRDS(file = "ana_full.RDS")
}

feb_2021 <- interval(ymd_hms("2021-02-01 00:00:00"), 
                     rollback(ymd_hms("2021-03-01 23:59:59"), 
                              preserve_hms = T), 
                     tzone = "Europe/Amsterdam")

# cleaning 1 ----
ana_2021.02 <- ana_full %>%
  filter(lg_ip != "a" # remove dummy
         & lg_http_req != "c" # remove dummy
         & !is.na(lg_http_req) # valid http-requests only
         & lg_cz_ts %within% feb_2021 # feb only
         & lg_http_resp_sts == "200" # valid http-response
         & !is.na(lg_usr_agt) # valid user-agents only
  ) %>%
  # split http-request
  separate(lg_http_req, 
           into = c("lg_http_cmd", "lg_cz_channel", "lg_http_protocol"), 
           sep = " ") %>%
  # drop redundant columns
  select(-lg_http_cmd, -lg_http_protocol, -lg_referrer, -lg_http_resp_sts) %>%
  # clean-up channel names & bytes
  mutate(lg_cz_channel = str_replace_all(lg_cz_channel, pattern = "/", replacement = ""),
         lg_n_bytes = as.numeric(lg_n_bytes))

saveRDS(ana_2021.02, "ana_2021.02.RDS")
