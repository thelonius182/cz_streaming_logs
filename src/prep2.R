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


fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (!exists(x = "cz_stats_cha")) {
  cz_stats_cha <- readRDS(file = "cz_stats_cha.RDS")
}

# feb_2021 <- interval(ymd_hms("2021-02-01 00:00:00"), 
#                      rollback(ymd_hms("2021-03-01 23:59:59"), 
#                               preserve_hms = T), 
#                      tzone = "Europe/Amsterdam")

# cleaning 1 ----
cz_stats_cha.01 <- cz_stats_cha %>%
  filter(lg_ip != "a" # remove dummy
         & lg_http_req != "c" # remove dummy
         & !is.na(lg_http_req) # non-empty http-requests only
         # & lg_cz_ts %within% feb_2021 # feb only
         & lg_http_resp_sts == "200" # valid http-response
         & !is.na(lg_usr_agt) # non-empty user-agents only
  ) %>%
  # split http-request
  separate(lg_http_req, 
           into = c("lg_http_cmd", "lg_cz_channel", "lg_http_protocol"), 
           sep = " ") %>%
  # drop redundant columns
  select(-lg_http_cmd, -lg_http_protocol, -lg_http_resp_sts) %>%
  # clean-up channel names & bytes
  mutate(lg_cz_channel = str_replace_all(lg_cz_channel, pattern = "/", replacement = ""),
         lg_n_bytes = as.numeric(lg_n_bytes))

saveRDS(cz_stats_cha.01, "cz_stats_cha.01.RDS")

