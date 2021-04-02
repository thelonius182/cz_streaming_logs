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

ana_full <- readRDS(file = "ana_full.RDS")


feb_2021 <- interval(ymd_hms("2021-02-01 00:00:00"), 
                     rollback(ymd_hms("2021-03-01 23:59:59"), 
                              preserve_hms = T), 
                     tzone = "Europe/Amsterdam")

# feb 2021 only
ana_2021.02 <- ana_full %>%
  filter(lg_ip != "a" 
         & lg_http_req != "c" 
         & !is.na(lg_http_req) 
         & lg_cz_ts %within% feb_2021
         & lg_http_resp_sts == "200" 
         & !is.na(lg_usr_agt)
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

ana_2021.02_A <- ana_2021.02 %>% 
  mutate(lg_bitrate = round(lg_n_bytes * 8 / 1024 / lg_session_length, 0)) %>% 

saveRDS(ana_2021.02_A, "ana_2021.02_A.RDS")

ana_2021.02_A <- readRDS(file = "ana_2021.02_A.RDS")

channels <- ana_2021.02_A %>% select(lg_cz_channel) %>% distinct() %>% 
  filter(!(str_detect(lg_cz_channel, pattern = "index|status|style|server|admin|test13|\\.(jpg|ico|png)") | str_length(str_trim(lg_cz_channel)) == 0))

ana_2021.02_B <- ana_2021.02_A %>% 
  # remove non-channel traffic
  filter(lg_cz_channel %in% channels$lg_cz_channel) %>% 
  mutate(lg_listened_secs = if_else(str_detect(lg_cz_channel, "\\blive\\b"), 
                                    round(lg_n_bytes * 8 / 1024 / 256, 0), 
                                    round(lg_n_bytes * 8 / 1024 / 128, 0))
  )
  
cz_stats.01 <- ana_2021.02 %>% 

# remove obvious bots
cz_stats.02 <- cz_stats.01 %>% 
  filter(!str_detect(lg_usr_agt,
                     pattern = "(bot|crawler|spider|checker|scanner|grabber|getter)[\\s_:,.;/)-]?")
                     # pattern = "(bot|spider)[\\s_:,.;/)-]")
  )
# 
# cz_bots.1 <- cz_stats.01 %>% 
#   filter(str_detect(lg_usr_agt, pattern = "^(curl|wget)"))
# 
# cz_bots.2 <- cz_stats.01 %>% 
#   filter(str_detect(lg_usr_agt,
#                     pattern = "(bot|crawler|spider|checker|scanner|grabber|getter)[\\s_:,.;/)-]")
#          )

# collect user-agents
# stats_ua <-
#   cz_stats.02 %>% group_by(lg_usr_agt) %>% 
#   summarise(n_visits = n(),
#             streamed_MB = round(sum(lg_n_bytes)/1024/1024, digits = 2))

# reproduce csv-example
cz_stats.03 <- cz_stats.02 %>% 
  mutate(lg_date = date(lg_cz_ts)) %>% 
  filter(lg_session_length >= 15L)

cz_stats.04 <- cz_stats.03 %>% 
  group_by(lg_date) %>% 
  summarise(n_valid_sessions = n())

write_delim(cz_stats.04, delim = "\t", file = "cz_stats_verify.tsv")

write_delim(channels, delim = "\t", file = "cz_stats_verify_channels.tsv")

cz_curl <- "curl 'https://freegeoip.app/json/51.91.219.191' \
  -H 'authority: freegeoip.app' \
  -H 'dnt: 1' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'user-agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
  -H 'sec-fetch-site: none' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-user: ?1' \
  -H 'sec-fetch-dest: document' \
  -H 'accept-language: nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7' \
  -H 'cookie: __cfduid=de21a991dc201214c6f5e3971f1d930621616846464' \
  --compressed  "

cz_straight <- straighten(cz_curl)

cz_res <- make_req(cz_straight, add_clip = F)

cz_geo <- toJSON(content(cz_res[[1]](), as="parsed"), auto_unbox = TRUE, pretty=TRUE)

