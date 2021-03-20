library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_ana.log"), "cz_ana_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/")

ana_full <- readRDS(file = "ana_full.RDS") 

feb_2021 <- interval(ymd("2021-02-01"), rollback(ymd("2021-03-01")))

# feb 2021 only
ana_2021.02 <- ana_full %>%
  filter(lg_cz_ts %within% feb_2021) %>% 
  # split http-request
  separate(lg_http_req, into = c("lg_http_cmd", "lg_cz_channel", "lg_http_protocol"), sep = " ") %>% 
  # "OK"-responses only
  filter(lg_http_resp_sts == "200") %>% 
  # drop redundant columns
  select(-lg_http_cmd, -lg_http_protocol, -lg_some_url, -lg_http_resp_sts) %>% 
  # clean-up channel names & bytes
  mutate(lg_cz_channel = str_replace_all(lg_cz_channel, pattern = "/", replacement = ""),
         lg_n_bytes = as.numeric(lg_n_bytes))

saveRDS(ana_2021.02, "ana_2021.02.RDS")

channels <- ana_2021.02 %>% select(lg_cz_channel) %>% distinct() %>% 
  filter(!(str_detect(lg_cz_channel, pattern = "[.]|admin|test13") | str_length(str_trim(lg_cz_channel)) == 0))

# remove non-channel traffic
cz_stats.01 <- ana_2021.02 %>% filter(lg_cz_channel %in% channels$lg_cz_channel)

# remove obvious bots
cz_stats.02 <- cz_stats.01 %>% 
  filter(!str_detect(lg_usr_agt, 
                     pattern = "(bot|crawler|spider|checker|scanner|grabber|getter|java|python)[\\s_:,.;/-]?")) %>% 
  filter(!str_detect(lg_usr_agt, pattern = "curl|wget"))

# collect user-agents
# stats_ua <-
#   cz_stats.02 %>% group_by(lg_usr_agt) %>% 
#   summarise(n_visits = n(),
#             streamed_MB = round(sum(lg_n_bytes)/1024/1024, digits = 2))

# reproduce csv-example
# regex "live"  streams on 320 kbps = 40 kB/s. 30 sec session duration = 1200 kB = 1200 * 1024 bytes
# regex !"live" streams on 160 kbps = 20 kB/s. 30 sec session duration =  600 kB =  600 * 1024 bytes
cz_stats.03 <- cz_stats.02 %>% 
  mutate(lg_date = date(lg_cz_ts),
         lg_valid_session = if_else(str_detect(lg_cz_channel, pattern = "live"),
                                    lg_n_bytes > 1228800,
                                    lg_n_bytes > 614400)) %>% 
  filter(lg_valid_session)

cz_stats.04 <- cz_stats.03 %>% 
  group_by(lg_date) %>% 
  summarise(n_valid_sessions = n())

write_delim(cz_stats.04, delim = "\t", file = "cz_stats_verify.tsv")

write_delim(channels, delim = "\t", file = "cz_stats_verify_channels.tsv")
