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

if (!exists(x = "ana_2021.02")) {
  ana_2021.02 <- readRDS(file = "ana_2021.02.RDS")
}

# get CZ theme channels
channels <- ana_2021.02 %>% select(lg_cz_channel) %>% distinct() %>% 
  filter(!(str_detect(lg_cz_channel, 
                      pattern = "index|status|style|server|admin|test13|\\.(jpg|ico|png)") 
           | str_length(str_trim(lg_cz_channel)) == 0)
         )

# cleaning 2 ----
cz_stats.01 <- ana_2021.02 %>% 
  # remove non-channel traffic
  filter(lg_cz_channel %in% channels$lg_cz_channel) %>% 
  # remove obvious bots
  filter(!str_detect(lg_usr_agt,
                     pattern = "(bot|crawler|spider|checker|scanner|grabber|getter)[\\s_:,.;/)-]?")
  )

# infer seconds listened using 256 kBps for live-stream and 128 kBps for others
# and apply StreamAnalyst Tuning Hours Adjustment (SATHA) derived from "SA validations.ods"
SATHA <- 0.972
cz_stats.01a <- cz_stats.01 %>% 
  mutate(lg_secs_listened = if_else(str_detect(lg_cz_channel, "\\blive\\b"), 
                                    round(lg_n_bytes * SATHA * 8 / 1024 / 256, 0), 
                                    round(lg_n_bytes * SATHA * 8 / 1024 / 128, 0)
                                    )
  )

# # skip listening sessions < 30 secs
cz_stats.01b <- cz_stats.01a %>%
  filter(lg_secs_listened > 30L)

# calculate time-attributes
bin_size <- 6 * 60 * 60 # n_secs in 6 hours
cz_stats.02a <- cz_stats.01b %>% 
  mutate(lg_lstn_start = lg_cz_ts,
         lg_lstn_start_bin = lg_lstn_start,
         lg_lstn_stop = lg_lstn_start + seconds(lg_secs_listened),
         lg_lstn_stop_bin = lg_lstn_stop)

minute(cz_stats.02a$lg_lstn_start_bin) <- 0
second(cz_stats.02a$lg_lstn_start_bin) <- 0
hour(cz_stats.02a$lg_lstn_start_bin) <- case_when(hour(cz_stats.02a$lg_lstn_start) < 6 ~ 3,
                                              hour(cz_stats.02a$lg_lstn_start) < 12 ~ 9,
                                              hour(cz_stats.02a$lg_lstn_start) < 18 ~ 15,
                                              T ~ 21)

minute(cz_stats.02a$lg_lstn_stop_bin) <- 0
second(cz_stats.02a$lg_lstn_stop_bin) <- 0
hour(cz_stats.02a$lg_lstn_stop_bin) <- case_when(hour(cz_stats.02a$lg_lstn_stop) < 6 ~ 3,
                                              hour(cz_stats.02a$lg_lstn_stop) < 12 ~ 9,
                                              hour(cz_stats.02a$lg_lstn_stop) < 18 ~ 15,
                                              T ~ 21)

cz_stats.02b <- cz_stats.02a %>% 
  mutate(lg_n_bins = 1 + int_length(lg_lstn_start_bin %--% lg_lstn_stop_bin) / bin_size,
         lg_secs_listened_by_bin = round(lg_secs_listened / lg_n_bins, 0),
         lg_bin_start_date = date(lg_lstn_start_bin),
         lg_bin_start_idx = (hour(lg_lstn_start_bin) - 3) / 6)

# 
# cz_stats.04 <- cz_stats.03 %>% 
#   group_by(lg_date) %>% 
#   summarise(n_sessions = n())
# 
# write_delim(cz_stats.04, delim = "\t", file = "cz_stats_n_sessions.tsv")
# 
# cz_stats.05 <- cz_stats.03 %>% 
#   group_by(lg_date) %>% 
#   summarise(n_hrs_listened = sum(lg_secs_listened) / 3600)
# 
# write_delim(cz_stats.05, delim = "\t", file = "cz_stats_hrs_listened.tsv")
# 
# cz_stats.06 <- cz_stats.03 %>% 
#   filter(lg_cz_channel == "jazznotjazz") %>% 
#   group_by(lg_date) %>% 
#   summarise(n_hrs_listened_ = sum(lg_secs_listened) / 3600)
# 
# write_delim(cz_stats.06, delim = "\t", file = "cz_stats_hrs_lstnd_jnj.tsv")
# 
# cz_stats.07 <- cz_stats.03 %>% 
#   filter(lg_cz_channel == "jazznotjazz") %>% 
#   group_by(lg_date) %>% 
#   summarise(n_hrs_listened_ = sum(lg_secs_listened) / 3600)
# 
# write_delim(cz_stats.06, delim = "\t", file = "cz_stats_hrs_lstnd_jnj.tsv")
# 
# write_delim(channels, delim = "\t", file = "cz_stats_verify_channels.tsv")
# 
# cz_curl <- "curl 'https://freegeoip.app/json/51.91.219.191' \
#   -H 'authority: freegeoip.app' \
#   -H 'dnt: 1' \
#   -H 'upgrade-insecure-requests: 1' \
#   -H 'user-agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0' \
#   -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
#   -H 'sec-fetch-site: none' \
#   -H 'sec-fetch-mode: navigate' \
#   -H 'sec-fetch-user: ?1' \
#   -H 'sec-fetch-dest: document' \
#   -H 'accept-language: nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7' \
#   -H 'cookie: __cfduid=de21a991dc201214c6f5e3971f1d930621616846464' \
#   --compressed  "
# 
# cz_straight <- straighten(cz_curl)
# 
# cz_res <- make_req(cz_straight, add_clip = F)
# 
# cz_geo <- toJSON(content(cz_res[[1]](), as="parsed"), auto_unbox = TRUE, pretty=TRUE)
# 
