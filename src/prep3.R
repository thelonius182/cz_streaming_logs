# library(tidyr)
# library(dplyr)
# library(stringr)
# library(readr)
# library(lubridate)
# library(fs)
# library(futile.logger)
# library(curlconverter)
# library(jsonlite)
# library(httr)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

if (!exists(x = "cz_stats_cha.01a")) {
  cz_stats_cha.01a <- readRDS(file = paste0(stats_data_flr(), "cz_stats_cha.01a.RDS"))
}

# read channel info ----
channels <- read_delim(
  "~/Documents/chan_prep_final.csv",
  "\t",
  escape_double = FALSE,
  trim_ws = TRUE
)

# update channels ----
cz_stats_cha.02 <- cz_stats_cha.01a %>% 
  # + filter: part of TD-3.1 ----
  filter(lg_cz_channel %in% channels$slug) %>% # remove invalid ones
  left_join(channels, by = c("lg_cz_channel" = "slug")) %>% 
  select(-item, -lg_cz_channel, lg_channel = name, lg_channel_idx = id)

rm(cz_stats_cha.01a)  
rm(channels)  

# + filter: part of TD-3.1 ----
cz_stats_cha.03 <- cz_stats_cha.02 %>%   
  mutate(clean_referrer = sub("https?://(www\\.)?(.*?/|.*?:|.*?\n)", "\\2", lg_referrer, perl=TRUE),
         clean_referrer = gsub("(.*?/|.*?:).*", "\\1", clean_referrer, perl=TRUE),
         clean_referrer = str_replace_all(clean_referrer, ":\\d*|/|\\.$", ""),
         ua_type = case_when(str_detect(lg_usr_agt, "iphone|android|mobile|ipad") 
                             & str_starts(lg_usr_agt, "mozilla|opera") ~ "mobile-web",
                             str_detect(lg_usr_agt, "iphone|android|mobile|ipad") ~ "mobile-app",
                             str_detect(lg_usr_agt, "windows|darwin|macintosh|linux|curl|wget|python|java|gvfs")
                             & str_starts(lg_usr_agt, "mozilla|opera") ~ "desktop",
                             str_detect(lg_usr_agt, "nsplayer|winamp|vlc") ~ "desktop",
                             str_detect(lg_usr_agt, "play|itunes|vlc|foobar|lavf|winamp|radio|stream|apple tv|hd|tv|sonos|bose|ipod")
                             | !str_starts(lg_usr_agt, "mozilla|opera|internet|ices|curl|wget") ~ "smart-speaker",
                             T ~ "overig")
         ) %>% 
  select(-lg_referrer, lg_referrer = clean_referrer) %>% 
  select(
    lg_src_idx,
    lg_ip,
    lg_device_type = ua_type,
    lg_channel_idx,
    lg_channel,
    lg_cz_ts,
    # actually, we don need bytes; Triton uses session length
    # lg_n_bytes,
    lg_session_length,
    lg_referrer,
    lg_usr_agt
  ) %>% 
  arrange(lg_src_idx,
          lg_ip,
          lg_device_type,
          lg_channel_idx,
          lg_cz_ts)

rm(cz_stats_cha.02)  

saveRDS(cz_stats_cha.03, file = paste0(stats_data_flr(), "cz_stats_cha.03.RDS"))
