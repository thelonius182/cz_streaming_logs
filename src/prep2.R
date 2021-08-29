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

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

if (!exists(x = "cz_stats_cha")) {
  cz_stats_cha <- readRDS(file = "cz_stats_cha.RDS")
}

# + filter: part of TD-3.1 ----
cz_stats_cha.01 <- cz_stats_cha %>%
  filter(lg_data_src != "s" # remove artefact
         & !is.na(lg_http_req) # non-empty http-requests only
         & lg_http_resp_sts == "200" # valid http-responses only
  ) %>%
  # split http-request
  separate(lg_http_req, 
           into = c("lg_http_cmd", "lg_cz_channel", "lg_http_protocol"), 
           sep = " "
  ) %>% 
  # drop redundant columns
  select(-lg_http_cmd, -lg_http_protocol, -lg_http_resp_sts
  ) %>% 
  # clean-up channel names
  mutate(lg_cz_channel = str_replace_all(lg_cz_channel, pattern = "/|\\.m3u|\\.xspf", replacement = ""),
         # bytes should be numbers to do arithmetic
         lg_n_bytes = as.numeric(lg_n_bytes),
         # create data source index for sorting properly
         lg_src_idx = as.integer(str_replace_all(lg_data_src, "access\\.log\\.", "")),
         # session lengths should be numeric to do arithmetic
         lg_session_length_clean = str_replace_all(lg_session_length, "[^0-9]", ""),
         lg_session_length = if_else(str_length(lg_session_length_clean) > 0,
                                     as.integer(lg_session_length_clean),
                                     0L)
  )

rm(cz_stats_cha)

cz_stats_cha.01a <- cz_stats_cha.01 %>% 
  select(
    lg_src_idx,
    lg_ip,
    lg_cz_ts,
    lg_cz_channel,
    lg_usr_agt,
    lg_referrer,
    lg_n_bytes,
    lg_session_length
  ) %>% 
  arrange(lg_src_idx,
          lg_ip,
          lg_cz_ts,
          lg_cz_channel,
          lg_usr_agt
  )

rm(cz_stats_cha.01)

saveRDS(cz_stats_cha.01a, "cz_stats_cha.01a.RDS")
