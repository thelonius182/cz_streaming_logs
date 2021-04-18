library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/", regexp = "access.+")

cz_stats_cha <- tibble(
  lg_ip = "a",
  lg_cz_ts = ymd_hms("1970-01-01 01:02:03"),
  lg_http_req = "c",
  lg_http_resp_sts = "d",
  lg_n_bytes = "e",
  lg_referrer = "f",
  lg_usr_agt = "g",
  lg_data_src = "h"
)

analyze_log <- function(logfile) {
  # logfile <- "/home/lon/Documents/cz_streaming_logs/access.log.7"
  flog.info(paste0("log file: ", logfile), name = "cz_stats_cha_log")

  # inlezen ----  
  suppressMessages(
    access_log <- read_delim(
      logfile,
      "\t",
      escape_double = FALSE,
      col_names = FALSE,
      trim_ws = TRUE, 
    )
  )
  
Encoding(access_log$X1) <- "UTF-8"
access_log$X1 <- iconv(access_log$X1, "UTF-8", "UTF-8", sub = '')

  # clean it up & split ----
  suppressWarnings(
    cz_log.1 <- access_log %>%
      mutate(
        lg_data_src = path_file(logfile),
        details = gsub('("[^"\r\n]*")? (?![^" \r\n]*"$)', "\\1¶", X1, perl = TRUE)
      ) %>%
      separate(
        col = details,
        into = paste0("fld", 1:11),
        sep = "¶"
      )
  )
  
  cz_log.2 <- cz_log.1 %>% 
    mutate(lg_ip = fld1, 
           cz_ts_raw = str_replace_all(fld4, "[\\[\\]]", '"'), 
           lg_cz_ts = dmy_hms(cz_ts_raw), 
           lg_http_req = str_replace_all(fld6, "[\"]", ""), 
           lg_http_resp_sts = fld7, 
           lg_n_bytes = fld8, 
           lg_referrer = str_replace_all(fld9, "[\"]", ""), 
           lg_referrer = str_replace_all(lg_referrer, "-", NA_character_),
           lg_usr_agt = str_replace_all(fld10, "[\"]", ""),
           lg_usr_agt = str_replace_all(lg_usr_agt, "-", NA_character_),
           lg_usr_agt = str_to_lower(lg_usr_agt),
           lg_session_length = as.integer(fld11)
    ) %>% 
    select(starts_with("lg_")) %>%
    select(-lg_data_src, everything(), lg_data_src) %>% 
    arrange(lg_ip, lg_cz_ts)

  rm(cz_log.1,
     access_log)
  
  return(cz_log.2)
}

for (some_log in cz_log_files) {
  ana_single <- analyze_log(some_log)
  cz_stats_cha <- bind_rows(cz_stats_cha, ana_single)
}

rm(ana_single)

saveRDS(cz_stats_cha, file = "cz_stats_cha.RDS")
