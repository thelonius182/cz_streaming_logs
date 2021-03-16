library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_ana.log"), "cz_ana_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/", regexp = "access.+")

ana_full <- tibble(
  lg_ip = "a",
  lg_cz_ts = ymd_hms("1970-01-01 01:02:03"),
  lg_http_req = "c",
  lg_http_resp_sts = "d",
  lg_n_bytes = "e",
  lg_some_url = "f",
  lg_usr_agt = "g",
  lg_data_src = "h"
)

analyze_log <- function(logfile) {
  # logfile <- "/home/lon/Documents/cz_streaming_logs/access.log.19"
  flog.info(paste0("log file: ", logfile), name = "cz_ana_log")

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
    mutate(lg_ip = fld1, cz_ts_raw = str_replace_all(fld4, "[\\[\\]]", '"'), lg_cz_ts = dmy_hms(cz_ts_raw), 
           lg_http_req = str_replace_all(fld6, "[\"]", ""), lg_http_resp_sts = fld7, lg_n_bytes = fld8, 
           lg_some_url = str_replace_all(fld9, "[\"]", ""), lg_usr_agt = str_replace_all(fld10, "[\"]", ""),
           lg_some_url = str_replace_all(lg_some_url, "-", NA_character_),
           lg_usr_agt = str_replace_all(lg_usr_agt, "-", NA_character_),
           lg_usr_agt = str_to_lower(lg_usr_agt)) %>% 
    select(starts_with("lg_")) %>%
    select(-lg_data_src, everything(), lg_data_src) %>% 
    arrange(lg_ip, lg_cz_ts)

  # verwijder mislukte requests ----
  cz_log.3 <- cz_log.2 %>%
    filter(!is.na(lg_n_bytes), lg_http_resp_sts == "200")
  
  # verwijder spiders/bots ----
  # cz_log.4 <- cz_log.3 %>% 
    # filter(str_detect(lg_usr_agt, pattern = "bot[\\s_ :,\\.\\;\\/\\-]")) %>% 
    # filter(str_detect(lg_usr_agt, pattern = "curl")) 
    # filter(str_detect(lg_usr_agt, pattern = "^mozilla/[45]\\.0$"))
    # filter(str_detect(lg_usr_agt, pattern = "steeler"))  # firefox/[0-9]\\.|[0-1][0]\\.
  # suppressMessages(
  #   cz_log.7 <- cz_log.6 %>%
  #     group_by(data_src, ip, title, cz_ts_ymd, cz_ts_h) %>%
  #     summarise(t_start = min(cz_ts), t_stop = max(cz_ts)) %>%
  #     mutate(cz_seconds = time_length(
  #       interval(start = t_start, end = t_stop), "seconds"
  #     )) %>%
  #     filter(cz_seconds > 300L)
  # )
  
  rm(cz_log.1,
     cz_log.2,
     access_log)
  
  return(cz_log.3)
}

for (some_log in cz_log_files) {
  ana_single <- analyze_log(some_log)
  ana_full <- bind_rows(ana_full, ana_single)
}

saveRDS(ana_full, file = "ana_full.RDS")
