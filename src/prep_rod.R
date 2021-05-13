library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_rod.log"), "cz_stats_rod_log")

cz_log_files <- dir_ls(path = "/home/lon/Documents/cz-rod_logs/", regexp = "cz-rod.*")

cz_stats_rod <- tibble(
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
  logfile <- "/home/lon/Documents/cz-rod_logs/cz-rod.log"
  flog.info(paste0("log file: ", logfile), name = "cz_stats_rod_log")
  
  # inlezen ----  
  suppressMessages(
    rod_log <- read_delim(
      logfile,
      "\t",
      escape_double = FALSE,
      col_names = FALSE,
      trim_ws = TRUE, 
    )
  )
  
  Encoding(rod_log$X1) <- "UTF-8"
  rod_log$X1 <- iconv(rod_log$X1, "UTF-8", "UTF-8", sub = '')
  
  # clean it up & split ----
  # suppressWarnings(
    cz_log.1 <- rod_log %>%
      mutate(
        lg_data_src = path_file(logfile),
        details = gsub('("[^"\r\n]*")?"(?![^""\r\n]*"$)', "\\1¶", X1, perl=TRUE)
      ) %>%
      separate(
        col = details,
        into = paste0("fld", 1:6),
        sep = "¶"
      )
  # )
  
  cz_log.2 <- cz_log.1 %>% 
    separate(col = fld1,
             into = c("lg_ip", "ts_raw"),
             sep = " - ") %>% 
    mutate(cz_ts_raw = str_replace_all(ts_raw, "[\\[\\] +-]|0200|0100", ""), 
         lg_cz_ts = dmy_hms(cz_ts_raw)) %>% 
    separate(col = fld2,
             into = c("lg_http_cmd", "lg_http_req_b", "skip_01"),
             sep = " ") %>% 
    # separate(col = lg_http_req_b,
    #          into = paste0("lg_http_req_b_idx", 1:10),
    #          sep = "/") %>% 
    mutate(fld3 = str_trim(fld3, side = "both")) %>% 
    separate(col = fld3,
             into = c("lg_http_resp_sts", "lg_n_bytes"),
             sep = " ") %>% 
    mutate(lg_n_bytes = as.numeric(lg_n_bytes),
           lg_referrer = str_replace_all(fld4, "[\"]", ""), 
           lg_referrer = str_replace_all(lg_referrer, "-", NA_character_),
           lg_usr_agt = str_replace_all(fld5, "[\"]", ""),
           lg_usr_agt = str_replace_all(lg_usr_agt, "-", NA_character_),
           lg_usr_agt = str_to_lower(lg_usr_agt)
           ) %>% 
    select(starts_with("lg_"))
  
  idx2_unique <- cz_log.2 %>% 
    select(lg_http_req_b_idx2) %>% 
    group_by(lg_http_req_b_idx2) %>% 
    mutate(n_idx2 = n()) %>% 
    distinct()

  cz_log.3 <- cz_log.2 %>% 
    filter(lg_ip != "a" # remove dummy
           & lg_http_req != "c" # remove dummy
           & !is.na(lg_http_req) # non-empty http-requests only
           & lg_http_resp_sts %in% c("200", "206") # valid http-response
           & !is.na(lg_usr_agt) # non-empty user-agents only
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
