library(R.utils)
library(magrittr)
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
library(ssh)

# identify this run ----
gh_log_run_ymd = now(tzone = "Europe/Amsterdam")

# load log history ----
gh_logs_his <- read_rds(file = "/home/lon/Documents/cz_streaming_logs/gh_logs_history/gh_logs_his.RDS")

gh_logs_his %<>% filter(lg_ymd == "2021-04-13")

# + connect to greenhost ----
gh_sess <- ssh_connect("cz@streams.greenhost.nl")

# download current list ----
gh_logs <- 
  ssh_exec_internal(gh_sess, 
                    "ls /var/log/icecast2 -lt --time-style=+'%Y-%m-%d %H:%M:%S' | grep access.log | grep gz") %>%
  .[["stdout"]] %>%
  rawToChar() %>%
  strsplit("\n") %>%
  unlist() %>% 
  as_tibble()

# prep the list ----
gh_logs_tib.a <- gh_logs %>% 
  mutate(l_row_clean = str_squish(value), 
         l_row_clean = str_split(l_row_clean, pattern = " ", n = 8)) %>% 
  select(-value)

suppressMessages(
  gh_logs_tib.b <- unnest_wider(data = gh_logs_tib.a, col = l_row_clean)
)

names(gh_logs_tib.b) <- paste0("itm_", 1:8)

gh_logs_tib.c <- gh_logs_tib.b %>% 
  mutate(lg_run_ymd = gh_log_run_ymd) %>% 
  select(lg_ymd = itm_6, lg_hms = itm_7, lg_size = itm_5, lg_name = itm_8, lg_run_ymd) %>% 
  arrange(lg_ymd, lg_hms, lg_run_ymd)

# find new logs ----
gh_logs_new <- gh_logs_tib.c %>% 
  anti_join(y = gh_logs_his, by = c("lg_ymd" = "lg_ymd", "lg_hms" = "lg_hms", "lg_size" = "lg_size"))

# process new logs ----
if (nrow(gh_logs_new) == 0) {
  
  # disconnect from greenhost ----
  ssh_disconnect(session = gh_sess)
  
} else {
  
  # prep new dir
  gh_dir_leaf <- gh_log_run_ymd %>% str_replace_all("[:-]", "") %>% str_replace_all("[ ]", "_")
  gh_dir <- paste0("/home/lon/Documents/cz_streaming_logs/L_",gh_dir_leaf)
  dir_create(path = gh_dir)
  
  # download all
  for (cur_log in gh_logs_new$lg_name) {
    scp_download(session = gh_sess, 
                 files = paste0("/var/log/icecast2/", cur_log), 
                 to = gh_dir)
  }
  
  # disconnect from greenhost ----
  ssh_disconnect(session = gh_sess)
  
  # unpack logs ----
  for (cur_log in gh_logs_new$lg_name) {
    gunzip(paste0(gh_dir, "/", cur_log), remove = T)
  }
  
  # append new names to his
  gh_logs_his %<>% bind_rows(gh_logs_new)
  write_rds(x = gh_logs_his, 
            file = "/home/lon/Documents/cz_streaming_logs/gh_logs_history/gh_logs_his.RDS", 
            compress = "gz")
}
