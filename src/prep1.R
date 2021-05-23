# library(tidyr)
# library(dplyr)
# library(stringr)
# library(readr)
# library(lubridate)
# library(fs)
# library(futile.logger)
# library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

# Initialize resulting datastructure
cz_stats_cha <- tibble(
  lg_data_src = "s",
  lg_ip = "a",
  lg_usr_agt = "g",
  lg_cz_ts = ymd_hms("1970-01-01 01:02:03", tz = "Europe/Amsterdam"),
  lg_http_req = "c",
  lg_http_resp_sts = "d",
  lg_referrer = "r",
  lg_n_bytes = 0,
  lg_session_length = "l"
)


# # # # # # #   T E S T   O N L Y   # # # # # # # 
# adjust for test: use feb '21
cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/L_20210301_070000/", regexp = "access.+")
# # # # # # #   T E S T   O N L Y   # # # # # # # 

for (some_log in cz_log_files) {
  # some_log <- "/home/lon/Documents/cz_streaming_logs/access.log.1"
  ana_single <- analyze_log(some_log)
  cz_stats_cha <- bind_rows(cz_stats_cha, ana_single)
}

rm(ana_single)

saveRDS(cz_stats_cha, file = "cz_stats_cha.RDS")
