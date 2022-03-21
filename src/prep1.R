
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

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

for (cur_log_file in cz_log_list$cz_log_list_path) {
  ana_single <- analyze_log(cur_log_file)
  cz_stats_cha <- bind_rows(cz_stats_cha, ana_single)
}

rm(ana_single)

cz_stats_cha.1 <- cz_stats_cha %>% 
  filter(month(lg_cz_ts) == month(cz_reporting_day_one))

flog.info(paste0("log line count = ", nrow(cz_stats_cha.1)), name = "cz_stats_proc_log")

write_rds(x = cz_stats_cha.1,
          file = paste0(stats_data_flr(), "cz_stats_cha.RDS"),
          compress = "gz")

rm(cz_stats_cha, cz_stats_cha.1)
