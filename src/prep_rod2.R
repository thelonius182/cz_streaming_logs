# suppressPackageStartupMessages(library(tidyr))
# suppressPackageStartupMessages(library(dplyr))
# suppressPackageStartupMessages(library(stringr))
# suppressPackageStartupMessages(library(readr))
# suppressPackageStartupMessages(library(lubridate))
# suppressPackageStartupMessages(library(fs))
# suppressPackageStartupMessages(library(futile.logger))
# suppressPackageStartupMessages(library(tibble))
# suppressPackageStartupMessages(library(magrittr))
# suppressPackageStartupMessages(library(ssh))
# suppressPackageStartupMessages(library(yaml))
 
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")
 
# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS")

# get list of log file names to process ----
cz_log_rod_list <- cz_log_limits %>% 
  filter(str_detect(cz_log_dir, "logs/R_") 
         & cz_ts_log >= cz_reporting_start
         & cz_ts_log <= cz_reporting_stop) %>% 
  mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) %>% 
  select(cz_log_list_path, cz_ts_log)

# analyze rod-loga arrived ----
cz_stats_rod.01 = tibble()

for (cur_file in cz_log_rod_list$cz_log_list_path) {
  cat("logfile:", cur_file, "\n")
  ana_single <- analyze_rod_log(cur_file)
  cz_stats_rod.01 <- bind_rows(cz_stats_rod.01, ana_single)
}

rod_logs.1 <- cz_stats_rod.01 %>%
  filter(month(lg_ts) == month(cz_reporting_day_one)) |> 
  mutate(audio_path = path_dir(lg_audio_file),
         stream = case_when(audio_path == "/cz/cz/rod" ~ "on_demand_cz",
                            audio_path == "/cz_rod/woj" ~ "on_demand_woj",
                            audio_path == "/cz_rod/cp" ~ "concertpodium",
                            TRUE ~ "on_demand_cz"),
         d = 120L)  |> 
  select(ip_address = lg_ipa, ts = lg_ts, d, stream)

rod_stream_order <- c("on_demand_cz", "on_demand_woj", "concertpodium")

cz_wj_rod_counts <- count_listeners(log_data = rod_logs.1) |> filter(session_hour < cutoff) 
cz_wj_rod_counts_sorted <- cz_wj_rod_counts |> mutate(stream = factor(stream, levels = rod_stream_order)) |> 
  arrange(session_hour, stream)

merged_stream_order = c(stream_order, rod_stream_order)
cz_wj_merged_counts <- cz_wj_counts_sorted |> bind_rows(cz_wj_rod_counts_sorted) |> 
  mutate(stream = factor(stream, levels = merged_stream_order)) |> 
  arrange(session_hour, stream)

cz_wj_merged_counts_labelled_tz <- cz_wj_rod_counts_sorted |> 
  mutate(session_hour = format(session_hour, tz = "Europe/Amsterdam", usetz = TRUE)) 

# rod_logs.2 <- rod_logs.1 %>%
#   mutate(clean_referrer = sub("https?://(www\\.)?(.*?/|.*?:|.*?\n)", "\\2", lg_ref, perl = TRUE),
#          clean_referrer = gsub("(.*?/|.*?:).*", "\\1", clean_referrer, perl = TRUE),
#          clean_referrer = str_replace_all(clean_referrer, ":\\d*|/|\\.$", ""),
#          ua_type = case_when(str_detect(lg_usr_agt, "iphone|android|mobile|ipad") & 
#                                str_starts(lg_usr_agt, "mozilla|opera")                                          ~ "mobile-web",
#                              str_detect(lg_usr_agt, "iphone|android|mobile|ipad")                               ~ "mobile-app",
#                              str_detect(lg_usr_agt, "windows|darwin|macintosh|linux|curl|wget|python|java|gvfs") &
#                                str_starts(lg_usr_agt, "mozilla|opera")                                          ~ "desktop",
#                              str_detect(lg_usr_agt, "nsplayer|winamp|vlc")                                      ~ "desktop",
#                              str_detect(lg_usr_agt, 
#                                         "play|itunes|vlc|foobar|lavf|winamp|radio|stream|apple tv|hd|tv|sonos|bose|ipod") |
#                                !str_starts(lg_usr_agt, "mozilla|opera|internet|ices|curl|wget")                 ~ "smart-speaker",
#                              T                                                                                  ~ "overig"))  |> 
#   rename(lg_referrer = clean_referrer, lg_device_type = ua_type) |> 
#   mutate(bc_src = if_else(str_detect(lg_audio_dir, "woj"), "WJ", "CZ")) |> 
#   select(-lg_ref)
# 
# rm(ana_single)
# 
# flog.info(paste0("log line count = ", nrow(rod_logs.2)), name = "cz_stats_proc_log")
# 
# write_rds(x = rod_logs.2,
#           file = paste0(stats_data_flr(), "rod_logs_2.RDS"),
#           compress = "gz")
# 
# ipa_cha_cz <- cz_stats_cha.1 |> filter(bc_src == "CZ") |> select(ipa = lg_ip) |> distinct()
# ipa_rod_cz <- rod_logs.2 |> filter(bc_src == "CZ" &str_detect(lg_usr_agt, "radio|bot|crawl", negate = T)) |> 
#   select(ipa = lg_ipa) |> distinct()
#   
# ipa_cha_wj <- cz_stats_cha.1 |> filter(bc_src == "WJ") |> select(ipa = lg_ip) |> distinct()
# ipa_rod_wj <- rod_logs.2 |> filter(bc_src == "WJ" &str_detect(lg_usr_agt, "radio|bot|crawl", negate = T)) |> 
#   select(ipa = lg_ipa) |> distinct()
# 
# ipa_all_cz <- bind_rows(ipa_cha_cz, ipa_rod_cz) |> distinct()
# ipa_all_wj <- bind_rows(ipa_cha_wj, ipa_rod_wj) |> distinct()
# 
# cat("ipa_all CZ =", nrow(ipa_all_cz), "\n")
# cat("ipa_all WJ =", nrow(ipa_all_wj))
# 
# summ_a <- cz_stats_cha.1 |> select(lg_http_req, bc_src, frag_seconds) |>  filter(bc_src == "WJ") |> 
#   group_by(lg_http_req) |> mutate(bc_len = round(sum(frag_seconds) / 3600, 0)) |> select(-frag_seconds) |> distinct()

# create hourly titles from nipper-pc query cz_wj_stats_hourly_titles.sql in 
# C:\Users\nipper\Documents\cz_queries
hourly_titles_raw <- read_tsv("/home/lon/Documents/cz_stats_data/2025-01/hourly_titles.tsv", 
                              col_names = c("pgm_start", "pgm_stop", "pgm_title", "post_type"),
                              col_types = cols(.default = "c"))

# Expand each row by pivotting start/stop to 1-hour rows
hourly_titles <- hourly_titles_raw |> rowwise() |> do(expand_hourly(pgm_start = .$pgm_start,
                                                                    pgm_stop = .$pgm_stop,
                                                                    pgm_title = .$pgm_title,
                                                                    post_type = .$post_type)) |> ungroup() 
# replace html-entity for ampersand by regular ´&'
hourly_titles <- hourly_titles |> 
  mutate(pgm_title = str_replace(pgm_title, pattern = "&amp;", replacement = "&"),
         station = if_else(post_type == "programma", "cz_live_stream", "woj_live_stream")) |> 
  select(-post_type)

cz_monthly_stats <- cz_wj_merged_counts |> 
  left_join(hourly_titles, by = join_by(session_hour == pgm_start, stream == station))

cz_monthly_stats_tz <- cz_monthly_stats |> 
  mutate(session_hour = format(session_hour, tz = "Europe/Amsterdam", usetz = TRUE))

write_tsv(x = cz_monthly_stats_tz, 
          file = "/home/lon/Documents/cz_stats_data/2025-01/cz_stats.tsv", na = "", append = FALSE)
