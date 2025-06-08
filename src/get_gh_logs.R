# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# READ ME ----
# Get the Channel- and RoD-logs from Greenhost
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
pacman::p_load(R.utils, magrittr, tidyr, dplyr, stringr, readr, lubridate, fs,
               futile.logger, jsonlite, httr, ssh)

# init logger ----
fa <- flog.appender(appender.file("~/ghlogs/cz_stats_proc.log"), "cz_stats_proc_log")

source(file = "src/prep_funcs.R", encoding = "UTF-8")

proc_gh_logs("Streams") # Themakanalen + Live-stream
proc_gh_logs("RoD")

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS")

# + get all dirs ----
flog.info(">>> START checking the logs on greenhost", name = "cz_stats_proc_log")

cz_log_dirs <- dir_ls(path = "c:/Users/nipper/Documents/ghlogs/cz_streaming_logs/", regexp = "[RS]_.+", type = "directory")

for (cur_log_dir in cz_log_dirs) {

  # + skip the known dirs
  if (cur_log_dir %in% cz_log_limits$cz_log_dir) {
    # flog.info(paste0(cur_log_dir, " already in cz_log_limits"), name = "cz_stats_proc_log")
    next
  }

  # + get log files in this dir ----
  cz_log_files <- dir_ls(path = cur_log_dir, type = "file", regexp = "access\\.log")

  for (cur_log_file in cz_log_files) {

    flog.info(paste0("getting logged period in ", cur_log_file), name = "cz_stats_proc_log")

    cz_period_logged <- get_period_logged(cur_log_file)

    # cz_log_limits <- cz_period_logged # (init)
    cz_log_limits %<>% add_row(cz_period_logged)
    cat("The log file starting at", as.character(cz_period_logged$cz_ts_log), "was fetched, size is",
        as.character(file_size(cur_log_file)), "\n")
  }
}

# store log_limits ----
write_rds(x = cz_log_limits, file = "cz_log_limits.RDS", compress = "gz")

flog.info(">>> job finished", name = "cz_stats_proc_log")
