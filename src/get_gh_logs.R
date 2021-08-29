# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
# READ ME ----
# Get the Channel- and RoD-logs from Greenhost
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 
suppressPackageStartupMessages(library(R.utils))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(curlconverter))
suppressPackageStartupMessages(library(jsonlite))
suppressPackageStartupMessages(library(httr))
suppressPackageStartupMessages(library(ssh))

source(file = "src/prep_funcs.R", encoding = "UTF-8")

proc_gh_logs("Streams") # Themakanalen + Live-stream
proc_gh_logs("RoD")

# get known periods logged ----
cz_log_limits <- read_rds(file = "cz_log_limits.RDS")

# + get all dirs ----
cz_log_dirs <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/", regexp = "[RS]_.+", type = "directory")

for (cur_log_dir in cz_log_dirs) {
  # TEST_TEST_TEST_TEST
  # cur_log_dir <- "/home/lon/Documents/cz_streaming_logs/R_20210518_204228"
  # TEST_TEST_TEST_TEST
  
  # + skip the known dirs
  if (cur_log_dir %in% cz_log_limits$cz_log_dir) {
    flog.info(paste0(cur_log_dir, " already in cz_log_limits"), name = "cz_stats_proc_log")
    next
  }
  
  # + get log files in this dir ----
  cz_log_files <- dir_ls(path = cur_log_dir, type = "file", regexp = "access\\.log")
  
  for (cur_log_file in cz_log_files) {
    # TEST_TEST_TEST_TEST    
    # cur_log_file = "/home/lon/Documents/cz_streaming_logs/R_20210518_204228/access.log.12"
    # TEST_TEST_TEST_TEST
    
    flog.info(paste0("getting logged period in ", cur_log_file), name = "cz_stats_proc_log")
    
    cz_period_logged <- get_period_logged(cur_log_file)
    
    # cz_log_limits <- cz_period_logged # (init)
    cz_log_limits %<>% add_row(cz_period_logged)
    flog.info("logged period added to cz_log_limits", name = "cz_stats_proc_log")
  }
}

# store log_limits ----
write_rds(x = cz_log_limits,
          file = "cz_log_limits.RDS",
          compress = "gz")
