suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(tibble))

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_rod.log"), "cz_stats_rod_log")

# load log_done history ----
cz_rod_done_path <- "/home/lon/Documents/cz_streaming_logs/gh_logs_history/cz_rod_done.RDS"

cz_rod_done <- NULL

# collect RoD-logs ----
cz_log_files <- dir_ls(path = "/home/lon/Documents/cz_streaming_logs/", recurse = T, type = "file",
                       regexp = ".*/R_[^/]+/access[.]log[.].*") %>% as_tibble() 

# if there is history: compare ----
if (file_exists(cz_rod_done_path)) {
  cz_rod_done <- read_rds(file = cz_rod_done_path)
  cz_log_files %<>% anti_join(cz_rod_done, by = c("value" = "rod_path"))
}

# if new files arrived ----
if (nrow(cz_log_files) > 0) {
  
  cz_stats_rod.1 = NULL
  
  for (a_path in cz_log_files$value) {
    # a_path <- "/home/lon/Documents/cz_streaming_logs/R_20210518_204228/access.log.9"
    ana_single <- analyze_rod_log(a_path)
    
    if (is.null(cz_stats_rod.1)) {
      cz_stats_rod.1 <- ana_single
    } else {
      cz_stats_rod.1 %<>% bind_rows(ana_single)
    }
    
    cz_rod_current <- tibble(rod_path = a_path)
    
    if (is.null(cz_rod_done)) {
      cz_rod_done <- cz_rod_current
    } else {
      cz_rod_done %<>% add_row(cz_rod_current)
    }
    
  }
  
  write_rds(x = cz_rod_done,
            file = cz_rod_done_path,
            compress = "gz")
  
  rm(ana_single)
  
  saveRDS(cz_stats_rod.1, file = "cz_stats_rod.1.RDS")
}
