# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =
#
# Concertzender Monthly Webcast Metrics
# Version 0.1 - 2021-05-01, SJ/LA
# Version 0.2 - 2021-08-19, SJ/LA
# Version 1.0 - 2022-03-21, SJ/LA
#
# Docs: docs.google.com/document/d/1vrwVwDFrxYJvcjXKxJkF7_R1uNgIE7T2KDOfPx_YIn8
#
# = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = = =

suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
# suppressPackageStartupMessages(library(curlconverter))
# suppressPackageStartupMessages(library(jsonlite))
# suppressPackageStartupMessages(library(httr))
suppressPackageStartupMessages(library(yaml))
# suppressPackageStartupMessages(library(ssh))

# init logger ----
# fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

# load functions ----
# has func defs only
source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_cfg <- read_yaml("config.yaml")

# get start from config file----
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

# flog.info(paste0("selecting logs for ", cz_reporting_day_one_chr), name = "cz_stats_proc_log")

# init stats directory
stats_flr <- paste0(cz_stats_cfg$stats_data_home, str_sub(cz_reporting_day_one_chr, 1, 7))

stats_report.1 <- read_rds(file = paste0(stats_data_flr(), "cz_licharod_stats_pgm_report.1.RDS")) 
