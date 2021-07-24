suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(ssh))

fa <-
  flog.appender(appender.file("/home/lon/Documents/cz_stats_rod.log"),
                "cz_stats_rod_log")

# Built by query on Nipper-pc, exported as .tsv
# C:\Users\nipper\Documents\cz_queries\salsa_stats_pgms.sql
salsa_stats_all_pgms_raw <-
  read_delim(
    "~/Downloads/salsa_stats_all_pgms.txt",
    delim = "\t",
    escape_double = FALSE,
    col_types = cols(pgmLang = col_skip()),
    trim_ws = TRUE
  )

cz_gids.1 <- salsa_stats_all_pgms_raw %>%
  mutate(
    pgm_start = round_date(ymd_h(pgmStart, tz = "Europe/Amsterdam"), unit = "30 minutes"),
    pgm_stop = round_date(ymd_h(pgmStop, tz = "Europe/Amsterdam"), unit = "30 minutes")
  ) %>%
  filter(pgm_start != pgm_stop) %>%
  select(pgm_start, pgm_stop, pgm_title = pgmTitle) %>%
  arrange(pgm_start)

write_rds(x = cz_gids.1,
          file = "cz_gids_1.RDS",
          compress = "gz")
