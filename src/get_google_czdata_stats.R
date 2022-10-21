library(googledrive)
library(keyring)
library(readxl)
library(yaml)

options(
  gargle_oauth_cache = ".secrets",
  gargle_oauth_email = "cz.teamservice@gmail.com"
)

cz_extract_sheet <- function(ss_name, sheet_name) {
  read_xlsx(ss_name,
            sheet = sheet_name,
            .name_repair = ~ ifelse(nzchar(.x), .x, LETTERS[seq_along(.x)]))
}

cz_get_url <- function(cz_ss) {
  cz_url <- paste0("url_", cz_ss)
  
  # use [[ instead of $, because it is a variable, not a constant
  paste0("https://", config$url_pfx, config[[cz_url]]) 
}

config <- read_yaml("config.yaml")

# downloads GD ------------------------------------------------------------

# aanmelden bij GD loopt via de procedure die beschreven is in "Operation Missa > Voortgang > 4.Toegangsrechten GD".

# Roosters 3.0 ophalen bij GD
path_roosters <- paste0(config$gs_downloads_ubu, "/", "roosters.xlsx")
drive_download(file = cz_get_url("roosters"), overwrite = T, path = path_roosters)

# verzendlijst Luistercijfers ophalen bij GD
path_stats_vzl <- paste0(config$gs_downloads_ubu, "/", "cz_stats_verzendlijst.xlsx")
drive_download(file = cz_get_url("stats_verzendlijst"), overwrite = T, path = path_stats_vzl)

# sheets als df -----------------------------------------------------------
tbl_raw_zenderschema <- cz_extract_sheet(path_roosters, sheet_name = paste0("modelrooster-", config$modelrooster_versie))
tbl_raw_stats_vzl_lst <- cz_extract_sheet(path_stats_vzl, sheet_name = "verzendlijst")
tbl_raw_stats_vzl_tpt <- cz_extract_sheet(path_stats_vzl, sheet_name = "themakanalen-per-titel")
tbl_raw_stats_vzl_mad <- cz_extract_sheet(path_stats_vzl, sheet_name = "mailadressen")

# refactor raw tables -----------------------------------------------------
source("src/refactor_raw_tables_stats.R")
