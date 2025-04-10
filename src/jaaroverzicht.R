pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, rio)
reports <- dir_ls("/home/lon/Documents/cz_stats_data/", recurse = T, type = "file", regexp = "cz_stats_channel_report") |> 
  as_tibble() |> filter(!str_detect(value, "archief"))
tib_reports <- import_list(reports$value, setclass = "tibble", rbind = T) 
tib_totals <- tib_reports |> filter(themakanalen == "TOTALEN") |> 
  select(jaar, maand, sessies, uren, sessies_per_uur, minuten_per_sessie, unieke_luisteraars)
write_tsv(tib_totals, "/home/lon/Documents/cz_stats_data/luistercijfers 2023, totalen.tsv")
