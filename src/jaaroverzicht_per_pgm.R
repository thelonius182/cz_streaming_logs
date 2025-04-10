pacman::p_load(tidyr, dplyr, stringr, readr, lubridate, fs, rio)
reports <- dir_ls("/home/lon/Documents/cz_stats_data/", recurse = T, type = "file", regexp = "cz_stats_pgms") |> 
  as_tibble() |> filter(!str_detect(value, "archief"))
tib_reports <- import_list(reports$value, setclass = "tibble", rbind = T) 

tib_reports.1 <- tib_reports |> mutate(cz_period = str_extract(`_file`, "/(\\d{4}-\\d{2})/", group = 1)) |> 
  select(cz_period, programma = pgm_title, uren_totaal = hours_tot, uren_live = hours_LIVE, uren_on_demand = hours_ROD, 
         uren_themakanalen = hours_TH_CHA, sessies_totaal = sess_tot, unieke_luisteraars = n_unique_dev)

write_tsv(tib_reports.1, "/home/lon/Documents/cz_stats_data/luistercijfers 2023, per programma.tsv")

jazz_pgms <- tib_reports.1 |> 
  mutate(is_jazz = str_detect(programma, 
                              regex("songbook|impro|dikes|jazz|co live|concertzender live|dansen en de blues|duke|dwarsliggers|groove|grote geluid|hipsters|house|injazz|krizz krazz|little|moanin|musicians corner|paleis|spinnin|three",
                                    ignore_case = T)),
         programma = str_squish(str_to_lower(programma))) |> filter(is_jazz & programma != "concertzender live") |> 
  select(-is_jazz) |> arrange(programma, cz_period) |> group_by(cz_period, programma) |> 
  mutate(across(matches("(uren|sess|uniek).*"), sum)) |> distinct()

write_tsv(jazz_pgms, "/home/lon/Documents/cz_stats_data/luistercijfers jazz, 2024-03-11.tsv")
