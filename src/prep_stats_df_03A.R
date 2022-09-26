library(readr)
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

cz_stats_rod.10 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_rod.10.RDS")) # from prep_rod4.R
cz_stats_cha_08 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_cha_08.RDS")) # from prep8.R
cz_ipa_geo_full <- read_rds(file = "cz_ipa_geo_full.RDS") # from prep_stats_df_02.R (renewed)

cz_stats_joined_01 <- cz_stats_cha_08 %>% 
  bind_rows(cz_stats_rod.10) %>% 
  select(-cz_row_id, -cz_id, -cz_cha_id) 

cz_stats_joined_02 <- cz_stats_joined_01 %>% 
  left_join(cz_ipa_geo_full, by = c("cz_ipa" = "ip")) %>% 
  select(-region_code, -region_name, -zip_code, -city, -latitude, -longitude, -metro_code) 

cz_tzones <- cz_stats_joined_02 %>% 
  select(time_zone) %>% 
  distinct() %>% 
  filter(!is.na(time_zone)) %>% 
  mutate(rrn = row_number(),
         NL_time = ymd_hms("2021-08-01 12:00:00", tz = "Europe/Amsterdam")) 
  
cz_tzones_diffs.1 <- NULL

for (t1 in 1:nrow(cz_tzones)) {
  # t1 <- 1
  cz_tz_tmp <- cz_tzones %>% 
    filter(rrn == t1) %>% 
    mutate(cz_local_ts = force_tz(NL_time, tzone = time_zone),
           cz_adj = as.period(NL_time - cz_local_ts, unit = "hour"),
           NL_time_adjusted = NL_time + cz_adj)
  
  if (is.null(cz_tzones_diffs.1)) {
    cz_tzones_diffs.1 <- cz_tz_tmp
  } else {
    cz_tzones_diffs.1 %<>% add_row(cz_tz_tmp)
  }
  
}

cz_tzones_diffs.2 <- cz_tzones_diffs.1 %>% 
  select(time_zone, cz_adj) 
  
cz_stats_joined_03 <- cz_stats_joined_02 %>% 
  left_join(cz_tzones_diffs.2) %>% 
  mutate(cz_ts_local = if_else(is.na(cz_adj), cz_ts, cz_ts + cz_adj)) %>% 
  select(-cz_ts, -time_zone, -cz_adj) %>% 
  filter(cz_ts_local >= cz_reporting_start & cz_ts_local <= cz_reporting_stop)

# program titles ----
# LET OP - doe dit alleen als er VEEL titels gewijzigd of bijgekomen zijn.
#          Alleen dan de onderstaande code ontsterren en uitvoeren
#          en de lijst opnieuw corrigeren. 
#          Is er maar een handvol wijzigingen, dan pgm_title_cleaner.tsv direct wijzigen.

# salsa_stats_all_pgms_raw <-
#   read_delim(
#     "~/Downloads/salsa_stats_all_pgms.txt",
#     delim = "\t",
#     escape_double = FALSE,
#     col_types = cols(pgmLang = col_skip()),
#     trim_ws = TRUE
#   ) %>% filter(pgmTitle != "NULL") %>% 
# 
# pgm_titles_unique <- salsa_stats_all_pgms_raw %>%
#   select(pgmTitle) %>%
#   distinct() %>%
#   mutate(pgmTtle_clean = str_to_sentence(str_replace_all(pgmTitle, "[[:punct:]]", ""))) %>%
#   arrange(pgmTitle)
# 
# write_delim(pgm_titles_unique, file = "~/Downloads/pgm_title_cleaner_20220402.tsv", delim = "\t")

# In ieder geval wel deze doen!
cz_pgm_titles_fixed.1 <- read_delim("~/Downloads/pgm_title_cleaner.tsv",
  delim = "\t",
  escape_double = FALSE,
  col_names = TRUE,
  trim_ws = TRUE
)

names(cz_pgm_titles_fixed.1) <- c("pgm_title", "pgm_title_fixed")

cz_stats_joined_04 <- cz_stats_joined_03 %>% 
  left_join(cz_pgm_titles_fixed.1, by = c("pgm_title" = "pgm_title"))

cz_stats_joined_04_missing <- cz_stats_joined_04 %>% filter(is.na(pgm_title_fixed))

write_rds(x = cz_stats_joined_04,
          file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS"),
          compress = "gz")
