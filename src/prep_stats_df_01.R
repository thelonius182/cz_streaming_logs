# 
# cz_stats_rod.10 <- read_rds(file = "cz_stats_rod.10.RDS") # from prep_rod4.R
# cz_stats_cha_08 <- read_rds(file = "cz_stats_cha_08.RDS") # from prep8.R
# cz_ipa_geo_full <- read_rds(file = "cz_ipa_geo_full.RDS") # from prep_stats_df_02.R
# 
# cz_stats_cha_08shft <- cz_stats_cha_08 %>% 
#   mutate(cz_ts = cz_ts + days(90))
# 
# cz_stats_rod.11 <- cz_stats_rod.10 %>% 
#   filter(cz_ts >= ymd_hms("2021-05-01 00:00:00", tz = "Europe/Amsterdam")
#          & cz_ts < ymd_hms("2021-06-01 00:00:00", tz = "Europe/Amsterdam"))
# 
# cz_stats_joined_01 <- cz_stats_cha_08shft %>% 
#   bind_rows(cz_stats_rod.11) %>% 
#   select(-cz_row_id, -cz_id, -cz_cha_id) 
