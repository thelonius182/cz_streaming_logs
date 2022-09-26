library(readr)
fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

if (!exists("cz_stats_joined_04")) {
  flog.info("loading DF cz_stats_joined_04 from RDS", name = "cz_stats_proc_log")
  cz_stats_joined_04 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS"))
} else {
  flog.info("reusing DF cz_stats_joined_04", name = "cz_stats_proc_log")
}
  
cz_stats_report.1 <- cz_stats_joined_04 %>% filter(!is.na(cha_name)) %>% 
  mutate(hour_of_day = hour(cz_ts_local),
         pct_pgm_coverage = case_when(cz_length >= 2700 ~ 100,
                                  cz_length >= 1800 ~ 75,
                                  cz_length >= 900 ~ 50,
                                  cz_length >= 450 ~ 25,
                                  T ~ 5),
         part_of_day = case_when(hour_of_day >= 18 ~ "E",
                                 hour_of_day >= 12 ~ "A",
                                 hour_of_day >= 6 ~ "M",
                                 T ~ "N")
         ) 

# total number of sessions this month
n_sess_month <- cz_stats_report.1 %>% nrow()

# total listening hours this month
lihrs_month <- round(sum(cz_stats_report.1$cz_length) / 3600, 0)

# total number of devices this month (= number of unique listeners)
n_devices_month <-  cz_stats_report.1 %>% 
  group_by(cz_ipa, cz_dev_type) %>% 
  summarise(n_sess = n()) %>% 
  ungroup() %>% nrow()

# total reporting hours this month
report_hrs = round(int_length(interval(start = min(cz_stats_report.1$cz_ts_local),
                                         end = max(cz_stats_report.1$cz_ts_local))
                               ) / 3600)

cz_referrers <- cz_stats_joined_04 %>% 
  filter(!is.na(cz_ref)) %>% 
  filter(!str_detect(cz_ref, "[-]|http|localhost|streams.greenhost.nl|www.concertzender.nl|concertzender.nl")) %>% 
  group_by(cz_ref) %>% 
  summarise(n_refs = n()) %>% 
  ungroup() %>% 
  mutate(n_refs_tot = sum(n_refs))

cz_referrers_pct <- round(100 * cz_referrers$n_refs_tot[1] / n_sess_month)

cz_referrers %<>% select(-n_refs_tot) %>% arrange(desc(n_refs))

# listening hours 0: listeners and sessions by channel
n_lihrs_00 <- cz_stats_report.1 %>% 
  group_by(cha_name) %>% 
  summarise(lihrs = round(sum(cz_length) / 3600),
            n_sess = n()) %>% 
  mutate(mean_sess_per_hr = ceiling(n_sess / report_hrs),
         mean_mins_per_sess = ceiling(60 * lihrs / n_sess)) %>% 
  ungroup() %>% 
  mutate(lihrs_tot = sum(lihrs), 
         n_sess_tot = sum(n_sess),
         lihrs_pct = round(100 * lihrs / lihrs_tot, 1)) %>% 
 select(cha_name, n_sess, lihrs, lihrs_pct, mean_sess_per_hr, mean_mins_per_sess)

# devices by channel
n_lihrs_00a <- cz_stats_report.1 %>% 
  group_by(cha_name, cz_ipa, cz_dev_type) %>% 
  summarise(n_sess = n()) %>% 
  ungroup() %>% 
  select(-n_sess) %>% 
  group_by(cha_name) %>% 
  summarise(n_devices = n())

# listening hours 1: pct's by channel by device
n_lihrs_01 <- cz_stats_report.1 %>% 
  group_by(cha_name, cz_dev_type) %>% 
  summarise(lihrs = round(sum(cz_length) / 3600),
            n_sess = n()) %>% 
  ungroup(cz_dev_type) %>% 
  mutate(lihrs_tot = sum(lihrs), 
         n_sess_tot = sum(n_sess),
         lihrs_pct = round(100 * lihrs / lihrs_tot, 1)) %>% 
  select(-c(lihrs:n_sess_tot)) %>% 
  pivot_wider(names_from = cz_dev_type, names_prefix = "pct_", values_from = lihrs_pct) %>% 
  select(cha_name, pct_lihrs_desktop = pct_desktop, pct_lihrs_mob_app = `pct_mobile-app`, pct_lihrs_mob_web = `pct_mobile-web`, pct_lihrs_webradio = `pct_smart-speaker`, pct_lihrs_other = pct_overig)

# listening hours 1: sessions by channel by device
n_lihrs_01a <- cz_stats_report.1 %>% 
  group_by(cha_name, cz_dev_type) %>% 
  summarise(n_sess = n()) %>% 
  ungroup(cz_dev_type) %>% 
  mutate(n_sess_tot = sum(n_sess)) %>% 
  select(-n_sess_tot) %>% 
  pivot_wider(names_from = cz_dev_type, names_prefix = "n_", values_from = n_sess) %>% 
  select(cha_name, desktop = n_desktop, mobile_app = `n_mobile-app`, mobile_web = `n_mobile-web`, webradio = `n_smart-speaker`, dev_overig = n_overig)

# listening hours 2: pct's originating from devices outside NL
n_lihrs_02 <- cz_stats_report.1 %>% 
  mutate(is_nl = if_else(country_code == 'NL', T, F)) %>%
  group_by(cha_name, is_nl) %>% 
  summarise(lihrs = round(sum(cz_length) / 3600),
            n_sess = n()) %>% 
  ungroup(is_nl) %>% 
  mutate(lihrs_tot = sum(lihrs), 
         n_sess_tot = sum(n_sess),
         lihrs_pct = round(100 * lihrs / lihrs_tot, 1)) %>% 
  filter(!is_nl) %>% 
  select(-c(is_nl:n_sess_tot), pct_lihrs_abroad = lihrs_pct)

# listening hours 3: pct's by channel by part of day
n_lihrs_03 <- cz_stats_report.1 %>% 
  group_by(cha_name, part_of_day) %>% 
  summarise(lihrs = round(sum(cz_length) / 3600),
            n_sess = n()) %>% 
  ungroup(part_of_day) %>% 
  mutate(lihrs_tot = sum(lihrs), 
         n_sess_tot = sum(n_sess),
         lihrs_pct = round(100 * lihrs / lihrs_tot, 1),
         n_sess_pct = round(100 * n_sess / n_sess_tot, 1)) %>% 
  select(cha_name, part_of_day, n_sess, n_sess_pct) %>% 
  pivot_wider(names_from = part_of_day, values_from = c(n_sess, n_sess_pct)) %>% 
  select(cha_name, n_sess_M, n_sess_A, n_sess_E, n_sess_N, n_sess_pct_M, n_sess_pct_A, n_sess_pct_E, n_sess_pct_N)

cz_year <- year(cz_reporting_day_one)
cz_month <- month(cz_reporting_day_one)

# report channel stats
cz_stats_report.2 <- n_lihrs_00 %>% 
  inner_join(n_lihrs_00a) %>% 
  inner_join(n_lihrs_01a) %>% 
  inner_join(n_lihrs_02) %>% 
  inner_join(n_lihrs_03) %>% 
  mutate(cz_stats_year = cz_year, cz_stats_month = cz_month) %>% 
  select(jaar = cz_stats_year,
         maand = cz_stats_month,
         themakanalen = cha_name,
         sessies = n_sess,
         uren = lihrs,
         uren_pct = lihrs_pct,
         sessies_per_uur = mean_sess_per_hr,
         minuten_per_sessie = mean_mins_per_sess,
         unieke_luisteraars = n_devices,
         desktop_ses = desktop,
         mobile_app_ses = mobile_app,
         mobile_web_ses = mobile_web,
         webradio_ses = webradio,
         overig_ses = dev_overig,
         uren_buitenland_pct = pct_lihrs_abroad,
         ochtend_ses = n_sess_M,          
         middag_ses = n_sess_A,
         avond_ses = n_sess_E,
         nacht_ses = n_sess_N,
         ochtend_pct = n_sess_pct_M,
         middag_pct = n_sess_pct_A,
         avond_pct = n_sess_pct_E,
         nacht_pct = n_sess_pct_N)

cz_stats_report_total <- cz_stats_report.2 %>% 
  mutate(cz_stats_year = cz_year, cz_stats_month = cz_month) %>% 
  group_by(cz_stats_year, cz_stats_month) %>% 
  summarise(sessies = sum(sessies),
            uren = sum(uren),
            uren_pct = sum(uren_pct),
            sessies_per_uur = sum(sessies_per_uur),
            minuten_per_sessie = round(mean(minuten_per_sessie), 1),
            desktop_ses = round(sum(desktop_ses), 1),
            mobile_app_ses = round(sum(mobile_app_ses), 1),
            mobile_web_ses = round(sum(mobile_web_ses), 1),
            webradio_ses = round(sum(webradio_ses), 1),
            overig_ses = round(sum(overig_ses), 1),
            uren_buitenland_pct = round(mean(uren_buitenland_pct), 1),
            ochtend_ses = sum(ochtend_ses),
            middag_ses = sum(middag_ses),
            avond_ses = sum(avond_ses),
            nacht_ses = sum(nacht_ses),
            ochtend_pct = round(mean(ochtend_pct), 1),
            middag_pct = round(mean(middag_pct), 1),
            avond_pct = round(mean(avond_pct), 1),
            nacht_pct = round(mean(nacht_pct), 1)) %>% 
  ungroup() %>% 
  mutate(unieke_luisteraars = n_devices_month, cha_name = "TOTALEN") %>% 
  select(jaar = cz_stats_year,
         maand = cz_stats_month,
         themakanalen = cha_name,
         sessies,
         uren,
         uren_pct,
         sessies_per_uur,
         minuten_per_sessie,
         unieke_luisteraars,
         everything())

cz_stats_report.3 <- cz_stats_report.2 %>% add_row(cz_stats_report_total) %>% arrange(desc(sessies))

write_rds(x = cz_stats_report.3,
          file = paste0(stats_data_flr(), "cz_stats_report.3.RDS"),
          compress = "gz")

# cz_stats_channel_report.tsv ----
write_excel_csv2(x = cz_stats_report.3, file = paste0(stats_data_flr(), "cz_stats_channel_report.tsv"), delim = "\t")

# listeners by the hour of day ----
cz_stats_report.4a <- cz_stats_report.1 %>% 
  group_by(hour_of_day) %>% 
  summarise(n_dev = n()) %>% 
  ungroup() %>% 
  mutate(cha_name = "TOTALEN")

cz_stats_report.4b <- cz_stats_report.1 %>% 
  select(cz_ipa, cz_dev_type, hour_of_day) %>% 
  distinct() %>% 
  group_by(hour_of_day) %>% 
  summarise(n_unique_dev = n())

cz_stats_report.4c <- cz_stats_report.4a %>% 
  inner_join(cz_stats_report.4b) %>% 
  mutate(pct_unique_dev = round(100 * n_unique_dev / n_dev)) %>% 
  select(cha_name, everything())

write_rds(x = cz_stats_report.4c,
          file = paste0(stats_data_flr(), "cz_stats_report.4c.RDS"),
          compress = "gz")

# listeners by channel by the hour of day ----
cz_stats_report.4d <- cz_stats_report.1 %>% 
  group_by(cha_name, hour_of_day) %>% 
  summarise(n_dev = n()) 

cz_stats_report.4e <- cz_stats_report.1 %>% 
  select(cz_ipa, cz_dev_type, cha_name, hour_of_day) %>% 
  distinct() %>% 
  group_by(cha_name, hour_of_day) %>% 
  summarise(n_unique_dev = n())

cz_stats_report.4f <- cz_stats_report.4d %>% 
  inner_join(cz_stats_report.4e) %>% 
  mutate(pct_unique_dev = round(100 * n_unique_dev / n_dev)) 

write_rds(x = cz_stats_report.4f,
          file = paste0(stats_data_flr(), "cz_stats_report.4f.RDS"),
          compress = "gz")

cz_stats_report.4g <- cz_stats_report.4f %>% bind_rows(cz_stats_report.4c)%>% 
  select(cha_name, hour_of_day, n_dev) %>% 
  pivot_wider(names_from = hour_of_day, names_prefix = "H", values_from = n_dev)

# cz_stats_hours_report.tsv ----

write_rds(x = cz_stats_report.4g,
          file = paste0(stats_data_flr(), "cz_stats_report.4g.RDS"),
          compress = "gz")


write_excel_csv2(x = cz_stats_report.4g, file = paste0(stats_data_flr(), "cz_stats_hours_report.tsv"), delim = "\t")

# by program ----
cz_stats_pgm_report.1 <- cz_stats_report.1 %>% 
  select(-cz_ref, -pgm_title, -country_name, -part_of_day) %>% 
  mutate(is_abroad = if_else(country_code == "NL", F, T),
         pgm_source = case_when(cha_name == "Live" ~ "LIVE",
                                cha_name == "RoD" ~ "ROD",
                                T ~ "TH_CHA")) %>% 
  select(-cha_name, -country_code, -hour_of_day, -cz_ts_local) %>% 
  rename(pgm_title = pgm_title_fixed)

# partial 1: hours by pgm_source ----
cz_stats_pgm_partial.1b <- cz_stats_pgm_report.1 %>% 
  group_by(pgm_title, pgm_source) %>% 
  summarise(hours = ceiling(sum(cz_length) / 3600),
            hours_tot = sum(hours)) %>% 
  ungroup() %>% 
  select(-hours) %>% 
  pivot_wider(names_from = pgm_source, names_prefix = "hours_", values_from = hours_tot) %>% 
  mutate(across(starts_with("hours"), ~ replace_na(., 0))) 

cz_stats_pgm_partial.1b_rs <- cz_stats_pgm_partial.1b %>% 
  select(-pgm_title) %>% rowSums()  %>% as_tibble() %>% rename(hours_tot = value)

cz_stats_pgm_partial.1b1 <- cz_stats_pgm_partial.1b %>% bind_cols(cz_stats_pgm_partial.1b_rs)

# partial 2: unique listeners ----
cz_stats_pgm_partial.3a1 <- cz_stats_pgm_report.1 %>% 
  group_by(pgm_title, cz_ipa, cz_dev_type) %>% 
  summarise(n1 = n()) %>% 
  ungroup(cz_ipa, cz_dev_type) %>% 
  summarise(n_unique_dev = n()) %>% 
  ungroup() 

cz_stats_pgm_partial.3a2 <- cz_stats_pgm_report.1 %>% 
  filter(is_abroad) %>% 
  group_by(pgm_title, cz_ipa, cz_dev_type) %>% 
  summarise(n1 = n()) %>% 
  ungroup(cz_ipa, cz_dev_type) %>% 
  summarise(n_unique_dev = n()) %>% 
  ungroup() 

cz_stats_pgm_partial.3c <- cz_stats_pgm_partial.3a1 %>% 
  left_join(cz_stats_pgm_partial.3a2, by = "pgm_title") %>% 
  rename(n_unique_dev = n_unique_dev.x, n_abroad = n_unique_dev.y) %>% 
  mutate(n_abroad = replace_na(n_abroad, 0),
         pct_abroad = ceiling(100 * n_abroad / n_unique_dev)) %>% 
  select(-n_abroad)

# partial 3: sessions by device ----
cz_stats_pgm_partial.1a <- cz_stats_pgm_report.1 %>% 
  mutate(cz_dev_type = str_replace_all(cz_dev_type, "-", "_"),
         cz_dev_type = str_replace_all(cz_dev_type, "smart_speaker", "webradio")) %>% 
  group_by(pgm_title, cz_ipa, cz_dev_type) %>% 
  summarise(n_sess = n()) %>% 
  ungroup() %>% 
  group_by(pgm_title, cz_dev_type) %>% 
  summarise(n_sess_tot = sum(n_sess)) %>% 
  ungroup() %>% 
  pivot_wider(names_from = cz_dev_type, names_prefix = "sess_", values_from = n_sess_tot) %>% 
  mutate(across(starts_with("sess"), ~ replace_na(., 0))) 

cz_stats_pgm_partial.1a_rs <- cz_stats_pgm_partial.1a %>% 
  select(-pgm_title) %>% rowSums()  %>% as_tibble() %>% rename(sess_tot = value)

cz_stats_pgm_partial.1a1 <- cz_stats_pgm_partial.1a %>% bind_cols(cz_stats_pgm_partial.1a_rs)

# partial 4: coverage ----
cz_stats_pgm_partial.4a <- cz_stats_pgm_report.1 %>% 
  group_by(pgm_title, pct_pgm_coverage) %>% 
  summarise(n_sess = n()) %>% 
  ungroup() %>% 
  pivot_wider(names_from = pct_pgm_coverage, names_prefix = "n_sess_P", values_from = n_sess) %>% 
  mutate(across(starts_with("n_sess_P"), ~ replace_na(., 0))) 

# pgms assembled ----
cz_stats_pgm_report.2 <- cz_stats_pgm_partial.1b1 %>% 
  inner_join(cz_stats_pgm_partial.3c) %>% 
  inner_join(cz_stats_pgm_partial.1a1) %>% 
  inner_join(cz_stats_pgm_partial.4a) 

# cz_stats_pgms_report.tsv ----

write_rds(x = cz_stats_pgm_report.2,
          file = paste0(stats_data_flr(), "cz_stats_pgm_report.2.RDS"),
          compress = "gz")

write_excel_csv2(x = cz_stats_pgm_report.2, file = paste0(stats_data_flr(), "cz_stats_pgms_report.tsv"), delim = "\t")
