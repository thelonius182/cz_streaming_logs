cz_stats_day_report.1 <- cz_stats_joined_04 %>% 
  filter(!is.na(cha_name)) %>% 
  select(-pgm_title) %>% 
  rename(pgm_title = pgm_title_fixed) %>% 
  mutate(cz_day = date(cz_ts_local),
         pgm_source = case_when(cha_name == "Live" ~ "LIVE",
                                cha_name == "RoD" ~ "ROD",
                                T ~ "TCHA")) %>% 
  select(cz_day, pgm_title, cz_ipa, cz_length, pgm_source, country_name) %>% 
  arrange(cz_day, pgm_title)

cz_stats_day_report.sessions_live <- cz_stats_day_report.1 %>% 
  filter(pgm_source == "LIVE") %>% 
  group_by(cz_day, pgm_title) %>% 
  summarise(n_sessions = n()) %>% 
  ungroup() %>% 
  select(cz_day, n_sessions, pgm_title) %>%
  arrange(cz_day, desc(n_sessions)) %>% 
  group_by(cz_day) %>% 
  mutate(top10 = row_number()) %>% 
  ungroup() %>% 
  filter(top10 <= 10L & month(cz_day) == 5)

rmarkdown::render("~/Documents/yihui_02.Rmd", "html_document")
