cz_stats_pgm_report.2.3 <-
  cz_stats_pgm_report.2.2 %>% mutate(pct_live = 100 * hours_LIVE / hours_tot,
                                     pct_live_median = median(pct_live),
                                     pct_rod = 100 * hours_ROD / hours_tot,
                                     pct_rod_median = median(pct_rod),
                                     pct_tch = 100 * hours_TH_CHA / hours_tot,
                                     pct_tch_median = median(pct_tch))

cz_stats_pgm_report.2.4 <- cz_stats_pgm_report.2.3 %>% select(pct_live_median, pct_rod_median, pct_tch_median) %>% distinct()

moro_title_compare.1 <- moro_titles_raw %>% left_join(moro_titles_wpgi_raw, keep = T)

moro_title_compare.2 <- moro_titles_wpgi_raw %>% left_join(moro_titles_raw, keep = T) %>% filter(is.na(moro_key.y))
