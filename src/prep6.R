# READ_ME ----
# Maak de lijst van TK-programma's, van ymd=cz_reporing_start tot en met ymd=cz_reporting_stop. Gaat obv df=caroussel, die per TK de
# programma's bevat. Van de TK-programma's die "nu" worden uitgezonden, is een snapshot te maken: df=cha_cur_pgms. Het tijdstip
# waarop dat gebeurt, zal na de einddatum van de rapportageperiode liggen (de rapportage gaat altijd over de vorige maand). De lijst
# hoeft dus alleen van achter naar voor te worden uitgebreid tot aan het begin, obv de lengtes van de TK-programma's. Als de
# caroussel van een TK zijn eerste programma bereikt, begint de TK-reeks opnieuw met het laatste programma.

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

# if (file_exists("caroussel.RDS")) {
#   caroussel <- readRDS(file = "caroussel.RDS")
# } else {
#   caroussel <- stage_caroussel()
# }

caroussel <- stage_caroussel()

cha_cur_pgms <- caroussel %>% 
  group_by(cha_id) %>% 
  mutate(cha_idx_max = max(cha_idx)) %>% 
  filter(!is.na(cp_snap_ts))

cp_snap_ts_new <- file_info("/home/lon/Downloads/themakanalen_current_pgms.txt") %>% select("change_time")
cha_cur_pgms %<>% mutate(cp_snap_ts = cp_snap_ts_new$change_time)

cur_cha_new <-  NULL

# infer intervals ----
for (a_cur_cha_id in cha_cur_pgms$cha_id) {
  
  # init ----
  # a_cur_cha_id <- 1L
  cur_cha <- cha_cur_pgms %>% filter(cha_id == a_cur_cha_id) %>% 
    mutate(track_start = cp_snap_ts, 
           track_stop = cp_snap_ts + dseconds(pgm_secs))
  
  if (is.null(cur_cha_new)){
    cur_cha_new <- cur_cha
  } else {
    cur_cha_new %<>% bind_rows(cur_cha)
  }
  
  # fill backwards ----
  # (first cur_pgm date is after end-of-report date)
  cur_start <- cur_cha$track_start
  cur_cha_idx <- cur_cha$cha_idx
  cur_cha_idx_max <- cur_cha$cha_idx_max
  
  while (cur_start > cz_reporting_start) {
    
    cur_cha_idx <- cur_cha_idx - 1L
    
    if (cur_cha_idx == 0) {
      cur_cha_idx <- cur_cha_idx_max
    }
    
    cur_cha <- caroussel %>% 
      filter(cha_id == a_cur_cha_id & cha_idx == cur_cha_idx) %>% 
      mutate(track_stop = cur_start, 
             track_start = track_stop - dseconds(pgm_secs))
    
    if (is.null(cur_cha_new)){
      cur_cha_new <- cur_cha
    } else {
      cur_cha_new %<>% bind_rows(cur_cha)
    }
    
    cur_start <- cur_cha$track_start
  }
}

# adjust stop time ----
# if a pgm stops TOTH, make it stop 1 second earlier to facilitate splitting by hour later on
caroussel.7 <- cur_cha_new %>% 
  mutate(stop_toth = if_else(minute(track_stop) == 0 & second(track_stop) == 0, T, F),
         track_stop = if_else(stop_toth, track_stop - dseconds(1L), track_stop),
         pgm_secs = if_else(stop_toth, pgm_secs - 1L, pgm_secs)
  ) %>% 
  arrange(cha_id, track_start)

saveRDS(caroussel.7, paste0(stats_data_flr(), "caroussel_7.RDS"))

rm(caroussel, cur_cha_new)
