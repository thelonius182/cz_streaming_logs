
# split live-stream----
live_by_hour <- tibble(
  lbh.id = 0L,
  lbh.ts = ymd_hms("1970-01-01 00:00:00", tz = "Europe/Amsterdam"),
  lbh.length = 0L
)

for (cid in salsa_stats_all_pgms.1$tbh.id) {
  # cid <- 2L
  cur_track <- salsa_stats_all_pgms.1 %>% filter(tbh.id == cid)
  fd_start <- floor_date(cur_track$tbh.start, unit = "hour")
  fd_stop <- floor_date(cur_track$tbh.stop, unit = "hour")
  
  if (fd_start == fd_stop) {
    cur_lbh <- tibble(
      lbh.id = cur_track$tbh.id,
      lbh.ts = fd_start,
      lbh.length = cur_track$tbh.secs
    )
    
    live_by_hour %<>% add_row(cur_lbh)
    
  } else {
    breaks = seq(fd_start, fd_stop, by = "1 hour")
    tbh.hours <- fd_start + dhours(0: length(breaks)) 
    tbh.itvls <- int_diff(tbh.hours)
    last_iter <- length(breaks)
    
    for (i1 in 1:last_iter) {
      
      if (i1 == 1) {
        
        cur_lbh <- tibble(
          lbh.id = cur_track$tbh.id,
          lbh.ts = fd_start,
          lbh.length = int_length(interval(cur_track$tbh.start, int_end(tbh.itvls[[1]])))
        )
        
      } else if (i1 == last_iter) {
        
        cur_lbh <- tibble(
          lbh.id = cur_track$tbh.id,
          lbh.ts = fd_stop,
          lbh.length = int_length(interval(int_start(tbh.itvls[[last_iter]]), cur_track$tbh.stop))
        )
        
      } else {
        
        cur_lbh <- tibble(
          lbh.id = cur_track$tbh.id,
          lbh.ts = int_start(tbh.itvls[[i1]]),
          lbh.length = 3600L
        )
        
      }
      
      live_by_hour %<>% add_row(cur_lbh)
    }
  }
}

lbh01 <- live_by_hour %>% 
  left_join(salsa_stats_all_pgms.1, by = c("lbh.id" = "tbh.id")) %>% 
  filter(!is.na(tbh.cha_id))

cz_stats_cha_07_live <- cz_stats_cha_07 %>% 
  filter(cz_cha_id == 0) %>% 
  left_join(lbh01, by = c("cz_ts" = "lbh.ts")) %>% 
  filter(!is.na(lbh.id)) %>% 
  mutate(cha_name = tbh.cha_name,
         pgm_title = tbh.title) %>% 
  select(-starts_with("lbh"), -starts_with("tbh"))

cz_stats_cha_07_tc <- cz_stats_cha_07 %>% 
  filter(cz_cha_id != 0)

cz_stats_cha_08a <- cz_stats_cha_07_live %>% 
  bind_rows(cz_stats_cha_07_tc)

# read channel info ----
channels <- read_delim(
  "~/Documents/chan_prep_final.csv",
  "\t",
  escape_double = FALSE,
  trim_ws = TRUE,
  quote = ""
)

cz_stats_cha_08 <- cz_stats_cha_08a %>% 
  inner_join(channels, by = c("cz_cha_id" = "id")) %>% 
  select(-item, -slug, -cha_name, cha_name = name) %>% 
  distinct()

saveRDS(cz_stats_cha_08, file = paste0(stats_data_flr(), "cz_stats_cha_08.RDS"))
