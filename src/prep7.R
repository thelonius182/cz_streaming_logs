
# assign an id to each caroussel track----
# caroussel 7 is end of prep6, car.1 to 6 are in prep_funcs.R
caroussel.7a <- caroussel.7 %>% 
  ungroup() %>% 
  mutate(caroussel_id = row_number(),
         # if a pgm stops TOTH, make it stop 1 second earlier to facilitate splitting by hour 
         track_stop = if_else(hour(track_stop) == 0 
                              & minute(track_stop) == 0
                              & second(track_stop) == 0,
                              track_stop - seconds(1L),
                              track_stop)) %>% 
  select(caroussel_id, everything())

rm(caroussel.7)

# split caroussel by hours ----
ct_tracks_by_hour <- tibble(
  sbh.id = 0L,
  sbh.ts = ymd_hms("1970-01-01 00:00:00", tz = "Europe/Amsterdam"),
  sbh.length = 0L
)

for (cid in caroussel.7a$caroussel_id) {
  cur_track <- caroussel.7a %>% filter(caroussel_id == cid)
  fd_start <- floor_date(cur_track$track_start, unit = "hour")
  fd_stop <- floor_date(cur_track$track_stop, unit = "hour")
  
  if (fd_start == fd_stop) {
    cur_sbh <- tibble(
      sbh.id = cur_track$caroussel_id,
      sbh.ts = fd_start,
      sbh.length = cur_track$pgm_secs
    )
    
    ct_tracks_by_hour %<>% add_row(cur_sbh)
    
  } else {
    breaks = seq(fd_start, fd_stop, by = "1 hour")
    track_hours <- fd_start + hours(0: length(breaks)) 
    track_itvls <- int_diff(track_hours)
    last_iter <- length(breaks)
    
    for (i1 in 1:last_iter) {
      
      if (i1 == 1) {
        
        cur_sbh <- tibble(
          sbh.id = cur_track$caroussel_id,
          sbh.ts = fd_start,
          sbh.length = int_length(interval(cur_track$track_start, int_end(track_itvls[[1]])))
        )
        
      } else if (i1 == last_iter) {
        
        cur_sbh <- tibble(
          sbh.id = cur_track$caroussel_id,
          sbh.ts = fd_stop,
          sbh.length = int_length(interval(int_start(track_itvls[[last_iter]]), cur_track$track_stop))
        )
        
      } else {
        
        cur_sbh <- tibble(
          sbh.id = cur_track$caroussel_id,
          sbh.ts = int_start(track_itvls[[i1]]),
          sbh.length = 3600L
        )
        
      }
      
      ct_tracks_by_hour %<>% add_row(cur_sbh)
    }
  }
}

caroussel.8 <- ct_tracks_by_hour %>%
  inner_join(caroussel.7a, by = c("sbh.id" = "caroussel_id"))

rm(ct_tracks_by_hour)

caroussel.9 <- caroussel.8 %>% 
  arrange(cha_id, sbh.ts, desc(sbh.length)) %>% 
  group_by(cha_id, sbh.ts) %>% 
  mutate(hour_idx = row_number()) %>% 
  filter(hour_idx == 1L) %>% 
  select(cha_id, sbh.ts, cha_name, pgm_title)

rm(caroussel.8)

# join stats info and caroussel ----
cz_stats_cha_07 <- cz_stats_cha_05 %>% 
  left_join(caroussel.9, by = c("cz_cha_id" = "cha_id", "cz_ts" = "sbh.ts"))

saveRDS(cz_stats_cha_07, "cz_stats_cha_07.RDS")

rm(cz_stats_cha_05)
