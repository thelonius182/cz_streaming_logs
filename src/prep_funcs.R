proc_gh_logs <- function(pe_log_type) {
  
  gh_log_dir <- ifelse(pe_log_type == "RoD", "apache2", "icecast2")
  
  gh_log_run_ymd = now(tzone = "Europe/Amsterdam")
  
  # load log history ----
  gh_rds_filename <- paste0("/home/lon/Documents/cz_streaming_logs/gh_logs_history/gh_logs_his", 
                            ifelse(pe_log_type == "RoD", "_R.RDS", "_S.RDS")
  )
  
  gh_logs_his <- NULL
  
  if (file_exists(gh_rds_filename)) {
    gh_logs_his <- read_rds(file = gh_rds_filename)
  }
  
  # + connect to greenhost ----
  gh_sess <- ssh_connect("cz@streams.greenhost.nl")
  
  gh_cmd <- paste0("ls /var/log/", gh_log_dir, " -lt --time-style=+'%Y-%m-%d %H:%M:%S' | grep access.log | grep gz")
  
  # download current list ----
  gh_logs <- ssh_exec_internal(session = gh_sess, gh_cmd) %>%
    .[["stdout"]] %>%
    rawToChar() %>%
    strsplit("\n") %>%
    unlist() %>%
    as_tibble()
  
  # prep the list ----
  gh_logs_tib.a <- gh_logs %>%
    mutate(l_row_clean = str_squish(value)) %>%
    select(-value) %>%
    separate(l_row_clean, into = paste0("itm_", 1:8), sep = " ")
  
  gh_logs_tib.c <- gh_logs_tib.a %>%
    mutate(lg_run_ymd = gh_log_run_ymd) %>%
    select(
      lg_ymd = itm_6,
      lg_hms = itm_7,
      lg_size = itm_5,
      lg_name = itm_8,
      lg_run_ymd
    ) %>%
    arrange(lg_ymd, lg_hms, lg_run_ymd)
  
  # find new logs ----
  if (is.null(gh_logs_his)) {
    
    gh_logs_new <- gh_logs_tib.c
    
  } else {
    
    gh_logs_new <- gh_logs_tib.c %>%
      anti_join(
        y = gh_logs_his,
        by = c(
          "lg_ymd" = "lg_ymd",
          "lg_hms" = "lg_hms",
          "lg_size" = "lg_size"
        )
      )
  }
  
  # process new logs ----
  if (nrow(gh_logs_new) == 0) {
    # disconnect from greenhost ----
    ssh_disconnect(session = gh_sess)
    
  } else {
    # prep new dir
    gh_fs_dir_leaf <-
      gh_log_run_ymd %>% str_replace_all("[:-]", "") %>% str_replace_all("[ ]", "_")
    
    gh_fs_dir <-
      paste0("/home/lon/Documents/cz_streaming_logs/",
             ifelse(pe_log_type == "RoD", "R_", "S_"), 
             gh_fs_dir_leaf)
    
    dir_create(path = gh_fs_dir)
    
    # download all
    for (cur_log in gh_logs_new$lg_name) {
      scp_download(
        session = gh_sess,
        files = paste0("/var/log/",
                       ifelse(pe_log_type == "RoD", "apache2/", "icecast2/"), 
                       cur_log),
        to = gh_fs_dir
      )
    }
    
    # disconnect from greenhost ----
    ssh_disconnect(session = gh_sess)
    
    # unpack logs ----
    for (cur_log in gh_logs_new$lg_name) {
      gunzip(paste0(gh_fs_dir, "/", cur_log), remove = T)
    }
    
    # append new names to his
    if (is.null(gh_logs_his)) {
      gh_logs_his <- gh_logs_new
    } else {
      gh_logs_his %<>% bind_rows(gh_logs_new)
    }
    
    write_rds(x = gh_logs_his, file = gh_rds_filename, compress = "gz")
  }
}

stage_caroussel <- function() {
  
  fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")
  
  # get all channel playlists ----
  # source: query on Nipper, exported as .csv
  # C:\Users\nipper\Documents\cz_queries\themakanalen_2.sql
  themakanalen_listed_raw <- read_csv("~/Downloads/themakanalen_listed.csv")
  
  themakanalen_listed <- themakanalen_listed_raw %>% 
    filter(!is.na(url) & !is.na(pgm_start))
  
  # select unique gh-dirs ----
  gh_cha_dirs <- themakanalen_listed %>% 
    mutate(cha_dir = path_dir(url),
           cha_dir = if_else(str_starts(cha_dir, "/"), cha_dir, paste0("/", cha_dir))
    ) %>% 
    select(cha_dir) %>% 
    # filter(!str_starts(cha_dir, "/ht") & !str_detect(cha_dir, "portal")
    # ) %>% 
    mutate(cha_dir = paste0("/srv/audio", cha_dir),
           # files in cz/cz/rod are symlinks to cz_rod. Dereference it first.
           cha_dir = if_else(cha_dir == "/srv/audio/cz/cz/rod", "/srv/audio/cz_rod", cha_dir)) %>% 
    group_by(cha_dir) %>% 
    mutate(n = n()) %>% 
    distinct() %>% 
    # filter(!str_detect(cha_dir, "/srv/audio/radio6/cz/(nachten|%s)")
    # ) %>% 
    arrange(cha_dir)
  
  # write_delim(gh_cha_dirs, file = "gh_cha_dirs.tsv", delim = "\t")
  
  cha_audio_full <- NULL
  
  # collect mp3 details ----
  # + connect to greenhost ----
  gh_sess <- ssh_connect("cz@streams.greenhost.nl")
  
  for (cur_dir in gh_cha_dirs$cha_dir) {
    
    flog.info(paste0("listing ", cur_dir), name = "cz_stats_cha_log")
    
    ls_cmd <- paste("ls", cur_dir, "-lt --time-style=+'%Y-%m-%d %H:%M:%S'")
    
    cha_audio <- NULL
    
    try(expr = cha_audio <- ssh_exec_internal(gh_sess, ls_cmd) %>%
          .[["stdout"]] %>%
          rawToChar() %>%
          strsplit("\n") %>%
          unlist() %>%
          as_tibble(),
        silent = T
    )
    
    # prep the list ----
    if (is.null(cha_audio)) {
      
      flog.info(paste0("listing failed for", cur_dir), name = "cz_stats_cha_log")
      
    } else {
      
      cha_audio.a <- cha_audio %>%
        mutate(l_row_clean = str_squish(value)) %>%
        select(-value) %>% 
        separate(l_row_clean, into = paste0("itm_", 1:10), sep = " ") %>%
        mutate(gh_src_dir = cur_dir)
      
      # append new names to his
      if (is.null(cha_audio_full)) {
        
        cha_audio_full <- cha_audio.a
        
      } else {
        
        cha_audio_full %<>% bind_rows(cha_audio.a)
        
      }
    }
  }
  
  # disconnect from greenhost ----
  ssh_disconnect(session = gh_sess)
  
  # saveRDS(cha_audio_full, "cha_audio_full.RDS")
  
  cha_audio_full.1 <- cha_audio_full %>% 
    mutate(key_tk_listed = str_replace(paste0(gh_src_dir, "/", itm_8), "/srv/audio", ""),
           key_tk_listed = str_replace(key_tk_listed, "-", "_"),
           key_tk.1 = path_dir(key_tk_listed),
           key_tk.2 = str_replace(path_file(key_tk_listed), "\\.mp3", ""),
           key_tk.3y = str_sub(key_tk.2, 1, 4),
           key_tk.3m = str_sub(key_tk.2, 5, 6),
           key_tk.3d = str_sub(key_tk.2, 7, 8),
           key_tk.3hh = str_sub(key_tk.2, 10, 11),
           key_tk.3mm = str_sub(key_tk.2, 12, 13),
           itm_5 = as.integer(itm_5)) %>% 
    na_if("") %>% 
    mutate(key_tk.3mm = if_else(is.na(key_tk.3mm), "00", key_tk.3mm)) %>% 
    filter(itm_1 != "total" 
           & !str_starts(itm_1, "d")
           & str_detect(key_tk.3y, "\\d{4}")
           & str_detect(key_tk.3m, "\\d{2}")
           & str_detect(key_tk.3d, "\\d{2}")
           & str_detect(key_tk.3hh, "\\d{2}")
           & str_detect(key_tk.3mm, "00|01|02|03|59|58|57|32|31|30|29|28")
    )
  
  cha_audio_full.2 <- cha_audio_full.1 %>% 
    mutate(key_tk.2_ymd = ymd_hm(paste0(key_tk.3y, "-",
                                        key_tk.3m, "-", 
                                        key_tk.3d, " ", 
                                        key_tk.3hh, ":",
                                        key_tk.3mm)
    ),
    key_tk.2_ymd_rnd = round_date(key_tk.2_ymd, unit = "30 minutes")
    )
  
  cha_audio_full.3 <- cha_audio_full.2 %>% 
    select(key_tk_ymd = key_tk.2_ymd_rnd,
           key_tk_dir = key_tk.1,
           audio_file = itm_8,
           audio_size = itm_5)
  
  # append gh-dirs ----
  themakanalen_listed.1 <- themakanalen_listed %>% 
    mutate(cha_dir = path_dir(url),
           cha_dir = str_replace(cha_dir, "http:/streams.greenhost.nl", ""),
           cha_dir = str_replace(cha_dir, "https://cgi.omroep.nl/cgi-bin/streams?", ""),
           cha_dir = if_else(str_starts(cha_dir, "/"), cha_dir, paste0("/", cha_dir)),
           cha_dir = str_replace(cha_dir, "cz/cz/rod", "cz_rod")
    ) %>% 
    filter(!str_detect(url, "wma$")) 
  
  themakanalen_listed.2 <- themakanalen_listed.1 %>% 
    mutate(
      key_tk.2 = str_replace(path_file(url), "\\.mp3", ""),
      key_tk.3y = str_sub(key_tk.2, 1, 4),
      key_tk.3m = str_sub(key_tk.2, 5, 6),
      key_tk.3d = str_sub(key_tk.2, 7, 8),
      key_tk.3hh = str_sub(key_tk.2, 10, 11),
      key_tk.3mm = str_sub(key_tk.2, 12, 13),
      key_tk.3mm = if_else(is.na(key_tk.3mm) | str_length(key_tk.3mm) == 0, "00", key_tk.3mm)
    ) %>% 
    filter(str_detect(key_tk.3y, "\\d{4}")
           & str_detect(key_tk.3m, "\\d{2}")
           & str_detect(key_tk.3d, "\\d{2}")
           & str_detect(key_tk.3hh, "\\d{2}")
           & str_detect(key_tk.3mm, "00|01|02|03|59|58|57|32|31|30|29|28")
    )
  
  themakanalen_listed.3 <- themakanalen_listed.2 %>% 
    mutate(key_tk.2_ymd = ymd_hm(paste0(key_tk.3y, "-",
                                        key_tk.3m, "-",
                                        key_tk.3d, " ",
                                        key_tk.3hh, ":",
                                        key_tk.3mm)
    ),
    key_tk.2_ymd_rnd = round_date(key_tk.2_ymd, unit = "30 minutes")
    ) 
  
  themakanalen_listed.4 <- themakanalen_listed.3 %>% 
    rename(key_tk_ymd = key_tk.2_ymd_rnd) %>% 
    select(key_tk_ymd,
           key_tk_dir = cha_dir,
           everything(),
           -starts_with("key_tk.")
    )
  
  caroussel.1 <- themakanalen_listed.4 %>% 
    left_join(cha_audio_full.3) %>% 
    filter(!is.na(audio_size)) %>% 
    select(-cupro)
  
  # remove hijack errors
  caroussel.2 <- caroussel.1 %>% 
    group_by(key_tk_ymd, key_tk_dir, channel) %>%
    mutate(pgm_seq_nbr = row_number()) %>% 
    ungroup() %>% 
    mutate(pgm_nbr = row_number())
  
  double_track_ids <- caroussel.2 %>% filter(pgm_seq_nbr == 2) %>% select(pgm_id)
  
  double_tracks <- caroussel.2 %>% 
    filter(pgm_id %in% double_track_ids$pgm_id) %>% 
    filter(!str_detect(audio_file, "00\\.mp3$"))
  
  caroussel.3 <- caroussel.2 %>% 
    filter(!pgm_nbr %in% double_tracks$pgm_nbr) %>% 
    select(-pgm_seq_nbr, -key_tk_ymd, -key_tk_dir, -url, -audio_file, audio_bytes = audio_size) %>% 
    select(pgm_nbr, everything())
  
  caroussel.4 <- caroussel.3 %>% 
    mutate(pgm_secs = time_length(interval(pgm_start, pgm_stop), unit = "second"),
           audio_secs_128 = audio_bytes * 8 / 1024 / 128,
           audio_secs_160 = audio_bytes * 8 / 1024 / 160,
           audio_secs_192 = audio_bytes * 8 / 1024 / 192,
           audio_secs_256 = audio_bytes * 8 / 1024 / 256,
           audio_secs_128_diff = abs(pgm_secs - audio_secs_128),
           audio_secs_160_diff = abs(pgm_secs - audio_secs_160),
           audio_secs_192_diff = abs(pgm_secs - audio_secs_192),
           audio_secs_256_diff = abs(pgm_secs - audio_secs_256)
    ) %>% 
    rowwise() %>% 
    mutate(min_diff = min(c_across(ends_with("diff")))) %>% 
    # differences of more than 20 minutes indicate invalid data
    filter(min_diff < 1200) 
  
  caroussel.5 <- caroussel.4 %>% 
    select(pgm_nbr:pgm_id, pgm_secs_sched = pgm_secs) %>% 
    mutate(pgm_secs_calc = if_else(stop == 0, pgm_secs_sched - start, stop - start)) %>% 
    select(-start, -stop, -pgm_secs_sched, pgm_secs = pgm_secs_calc) %>% 
    # remove invalid entries
    filter(pgm_secs > 1000)
  
  cur_pgms_snapshot_filename <- "~/Downloads/themakanalen_current_pgms.csv"
  cur_pgms_snapshot <- read_csv(cur_pgms_snapshot_filename)
  
  tc_cur_pgms <- cur_pgms_snapshot %>% 
    select(channel, current_program) %>% 
    mutate(cp_snap_ts = file_info(cur_pgms_snapshot_filename)$modification_time)
  
  caroussel.6 <- caroussel.5 %>% 
    left_join(tc_cur_pgms, by = c("channel" = "channel", "pgm_id" = "current_program")) %>% 
    group_by(channel) %>% 
    mutate(cha_idx = row_number()) %>% 
    ungroup() %>% 
    select(pgm_idx = pgm_nbr, cha_id = channel, cha_idx, cha_name = name, 
           pgm_id, pgm_title, pgm_secs, cp_snap_ts)
  
  saveRDS(caroussel.6, "caroussel.RDS")
  
  return(caroussel.6)
}