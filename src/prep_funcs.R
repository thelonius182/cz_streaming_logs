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
  
  fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_proc_log")
  
  themakanalen_listed <- themakanalen_listed_raw %>% 
    filter(!is.na(url) & !is.na(pgm_start))
  
  # select unique gh-dirs ----
  gh_cha_dirs <- themakanalen_listed %>% 
    mutate(cha_dir = path_dir(url),
           cha_dir = if_else(str_starts(cha_dir, "/"), cha_dir, paste0("/", cha_dir))
    ) %>% 
    select(cha_dir) %>% 
    mutate(cha_dir = paste0("/srv/audio", cha_dir),
           # files in cz/cz/rod are symlinks to cz_rod. Dereference it first.
           cha_dir = if_else(cha_dir == "/srv/audio/cz/cz/rod", "/srv/audio/cz_rod", cha_dir)) %>% 
    group_by(cha_dir) %>% 
    mutate(n = n()) %>% 
    distinct() %>% 
    arrange(cha_dir)
  
  cha_audio_full <- NULL
  
  # collect mp3 details ----
  # + connect to greenhost ----
  gh_sess <- ssh_connect("cz@streams.greenhost.nl")
  
  for (cur_dir in gh_cha_dirs$cha_dir) {
    
    flog.info(paste0("listing ", cur_dir), name = "cz_stats_proc_log")
    
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
      
      flog.info(paste0("listing failed for", cur_dir), name = "cz_stats_proc_log")
      
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
  
  # clean up audio info ----
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
    mutate_all(~ifelse(. == "", NA, .)) %>%
    mutate(key_tk.3mm = if_else(is.na(key_tk.3mm), "00", key_tk.3mm)) %>% 
    filter(itm_1 != "total" 
           & !str_starts(itm_1, "d")
           & str_detect(key_tk.3y, "\\d{4}")
           & str_detect(key_tk.3m, "\\d{2}")
           & str_detect(key_tk.3d, "\\d{2}")
           & str_detect(key_tk.3hh, "\\d{2}")
           & str_detect(key_tk.3mm, "00|01|02|03|59|58|57|32|31|30|29|28")
    )
  
  # more cleaning...
  cha_audio_full.2 <- cha_audio_full.1 %>% 
    mutate(key_tk.2_ymd = ymd_hm(paste0(key_tk.3y, "-",
                                        key_tk.3m, "-", 
                                        key_tk.3d, " ", 
                                        key_tk.3hh, ":",
                                        key_tk.3mm),
                                 tz = "Europe/Amsterdam"),
           key_tk.2_ymd_rnd = round_date(key_tk.2_ymd, unit = "30 minutes")
    )
  
  # final cleaning....
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
  
  # clean up themakanaal info ----
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
  
  # more cleaning ...
  themakanalen_listed.3 <- themakanalen_listed.2 %>% 
    mutate(key_tk.2_ymd = ymd_hm(paste0(key_tk.3y, "-",
                                        key_tk.3m, "-",
                                        key_tk.3d, " ",
                                        key_tk.3hh, ":",
                                        key_tk.3mm),
                                 tz = "Europe/Amsterdam"),
    key_tk.2_ymd_rnd = round_date(key_tk.2_ymd, unit = "30 minutes")
    ) 
  
  themakanalen_listed.4 <- themakanalen_listed.3 %>% 
    rename(key_tk_ymd = key_tk.2_ymd_rnd) %>% 
    select(key_tk_ymd,
           key_tk_dir = cha_dir,
           everything(),
           -starts_with("key_tk.")
    )
  
  # build theme channels caroussel by joining theme channel-info and audio-info
  caroussel.1 <- themakanalen_listed.4 %>% 
    left_join(cha_audio_full.3) %>% 
    filter(!is.na(audio_size)) %>% 
    select(-cupro)
  
  # remove hijack cutting errors
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
  
  # infer audio length from bitrate
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

analyze_log <- function(logfile) {
  # logfile <- "/home/lon/Documents/cz_streaming_logs/access.log.7"
  flog.info(paste0("log file: ", logfile), name = "cz_stats_proc_log")
  
  # inlezen ----  
  suppressMessages(
    access_log <- read_delim(
      logfile,
      "\t",
      escape_double = FALSE,
      col_names = FALSE,
      trim_ws = TRUE,
      quote = ""
    )
  )
  
  # Clean it up
  # + part of TD-3.1 ----
  Encoding(access_log$X1) <- "UTF-8"
  access_log$X1 <- iconv(access_log$X1, "UTF-8", "UTF-8", sub = '') 
  
  # Split it up
  suppressWarnings(
    cz_log.1 <- access_log %>%
      mutate(
        lg_data_src = path_file(logfile),
        details = gsub('("[^"\r\n]*")? (?![^" \r\n]*"$)', "\\1¶", X1, perl = TRUE)
      ) %>%
      separate(
        col = details,
        into = paste0("fld", 1:11),
        sep = "¶"
      )
  )
  
  cz_log.2 <- cz_log.1 %>% 
    mutate() %>% 
    mutate(lg_ip = fld1, 
           cz_ts_raw = str_replace_all(fld4, "[\\[\\] +-]|0200|0100", ""), 
           lg_cz_ts = dmy_hms(cz_ts_raw, tz = "Europe/Amsterdam"),
           lg_http_req = str_replace_all(fld6, "[\"]", ""), 
           lg_http_resp_sts = fld7,
           lg_n_bytes = as.numeric(fld8), 
           lg_referrer = str_replace_all(fld9, "[\"]", ""), 
           lg_referrer = str_replace_all(lg_referrer, "-", NA_character_),
           lg_usr_agt = str_replace_all(fld10, "[\"]", ""),
           lg_usr_agt = str_replace_all(lg_usr_agt, "-", NA_character_),
           lg_usr_agt = str_to_lower(lg_usr_agt),
           lg_session_length = fld11
    ) %>% 
    select(
      lg_data_src,
      lg_ip,
      lg_usr_agt,
      lg_cz_ts,
      lg_http_req,
      lg_http_resp_sts,
      lg_referrer,
      lg_n_bytes,
      lg_session_length
    ) 
  
  rm(cz_log.1, access_log)
  
  return(cz_log.2)
}

analyze_rod_log <- function(logfile) {
  # logfile <- "/home/lon/Documents/cz_streaming_logs/R_20210518_204228/access.log.9"
  
  flog.info(paste0("log file: ", logfile), name = "cz_stats_proc_log")
  
  # inlezen ----  
  suppressMessages(
    rod_log <- read_delim(
      logfile,
      "\t",
      escape_double = FALSE,
      col_names = FALSE,
      trim_ws = TRUE, 
      quote = ""
    )
  )
  
  # + part of TD-3.1 ----
  Encoding(rod_log$X1) <- "UTF-8"
  rod_log$X1 <- iconv(rod_log$X1, "UTF-8", "UTF-8", sub = '')
  
  # clean it up & split ----
  tomcat_log_pattern <- gregexpr(pattern = '^(?<client>\\S+) (?<auth>\\S+ \\S+) \\[(?<datetime>[^]]+)\\] "(?:GET|POST|HEAD) (?<file>[^ ?"]+)\\??(?<parameters>[^ ?"]+)? HTTP/[0-9.]+" (?<status>[0-9]+) (?<size>[-0-9]+) "(?<referrer>[^"]*)" "(?<useragent>[^"]*)"$', 
                                 text = rod_log$X1, 
                                 perl = TRUE)
  rod_log_items <- lapply(tomcat_log_pattern, function(m) attr(m, "capture.start")[,1:9])
  
  for (i1 in seq_along(rod_log_items)) {
    attr(rod_log_items[[i1]], "match.length") <- attr(tomcat_log_pattern[[i1]], "capture.length")[,1:9]
  }
  
  rod_log_item_list <- regmatches(rod_log$X1, rod_log_items) 
  rod_log_item_list_names <- vector("list", length(rod_log_item_list))
  rod_log_item_list_values <- vector("list", length(rod_log_item_list))
  
  for (i1 in seq_along(rod_log_item_list)) {
    
    for (j1 in seq_along(rod_log_item_list[[i1]])) {
      rod_log_item_list_names[[i1]][j1] <- rod_log_item_list[[i1]][j1] %>% names()
      rod_log_item_list_values[[i1]][j1] <- rod_log_item_list[[i1]][j1] %>% paste(collapse = "")
    }
    
  }
  
  log_item_names <- rod_log_item_list_names %>% unlist() %>% as_tibble()
  names(log_item_names) <- "log_item_name"
  log_item_values <- rod_log_item_list_values %>% unlist() %>% as_tibble()
  names(log_item_values) <- "log_item_value"
  log_item_nvp <- bind_cols(log_item_names, log_item_values) %>%
    mutate(nvp_idx = if_else(log_item_name == "client", row_number(), NA_integer_)) %>% 
    fill(nvp_idx, .direction = "down")
  
  cz_log.1 <- log_item_nvp %>%
    pivot_wider(names_from = log_item_name,
                values_from = log_item_value) %>%
    filter(str_detect(file, "[.]mp3")) %>%
    mutate(lg_ts = dmy_hms(datetime, tz = "Europe/Amsterdam", quiet = T)) %>%
    select(
      lg_ipa = client,
      lg_ts,
      lg_audio_file = file,
      lg_sts = status,
      lg_size = size,
      lg_ref = referrer,
      lg_usr_agt = useragent,-nvp_idx,-auth,-parameters,-datetime
    ) %>%
    filter(lg_sts %in% c("200", "206")) %>%
    mutate(
      lg_size = as.numeric(lg_size),
      lg_ref = gsub("(?:.*//)([^/]*)/.*", "\\1", lg_ref, perl = TRUE),
      lg_usr_agt = str_to_lower(lg_usr_agt)
    ) %>%
    select(starts_with("lg_"))
  
  rm(log_item_names, log_item_nvp, log_item_values, rod_log, rod_log_item_list,
     rod_log_item_list_names, rod_log_item_list_values, rod_log_items, tomcat_log_pattern)
  
  return(cz_log.1)
}

get_period_logged = function(filepath) {
  # TEST_TEST_TEST_TEST    
  # filepath = "/home/lon/Documents/cz_streaming_logs/S_20210514_115638/access.log.1"
  # TEST_TEST_TEST_TEST    
  
  con = file(filepath, "r")
  log_line <- readLines(con, n = 1)
  close(con)
  
  # ts_max = ymd_hms("1900-01-01 00:00:00")
  # ts_min = ymd_hms("2100-01-01 00:00:00")
  
  # for (cur_line in log_lines) {
  #   
  #   ts_cur_chr <- str_extract(string = cur_line, pattern = "\\[(.*?)\\]")
  #   ts_cur <- suppressMessages(dmy_hms(ts_cur_chr, tz = "Europe/Amsterdam"))
  #   
  #   if (ts_cur > ts_max) {
  #     ts_max = ts_cur
  #   }
  #   
  #   if (ts_cur < ts_min) {
  #     ts_min = ts_cur
  #   }
  #   
  # }
  
  ts_log_chr <-
    str_extract(string = log_line, pattern = "\\[(.*?)\\]")
  ts_log <-
    suppressMessages(dmy_hms(ts_log_chr, tz = "Europe/Amsterdam"))
  
  return(
    tibble(
      cz_log_dir = path_dir(filepath),
      cz_log_file = path_file(filepath),
      cz_ts_log = ts_log
    )
  )
}

stats_data_flr <- function(scope = "maand") {
  stats_flr_mnd <- cz_stats_cfg$`current-month` %>% str_sub(1, 7)
  stats_flr_jaar <- cz_stats_cfg$`current-month` %>% str_sub(1, 4)
  stats_flr <- NULL
  
  if (scope == "maand") {
    stats_flr <- stats_flr_mnd
  } else {
    stats_flr <- stats_flr_jaar
  }
  
  return(paste0(cz_stats_cfg$stats_data_home, stats_flr, "/"))
}

f2si2 <- function (number, rounding = F)
{
  lut <- c(
    1e-24,
    1e-21,
    1e-18,
    1e-15,
    1e-12,
    1e-09,
    1e-06,
    0.001,
    1,
    1000,
    1e+06,
    1e+09,
    1e+12,
    1e+15,
    1e+18,
    1e+21,
    1e+24
  )
  pre <- c("y",
           "z",
           "a",
           "f",
           "p",
           "n",
           "u",
           "m",
           "",
           "k",
           "M",
           "G",
           "T",
           "P",
           "E",
           "Z",
           "Y")
  ix <- findInterval(number, lut)
  if (lut[ix] != 1) {
    if (rounding == T) {
      sistring <- paste(round(number / lut[ix]), pre[ix])
    }
    else {
      sistring <- paste(number / lut[ix], pre[ix])
    }
  }
  else {
    sistring <- as.character(number)
  }
  return(sistring)
}

# get reamining GEO-queries at MaxMind
queries_remaining <- function(arg_creds) {
  
  # use an arbitrary, exisiting IP-address. The response includes the number of remaining queries
  ip_request <- "https://geoip.maxmind.com/geoip/v2.1/city/1.164.216.28?pretty"
  request_sts <- try(mm_response <- GET(url = ip_request,
                                        authenticate(arg_creds$account_id, 
                                                     arg_creds$license_key)))
  # service-error
  if (attr(request_sts, "class") == "try-error" | mm_response$status_code != 200) {
    return(NULL)
  } 
  
  # retrieve the response content
  response_content <- content(mm_response, as = "parsed", type = "application/json")
  
  # parse the response content
  parsing_sts <- try(parsed_response <-
                       response_content %>% as.data.frame() %>% as_tibble() %>%
                       pivot_longer(
                         cols = contains("names.en") |
                           (contains("code") &
                              !contains("metro")),
                         names_to = "key",
                         values_to = "value"
                       ) %>%
                       select(
                         lat = location.latitude,
                         lng = location.longitude,
                         stats_tz = location.time_zone,
                         queries_remaining,
                         key,
                         value
                       ))
  
  # service-error
  if ("try-error" %in% attr(parsing_sts, "class")) {
    retrun(NULL)
  } 
  
  # return queries remaining
  n_req_stock_tib <- parsed_response %>% select(n = queries_remaining) 
  return(n_req_stock_tib$n[[1]])
}