library(magrittr)
library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)

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
  ## TEST
  # cur_dir <- "/srv/audio/cz_rod"
  ## TEST
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
         & str_detect(key_tk.3mm, "00|59|58|30|29")
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
         & str_detect(key_tk.3mm, "00|59|58|30|29")
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
  

caroussel.1 <- themakanalen_listed.4 %>% left_join(cha_audio_full.3)
