suppressPackageStartupMessages(library(tidyr))
suppressPackageStartupMessages(library(dplyr))
suppressPackageStartupMessages(library(stringr))
suppressPackageStartupMessages(library(readr))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(fs))
suppressPackageStartupMessages(library(futile.logger))
suppressPackageStartupMessages(library(tibble))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(ssh))

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_rod.log"), "cz_stats_rod_log")

# select gh audio dirs ----
rod_audio_full <- NULL

gh_rod_dirs <-
  read_csv("~/Downloads/gh_rod_dirs.txt",
           col_names = FALSE)
names(gh_rod_dirs) <- "rod_dir"

# collect mp3 details ----
# + connect to greenhost ----
gh_sess <- ssh_connect("cz@streams.greenhost.nl")

for (cur_dir in gh_rod_dirs$rod_dir) {
  flog.info(paste0("listing ", cur_dir), name = "cz_stats_cha_log")
  
  ls_cmd <-
    paste("ls", cur_dir, "-lt --time-style=+'%Y-%m-%d %H:%M:%S'")
  
  rod_audio <- NULL
  
  try(expr = rod_audio <- ssh_exec_internal(gh_sess, ls_cmd) %>%
        .[["stdout"]] %>%
        rawToChar() %>%
        strsplit("\n") %>%
        unlist() %>%
        as_tibble(),
      silent = T)
  
  # prep the list ----
  if (is.null(rod_audio)) {
    flog.info(paste0("listing failed for", cur_dir), name = "cz_stats_cha_log")
    
  } else {
    rod_audio.a <- rod_audio %>%
      mutate(l_row_clean = str_squish(value)) %>%
      select(-value) %>%
      separate(l_row_clean,
               into = paste0("itm_", 1:10),
               sep = " ") %>%
      mutate(gh_src_dir = cur_dir)
    
    # append new names to his
    if (is.null(rod_audio_full)) {
      rod_audio_full <- rod_audio.a
      
    } else {
      rod_audio_full %<>% bind_rows(rod_audio.a)
      
    }
  }
}

# disconnect from greenhost ----
ssh_disconnect(session = gh_sess)

# clean up audio info ----
rod_audio_full.1 <- rod_audio_full %>%
  mutate(
    key_tk_listed = str_replace(paste0(gh_src_dir, "/", itm_8), "/srv/audio", ""),
    key_tk_listed = str_replace(key_tk_listed, "-", "_"),
    key_tk.1 = path_dir(key_tk_listed),
    key_tk.2 = str_replace(path_file(key_tk_listed), "\\.mp3", ""),
    key_tk.3y = str_sub(key_tk.2, 1, 4),
    key_tk.3m = str_sub(key_tk.2, 5, 6),
    key_tk.3d = str_sub(key_tk.2, 7, 8),
    key_tk.3hh = str_sub(key_tk.2, 10, 11),
    key_tk.3mm = str_sub(key_tk.2, 12, 13),
    itm_5 = as.integer(itm_5)
  ) %>%
  na_if("") %>%
  mutate(key_tk.3mm = if_else(is.na(key_tk.3mm), "00", key_tk.3mm)) %>%
  filter(
    itm_1 != "total"
    & !str_starts(itm_1, "d")
    & str_detect(key_tk.3y, "\\d{4}")
    & str_detect(key_tk.3m, "\\d{2}")
    & str_detect(key_tk.3d, "\\d{2}")
    & str_detect(key_tk.3hh, "\\d{2}")
    & str_detect(key_tk.3mm, "00|01|02|03|59|58|57|32|31|30|29|28")
    & str_detect(itm_8, "\\.mp3") # some files miss the .mp3-extension; skip those
  )

# more cleaning...
rod_audio_full.2 <- rod_audio_full.1 %>%
  mutate(
    key_tk.2_ymd = ymd_hm(
      paste0(
        key_tk.3y,
        "-",
        key_tk.3m,
        "-",
        key_tk.3d,
        " ",
        key_tk.3hh,
        ":",
        key_tk.3mm
      ),
      tz = "Europe/Amsterdam"
    ),
    key_tk.2_ymd_rnd = round_date(key_tk.2_ymd, unit = "30 minutes")
  )

# final cleaning....
rod_audio_full.3 <- rod_audio_full.2 %>%
  select(
    key_tk_ymd = key_tk.2_ymd_rnd,
    key_tk_dir = key_tk.1,
    audio_file = itm_8,
    audio_size = itm_5
  ) %>%
  filter(key_tk_dir != "/cz/cz/rod") %>% 
  group_by(key_tk_ymd, key_tk_dir) %>% 
  arrange(key_tk_ymd, key_tk_dir, desc(audio_size)) %>% 
  mutate(key_rowNum = row_number()) %>% 
  filter(key_rowNum == 1)

write_rds(x = rod_audio_full.3,
          file = paste0(stats_data_flr(), "cz_rod_audio_1.RDS"),
          compress = "gz")
