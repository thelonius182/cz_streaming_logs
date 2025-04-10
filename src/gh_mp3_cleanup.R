pacman::p_load(R.utils, magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, 
               futile.logger, jsonlite, httr, ssh, RMySQL, purrr)

delete_remote_file <- function(file) {
  full_file_path <- path("/srv/audio/cz_rod", file)
  command <- sprintf("rm -f '%s'", full_file_path)
  ssh_exec_wait(gh_sess, command)
  message(paste("Deleted:", full_file_path))
}

gh_sess <- ssh_connect("cz@streams.greenhost.nl")
gh_list <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/") 
output_text <- rawToChar(gh_list$stdout) |> str_split_1("\n") |> as_tibble()

gh_mp3s_a <- output_text |> mutate(n_bytes = parse_integer(str_extract(value, "\\b[0-9]{5,}\\b")),
                                   mp3_name = str_extract(value, "20[0-9]{6}_[0-9]{4}\\.mp3$")) |> 
  filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-value)

bc_fmt <- stamp("20241227_2300", orders = "%Y%Om%d_%H%M", quiet = T)
gh_mp3s_b <- gh_mp3s_a |> mutate(bc_time = str_extract(mp3_name, "([0-9]{4})\\.mp3$", group = 1), 
                                 bc_date = str_extract(mp3_name, "([0-9]{8})_[0-9]{4}\\.mp3$", group = 1), 
                                 bc_ts_chr = paste0(bc_date, " ", bc_time),
                                 bc_ts = round_date(ymd_hm(bc_ts_chr), unit = "hour"),
                                 bc_ts_fmt = bc_fmt(bc_ts))

wp_replays <- read_lines("/mnt/muw/basie_beats/wp_replays_list_20240929.txt", lazy = F) |> as_tibble() |> 
  mutate(replay_ts = round_date(ymd_hms(value), unit = "15 minutes"),
         replay_ts_fmt = bc_fmt(replay_ts))

gh_mp3s_c <- gh_mp3s_b |> inner_join(wp_replays, by = join_by(bc_ts_fmt == replay_ts_fmt))
write_rds(gh_mp3s_c, "/mnt/muw/rod_bak/gh_mp3s_c.RDS")
gh_mp3s_c_tmp <- gh_mp3s_c |> head(3)
gh_mp3s_c_tmp |>  pull(mp3_name) |> walk(delete_remote_file)

ssh_disconnect(gh_sess)

n_gibi_bytes <- sum(gh_mp3s_c$n_bytes, na.rm = T) / 1024 / 1024 / 1024 / 1024
