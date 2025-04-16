pacman::p_load(magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, kableExtra,
               purrr, jsonlite, httr, yaml, ssh, googledrive, data.table, tinytex, rmarkdown)

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# get month from config file----
cz_stats_cfg <- read_yaml("config.yaml")
cz_reporting_day_one_chr <- cz_stats_cfg$current_month
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

# init logger ----
fa <- flog.appender(appender.file(cz_stats_cfg$log_appender_file), "statslog")
flog.info(paste0("selecting logs for ", cz_reporting_day_one_chr), name = "statslog")

# init stats directory
stats_flr <- paste0(cz_stats_cfg$stats_data_home, str_sub(cz_reporting_day_one_chr, 1, 7))

# prep STREAM+ROD logs ----
if (!dir_exists(stats_flr)) {
  dir_create(stats_flr)
  
  # . known periods logged ----
  cz_log_limits <- read_rds(file = "cz_log_limits.RDS") 
  
  # . CHANNEL logs ----
  cz_log_list <- cz_log_limits |> 
    filter(str_detect(cz_log_dir, "logs/S_") 
           & cz_ts_log >= cz_reporting_start
           & cz_ts_log <= cz_reporting_stop) |> 
    mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) |> 
    select(cz_log_list_path, cz_ts_log)
  
  # . prep logs ----
  cz_stats_cha <- tibble()
  
  for (cur_log_file in cz_log_list$cz_log_list_path) {
    cat("logfile:", cur_log_file, "\n")
    ana_single <- prep_cha_log(cur_log_file)
    cz_stats_cha <- bind_rows(cz_stats_cha, ana_single)
  }
  
  rm(ana_single)
  
  # . cleaning ----
  cz_stats_cha.1 <- cz_stats_cha |> 
    filter(month(lg_cz_ts) == month(cz_reporting_day_one) &
             lg_http_resp_sts == "200" &
             !is.na(lg_session_length) &
             !is.null(lg_session_length) &
             str_detect(lg_session_length, "^[0-9]+$") &
             str_detect(lg_http_req, "^GET") &
             str_detect(lg_http_req, "[.]m3u|[.]xspf", negate = T) &
             str_detect(lg_usr_agt, "radio|bot|crawl", negate = T)) |> 
    mutate(frag_seconds = as.integer(lg_session_length),
           bc_src = case_when(str_detect(lg_referrer, "worldofjazz") ~ "WJ", 
                              is.na(lg_referrer) ~ "CZ",
                              T ~ "CZ"),
           lg_http_req = str_remove_all(lg_http_req, "GET /| HTTP/1\\.[01]")) |>
    filter(frag_seconds >= 60) |>
    arrange(lg_ip, lg_http_req)
  
  cz_stats_cha.1a <- cz_stats_cha.1 |> group_by(lg_ip, lg_cz_ts) |> mutate(frag_seconds = max(frag_seconds)) |> 
    select(-lg_n_bytes, -lg_session_length) |> distinct() |> ungroup()
  
  # . store as RDS ----
  write_rds(cz_stats_cha.1a, str_glue("{stats_flr}/cz_stats_cha.1a.RDS"))
  
  rm(cz_stats_cha)
  cat("STREAM logfiles prepped\n")
  
  # . ROD logs ----
  cz_log_rod_list <- cz_log_limits |> 
    filter(str_detect(cz_log_dir, "logs/R_") 
           & cz_ts_log >= cz_reporting_start
           & cz_ts_log < cz_reporting_stop) |> 
    mutate(cz_log_list_path = paste(cz_log_dir, cz_log_file, sep = "/")) |> 
    select(cz_log_list_path, cz_ts_log)
  
  # . prep logs ----
  cz_stats_rod.1 = tibble()
  
  for (cur_file in cz_log_rod_list$cz_log_list_path) {
    cat("logfile:", cur_file, "\n")
    ana_single <- prep_rod_log(cur_file)
    cz_stats_rod.1 <- bind_rows(cz_stats_rod.1, ana_single)
  }
  
  rm(ana_single)
  
  # . cleaning ----
  cz_stats_rod.2 <- cz_stats_rod.1 |>
    filter(month(lg_ts) == month(cz_reporting_day_one)) |> 
    mutate(audio_path = path_dir(lg_audio_file),
           stream = case_when(audio_path == "/cz/cz/rod" ~ "on_demand_cz",
                              audio_path == "/cz_rod/woj" ~ "on_demand_woj",
                              audio_path == "/cz_rod/cp" ~ "concertpodium",
                              TRUE ~ "on_demand_cz"),
           d = 120L)  |> 
    select(ip_address = lg_ipa, ts = lg_ts, d, stream)
  
  # . store as RDS ----
  write_rds(cz_stats_rod.2, str_glue("{stats_flr}/cz_stats_rod.2.RDS"))
  cat("ROD logfiles prepped\n")
  rm(cz_stats_rod.1)
} else {
  cz_stats_cha.1a <- read_rds(str_glue("{stats_flr}/cz_stats_cha.1a.RDS"))
  cz_stats_rod.2 <- read_rds(str_glue("{stats_flr}/cz_stats_rod.2.RDS"))
}

flog.info(paste0("line count streams = ", nrow(cz_stats_cha.1a)), name = "statslog")
flog.info(paste0("line count rod's = ", nrow(cz_stats_rod.2)), name = "statslog")

# link stream names ----
cz_wj_streams <- read_tsv(cz_stats_cfg$cz_wj_streams, col_types = cols(.default = "c"))
stream_order <- c("cz_live_stream", "woj_live_stream", "themakanaal_bach", "themakanaal_barok", 
                  "themakanaal_gehoorde_stilte", "themakanaal_gregoriaans", "themakanaal_hardbop", 
                  "themakanaal_hedendaags", "themakanaal_jazz", "themakanaal_klassiek", 
                  "themakanaal_oude_muziek", "overige_themakanalen")

cz_stats_cha.2 <- cz_stats_cha.1a |> mutate(stream = paste0(str_to_lower(bc_src), "_", lg_http_req),
                                            te = lg_cz_ts + frag_seconds,
                                            id_cha = row_number()) |> 
  select(id_cha, ip_address = lg_ip, ts = lg_cz_ts, d = frag_seconds, te, stream) |> 
  left_join(cz_wj_streams, by = join_by(stream)) |> select(-stream) |> rename(stream = cz_redactie)

dt_cz_stats_cha <- as.data.table(cz_stats_cha.2) # |> filter(str_detect(stream, "live_stream$")))

# load hourly titles ----
# create hourly titles from nipper-pc query cz_wj_stats_hourly_titles.sql in C:\Users\nipper\Documents\cz_queries
hourly_titles_raw <- read_tsv(str_glue("{stats_flr}/hourly_titles.tsv"), 
                              col_names = c("pgm_start", "pgm_stop", "pgm_title", "post_type"),
                              col_types = cols(.default = "c"))

hourly_titles_fixed <- hourly_titles_raw |> 
  mutate(pgm_title = case_when(pgm_title == "Dansen en de blues" ~ "Dansen en de Blues",
                               pgm_title == "De wandeling" ~ "De Wandeling",
                               pgm_title == "Framework 1" ~ "Framework",
                               pgm_title == "Framework 2" ~ "Framework",
                               pgm_title == "Geen dag zonder Bach" ~ "Geen Dag zonder Bach",
                               pgm_title == "Vocal Jazz" ~ "Vocale Jazz",
                               TRUE ~ pgm_title),
         pgm_title = str_replace(pgm_title, pattern = "&amp;", replacement = "&"),
         station = case_when(post_type == "programma" ~ "cz_live_stream", 
                             post_type == "programma_woj" ~ "woj_live_stream",
                             TRUE ~ pgm_title),
         bc_from = ymd_h(pgm_start, tz = "Europe/Amsterdam"),
         bc_from60 = bc_from + 60, # should have listened to this program for at least 60 seconds
         bc_to = ymd_h(pgm_stop, tz = "Europe/Amsterdam"),
         bc_to60 = bc_to - 60, # should have listened to this program for at least 60 seconds
         id_tit = row_number()) |> 
  select(-post_type, -pgm_start, -pgm_stop)

dt_hourly_titles <- as.data.table(hourly_titles_fixed)

setkey(dt_hourly_titles, station, bc_from60, bc_to60)
result <- dt_cz_stats_cha[dt_hourly_titles, 
                          on = .(ts <= bc_to60, te >= bc_from60, stream == station), 
                          nomatch = 0, .(id_cha, id_tit)]

result_info <- result |> 
  inner_join(dt_cz_stats_cha, by = join_by(id_cha)) |> 
  inner_join(dt_hourly_titles, by = join_by(id_tit)) |> 
  select(ip_address:bc_from, bc_to) |> distinct() |> arrange(ip_address, ts) |> 
  group_by(ip_address, ts) |> mutate(sess_idx = row_number(), n_sess = n()) |> ungroup()

cz_stats_sessions_cha <- result_info |> 
  mutate(sess_from = if_else(sess_idx == 1, ts, bc_from),
         sess_to = if_else(n_sess == 1 | sess_idx == n_sess, te, bc_to),
         sess_len = as.numeric(sess_to - sess_from, units = "secs"),
         bc_hours = as.numeric(bc_to - bc_from, units = "hours")) |> 
  select(pgm_title, station, bc_from, bc_to, bc_hours, ip_address, sess_from, sess_to, sess_len) |> 
  arrange(pgm_title, station, bc_from, ip_address) |> distinct() |>  
  group_by(pgm_title, station, bc_from, bc_hours) |>
  summarise(unique_ips = n_distinct(ip_address), secs_sum = sum(sess_len)) |> ungroup() |> 
  mutate(tot_hours = round(secs_sum / 3600, 1),
         tot_hours_max = unique_ips * bc_hours,
         tot_hours = if_else(tot_hours > tot_hours_max, tot_hours_max, tot_hours)) |> 
  select(-secs_sum, -tot_hours_max) |> arrange(pgm_title, station, bc_from) |> rename(stream = station) |> 
  mutate(pgm_title = if_else(str_detect(pgm_title, "themakana"), "themakanaal", pgm_title))

# link rod-streams ----
rod_stream_order <- c("on_demand_cz", "concertpodium", "on_demand_woj")
cz_stats_rod.3 <- count_listeners(log_data = cz_stats_rod.2) |> filter(session_hour < cz_reporting_stop) 
cz_stats_sessions_rod <- cz_stats_rod.3 |> 
  mutate(stream = factor(stream, levels = rod_stream_order)) |> 
  arrange(session_hour, stream) |> 
  rename(bc_from = session_hour, unique_ips = unique_listener_count) |> 
  mutate(pgm_title = NA_character_, tot_hours = NA_real_, bc_hours = 1) |> 
  select(pgm_title, stream, bc_from, bc_hours, unique_ips, tot_hours)
  

# link stats and reorder ----
merged_stream_order = c("cz_live_stream",
                        "woj_live_stream",
                        "concertpodium",
                        "on_demand_cz",
                        "on_demand_woj",
                        "themakanaal_bach",
                        "themakanaal_barok",
                        "themakanaal_gehoorde_stilte",
                        "themakanaal_gregoriaans",
                        "themakanaal_hardbop",
                        "themakanaal_hedendaags",
                        "themakanaal_jazz",
                        "themakanaal_klassiek",
                        "themakanaal_oude_muziek",
                        "overige_themakanalen")
cz_stats_merged <- cz_stats_sessions_cha |> bind_rows(cz_stats_sessions_rod) |> 
  mutate(stream = factor(stream, levels = merged_stream_order)) |>
  arrange(bc_from, stream, pgm_title) |>
  select(bc_from, stream, pgm_title, everything()) |> 
  mutate(pgm_title = if_else(is.na(pgm_title), "on_demand", pgm_title))

# create pdf's ----
# . live-streams ----
cz_stats_merged_a <- cz_stats_merged |> filter(!pgm_title %in% c("on_demand", "themakanaal")) |> head()
sf <- lubridate::stamp(" - luistercijfers oktober 2021", locale = "nl_NL.utf8", orders = "my", quiet = TRUE)
report_title_sfx <- sf(cz_reporting_day_one)

for (cur_title in unique(cz_stats_merged_a$pgm_title)) {
  cat("Rendering:", cur_title, "\n")
  
  cz_summary <- cz_stats_merged |>
    filter(pgm_title == cur_title) |>
    mutate(
      date = as_date(bc_from),
      time = format(bc_from, "%H:%M"),
      weekday = lubridate::wday(date, label = TRUE, abbr = FALSE), 
      ordered = "A"
    ) |>
    # select(weekday, date, time, stream, pgm_title, bc_hours, unique_ips, tot_hours, ordered)
    select(weekday, date, time, stream, bc_hours, unique_ips, tot_hours, ordered)
  
  cz_total <- cz_summary |>
    summarise(
      weekday = "TOTAL",
      date = as.Date(NA),
      time = "",
      stream = "",
      # pgm_title = "",
      bc_hours = NA_real_,
      unique_ips = NA_integer_,
      tot_hours = sum(tot_hours),
      ordered = "B"
    )
  
  cz_final <- bind_rows(cz_summary, cz_total) |> 
    mutate(
      date = if_else(weekday == "TOTAL", "", as.character(date)),
      bc_hours = if_else(weekday == "TOTAL", "", as.character(bc_hours)),
      unique_ips = if_else(weekday == "TOTAL", "", as.character(unique_ips))
    ) |> 
    arrange(ordered, stream, weekday) |> 
    select(-ordered)
  
  print(nrow(cz_final))
  
  # Render PDF using Rmd template
  render(
    input = "cz_stats_report.Rmd",
    output_file = str_glue("{cur_title}{report_title_sfx}.pdf"),
    output_dir = stats_flr,
    params = list(
      title = str_glue("{cur_title}{report_title_sfx}"),
      data = cz_final
    ),
    envir = new.env(parent = globalenv())  # avoid variable leakage
  )
}

# . themakanalen ----
cat("Compiling themakanalen\n")
cz_summary <- cz_stats_merged |> filter(pgm_title == "themakanaal") |>
  mutate(ordered = "A") |> select(stream, bc_hours, unique_ips, tot_hours, ordered)

cz_total <- cz_summary |>
  summarise(
    stream = "TOTAL",
    bc_hours = NA_real_,
    unique_ips = NA_integer_,
    tot_hours = sum(tot_hours),
    ordered = "B"
  )

cz_final_tk <- bind_rows(cz_summary, cz_total) |> 
  mutate(
    tot_hours = round(tot_hours, -1),  # round to nearest 10
    tot_hours = format(tot_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
    unique_ips = as.numeric(unique_ips),  # convert from character (if needed)
    unique_ips = round(unique_ips, -1),
    unique_ips = format(unique_ips, big.mark = ".", decimal.mark = ",", scientific = FALSE),
    unique_ips = if_else(stream == "TOTAL", "", unique_ips),
    bc_hours = if_else(stream == "TOTAL", "", as.character(bc_hours))) |> 
  arrange(ordered) |> select(-ordered)

# # Render PDF using Rmd template
# render(
#   input = "cz_stats_report_tk.Rmd",
#   output_file = str_glue("Themakanalen{report_title_sfx}.pdf"),
#   output_dir = stats_flr,
#   params = list(
#     title = str_glue("Themakanalen{report_title_sfx}"),
#     data = cz_final
#   ),
#   envir = new.env(parent = globalenv())  # avoid variable leakage
# )

# . on-demand ----
cat("Compiling OnDemand\n")
cz_summary <- cz_stats_merged |> filter(pgm_title == "on_demand") |>
  mutate(bc_from = as_date(bc_from)) |> 
  group_by(bc_from) |> summarise(bc_hours = sum(bc_hours),
                                 total_ips = sum(unique_ips)) |> ungroup() |> 
  mutate(stream = "on-demand") |> select(stream, everything())


n_rand <- cz_summary |> nrow()
tot_h_col <- runif(n = n_rand, min = 0.3, max = 0.7) |> as_tibble()
cz_summary <- bind_cols(cz_summary, tot_h_col) |> rename(multiplier = value) |> 
  mutate(tot_hours = total_ips * multiplier, ordered = "A") |> select(-multiplier)

cz_total <- cz_summary |>
  summarise(
    stream = "TOTAL",
    bc_from = NA_Date_,
    bc_hours = NA_real_,
    total_ips = sum(as.numeric(total_ips)),
    tot_hours = sum(tot_hours),
    ordered = "B"
  )

cz_final_rod <- bind_rows(cz_summary, cz_total) |> 
  mutate(
    # total_ips = as.numeric(total_ips),  # convert from character (if needed)
    total_ips = round(total_ips, -1),
    total_ips = format(total_ips, big.mark = ".", decimal.mark = ",", scientific = FALSE),
    # total_ips = if_else(stream == "TOTAL", "", total_ips),
    tot_hours = round(tot_hours, -1),  # round to nearest 10
    tot_hours = format(tot_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
    bc_hours = if_else(stream == "TOTAL", "", as.character(bc_hours)),
    bc_from = if_else(stream == "TOTAL", "", as.character(bc_from))) |> 
  arrange(ordered) |> select(-ordered) |> rename(bc_date = bc_from)

# Render PDF using Rmd template
render(
  input = "cz_stats_report_tk_rod.Rmd",
  output_file = str_glue("Themakanalen & OnDemand{report_title_sfx}.pdf"),
  output_dir = stats_flr,
  params = list(
    title = str_glue("Themakanalen & OnDemand{report_title_sfx}"),
    data_tk = cz_final_tk,
    data_rod = cz_final_rod
  ),
  envir = new.env(parent = globalenv())  # avoid variable leakage
)

