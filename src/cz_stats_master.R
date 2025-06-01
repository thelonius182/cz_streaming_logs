# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# 1. adjust `current_month` in config.yaml
# 2. on AnyDesk/Nipper-pc:
#     a. run C:\Users\nipper\Documents\cz_queries > cz_wj_stats_hourly_titles.sql
#     b. export as tab-sep file from MySQL Workbench as `salsa_stats_all_pgms.txt` (sic) into g:\salsa
#     c. download to UBU-VM into C:\u2\muziekweb_downloads\cz_stats_wpdata >> /mnt/muw/cz_stats_wpdata/
# 3. run this script manually up to the ">> MARK" mark.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

pacman::p_load(magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, kableExtra, openxlsx,
               purrr, jsonlite, httr, yaml, ssh, googledrive, data.table, tinytex, rmarkdown)

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# get reporting month----
cz_stats_cfg <- read_yaml("config.yaml")
cz_reporting_day_one_chr <- cz_stats_cfg$current_month
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_reporting_start <- cz_reporting_day_one - ddays(1)
cz_reporting_stop <- ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")

# init logger ----
fa <- flog.appender(appender.file(cz_stats_cfg$log_appender_file), "statslog")
flog.info(paste0("selecting logs for ", cz_reporting_day_one_chr), name = "statslog")

# init stats directories
stats_flr <- paste0(cz_stats_cfg$stats_data_home, str_sub(cz_reporting_day_one_chr, 1, 7))
dir_create(stats_flr)
stats_flr_reports <- str_glue("{stats_flr}/reports")

# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# >> MARK ----
# Execute manually upto this point, then store salsa_stats_all_pgms.txt from /mnt/muw/cz_stats_wpdata/ into 
# {stats_flr} and continue from here
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

# prep hourly titles ----
titles_by_pgm_start.all <- read_tsv(str_glue("{stats_flr}/salsa_stats_all_pgms.txt"), col_types = cols(.default = "c")) |> 
  group_by(pgm_start, post_type) |> mutate(idx = row_number()) |> ungroup() |> filter(idx == 1) |> 
  mutate(pgm_title = str_replace_all(pgm_title, "&amp;", "&"),
         pgm_title = case_when(str_detect(pgm_title, 
                                          pattern = regex("acoustic moods", ignore_case = TRUE)) ~ "Acoustic Moods",
                               str_detect(pgm_title, 
                                          pattern = regex("concertzender live", ignore_case = TRUE)) ~ "Concertzender Live",
                               str_detect(pgm_title, 
                                          pattern = regex("folk it", ignore_case = TRUE)) ~ "Folk It!",
                               str_detect(pgm_title, 
                                          pattern = regex("folk sounds", ignore_case = TRUE)) ~ "New Folk Sounds on Air",
                               str_detect(pgm_title, 
                                          pattern = regex("framework", ignore_case = TRUE)) ~ "Framework",
                               str_detect(pgm_title, 
                                          pattern = regex("geen dag zonder bach", ignore_case = TRUE)) ~ "Geen Dag zonder Bach",
                               str_detect(pgm_title, 
                                          pattern = regex("klankcaf", ignore_case = TRUE)) ~ "Klankcafé",
                               str_detect(pgm_title, 
                                          pattern = regex("kraak helder", ignore_case = TRUE)) ~ "Kraak Helder",
                               str_detect(pgm_title, 
                                          pattern = regex("kroniek van", ignore_case = TRUE)) ~ "Kroniek van de Nederlandse Muziek",
                               str_detect(pgm_title, 
                                          pattern = regex("lüdeke", ignore_case = TRUE)) ~ "Lüdeke Straightahead",
                               str_detect(pgm_title, 
                                          pattern = regex("mazen van het net", ignore_case = TRUE)) ~ "Door de Mazen van het Net",
                               str_detect(pgm_title, 
                                          pattern = regex("muziek-?essay", ignore_case = TRUE)) ~ "Radio Muziek-essay",
                               str_detect(pgm_title, 
                                          pattern = regex("ochtendeditie", ignore_case = TRUE)) ~ "Ochtendeditie",
                               str_detect(pgm_title, 
                                          pattern = regex("oriënt", ignore_case = TRUE)) ~ "Oriënt Express",
                               str_detect(pgm_title, 
                                          pattern = regex("piano-?etude", ignore_case = TRUE)) ~ "De piano-etude, van oefening tot kunst",
                               str_detect(pgm_title, 
                                          pattern = regex("popart", ignore_case = TRUE)) ~ "Popart",
                               str_detect(pgm_title, 
                                          pattern = regex("rondje evenaar", ignore_case = TRUE)) ~ "Radio Rondje Evenaar",
                               str_detect(pgm_title, 
                                          pattern = regex("vredenburg live", ignore_case = TRUE)) ~ "Vredenburg live!",
                               pgm_title == "American Highways | Rush Hour" ~ "American Highways",
                               pgm_title == "Brazil In A Nutshell" ~ "Brazil in a Nutshell",
                               pgm_title == "CD van de week" ~ "CD van de Week",
                               pgm_title == "Componist van de maand" ~ "Componist van de Maand",
                               pgm_title == "Dansen en de blues" ~ "Dansen en de Blues",
                               pgm_title == "De boekenkast" ~ "De Boekenkast",
                               pgm_title == "De diva op de erwt" ~ "De Diva op de erwt",
                               pgm_title == "De Vorige eeuw" ~ "De Vorige Eeuw",
                               pgm_title == "De wandeling" ~ "De Wandeling",
                               pgm_title == "Disc-cover" ~ "Disc-Cover!",
                               pgm_title == "Disc-Cover" ~ "Disc-Cover!",
                               pgm_title == "Een Vroege Wandeling" ~ "Een vroege wandeling",
                               pgm_title == "Front Runnin'" ~ "Front Runnin’",
                               pgm_title == "Het Chanson" ~ "Chanson",
                               pgm_title == "JazzNotJazz Music & Politics" ~ "JazzNotJazz",
                               pgm_title == "Klassieke muziek actua" ~ "Klassieke muziek Actua",
                               pgm_title == "L'Esprit Baroque" ~ "L’Esprit Baroque",
                               pgm_title == "Missa etcetera" ~ "Missa Etcetera",
                               pgm_title == "Moanin' the Blues" ~ "Moanin’ the Blues",
                               pgm_title == "New Folk Sounds On Air" ~ "New Folk Sounds on Air",
                               pgm_title == "Nieuw Verschenen" ~ "Nieuw verschenen",
                               pgm_title == "Solta a franga" ~ "Solta a Franga",
                               pgm_title == "Tango: Lied van Buenos Aires" ~ "Tango: lied van Buenos Aires",
                               pgm_title == "The sound of movies" ~ "The Sound of Movies",
                               pgm_title == "Tussen droom en daad" ~ "Tussen Droom en Daad",
                               pgm_title == "UUR 23: Spinning the Blues / Popart Live!" ~ "Spinning the Blues",
                               pgm_title == "Vocal Jazz" ~ "Vocale Jazz",
                               pgm_title == "Wereldmuziek NL" ~ "Wereldmuziek NL!",
                               pgm_title == "Wereldmuze" ~ "WereldMuze",
                               pgm_title == "X-rated" ~ "X-Rated",
                               pgm_title == "Zuiver Klassiek" ~ "Zuiver klassiek",
                               pgm_title == "Zwerven door de renaissance" ~ "Zwerven door de Renaissance",
                               pgm_title == "Zwerven door de barok" ~ "Zwerven door de Barok",
                               TRUE ~ pgm_title)) |> select(-idx)

stamp_pgm_ts <- lubridate::stamp("19581225", locale = "nl_NL.utf8", orders = "ymd", quiet = TRUE)
pgm_start_min <- paste0(stamp_pgm_ts(cz_reporting_start), "_99")
pgm_start_max <- paste0(stamp_pgm_ts(cz_reporting_stop), "_00")
titles_by_pgm_start.cur_mnth <- titles_by_pgm_start.all |> filter(pgm_start_min < pgm_start & pgm_start < pgm_start_max)

# prep STREAM+ROD logs ----
if (!dir_exists(stats_flr_reports)) {
  dir_create(stats_flr_reports)
  
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
    select(ip_address = lg_ipa, ts = lg_ts, d, stream, lg_audio_file)
  
  # . - map pgms ----
  cz_stats_rod.2a <- cz_stats_rod.2 |> select(lg_audio_file, ip_address, stream) |> distinct() |> 
    group_by(lg_audio_file) |> mutate(n = n()) |> ungroup() |> filter(str_detect(stream, "on_d")) |> 
    mutate(pgm_file = path_file(lg_audio_file),
           pgm_start = str_extract(pgm_file, "[0-9]{8}-[0-9]{2}"),
           pgm_start = str_replace(pgm_start, "[-]", "_"),
           post_type = if_else(stream == "on_demand_cz", "programma", "programma_woj")) |> distinct() |> 
    left_join(titles_by_pgm_start.all, by = join_by(pgm_start, post_type)) |> select(-ip_address) |> distinct() |> 
    filter(!is.na(pgm_title)) |> 
    select(pgm_title, post_type, n) |> group_by(pgm_title, post_type) |> summarise(tot_listeners = sum(n)) |> ungroup() 
  
  # . store as RDS ----
  write_rds(cz_stats_rod.2, str_glue("{stats_flr}/cz_stats_rod.2.RDS"))
  write_rds(cz_stats_rod.2a, str_glue("{stats_flr}/cz_stats_rod.2a.RDS"))
  cat("ROD logfiles prepped\n")
  rm(cz_stats_rod.1)
} else {
  cz_stats_cha.1a <- read_rds(str_glue("{stats_flr}/cz_stats_cha.1a.RDS"))
  cz_stats_rod.2 <- read_rds(str_glue("{stats_flr}/cz_stats_rod.2.RDS"))
  cz_stats_rod.2a <- read_rds(str_glue("{stats_flr}/cz_stats_rod.2a.RDS"))
}

flog.info(paste0("line count streams = ", nrow(cz_stats_cha.1a)), name = "statslog")
flog.info(paste0("line count rod's = ", nrow(cz_stats_rod.2)), name = "statslog")

# link stream names ----
cz_wj_streams <- read_tsv(cz_stats_cfg$cz_wj_streams, col_types = cols(.default = "c"))
stream_order <- c("cz_live_stream", "woj_live_stream", "themakanaal_bach", "themakanaal_barok", 
                  "themakanaal_chanson", "themakanaal_film",
                  "themakanaal_gehoorde_stilte", "themakanaal_gregoriaans", "themakanaal_hardbop", 
                  "themakanaal_hedendaags", "themakanaal_jazz", "themakanaal_jazznotjazz",
                  "themakanaal_klassiek", "themakanaal_orientexpress",
                  "themakanaal_oude_muziek", "themakanaal_wereldmuziek", "overige_themakanalen")

cz_stats_cha.2 <- cz_stats_cha.1a |> mutate(stream = paste0(str_to_lower(bc_src), "_", lg_http_req),
                                            te = lg_cz_ts + frag_seconds,
                                            id_cha = row_number()) |> 
  select(id_cha, ip_address = lg_ip, ts = lg_cz_ts, d = frag_seconds, te, stream) |> 
  left_join(cz_wj_streams, by = join_by(stream)) |> select(-stream) |> rename(stream = cz_redactie)

# . set data.table cha-stats ----
dt_cz_stats_cha <- as.data.table(cz_stats_cha.2) # |> filter(str_detect(stream, "live_stream$")))

# load hourly titles ----
# titles_by_pgm_start.cur_mnth <- read_tsv(str_glue("{stats_flr}/hourly_titles.txt"), 
#                               col_types = cols(.default = "c")) |> 
#   rename(pgm_start = pgmStart, pgm_stop = pgmStop, pgm_title = pgmTitle)

# . add theme channels ----
titles_tk <- read_tsv(str_glue("{cz_stats_cfg$stats_data_home}/titles_theme_channels.tsv"), 
                             col_names = c("pgm_title", "post_type"),
                             col_types = cols(.default = "c")) |> 
  mutate(pgm_start = titles_by_pgm_start.cur_mnth[1, ]$pgm_start, 
         pgm_stop = titles_by_pgm_start.cur_mnth[nrow(titles_by_pgm_start.cur_mnth), ]$pgm_stop) |> 
  select(pgm_start, pgm_stop, pgm_title, post_type)

hourly_titles_merged <- bind_rows(titles_by_pgm_start.cur_mnth, titles_tk)
hourly_titles_fixed <- hourly_titles_merged |> 
  mutate(station = case_when(post_type == "programma" ~ "cz_live_stream", 
                             post_type == "programma_woj" ~ "woj_live_stream",
                             TRUE ~ pgm_title),
         bc_from = ymd_h(pgm_start, tz = "Europe/Amsterdam"),
         bc_from60 = bc_from + 60, # should have listened to this program for at least 60 seconds
         bc_to = ymd_h(pgm_stop, tz = "Europe/Amsterdam"),
         bc_to60 = bc_to - 60, # should have listened to this program for at least 60 seconds
         id_tit = row_number()) |> 
  select(-post_type, -pgm_start, -pgm_stop)

# . set data.table hours ----
dt_hourly_titles <- as.data.table(hourly_titles_fixed)

# get session matches ----
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
# rod_stream_order <- c("on_demand_cz", "concertpodium", "on_demand_woj")
# cz_stats_rod.3 <- count_listeners(log_data = cz_stats_rod.2) |> filter(session_hour < cz_reporting_stop) 
# cz_stats_sessions_rod <- cz_stats_rod.3 |> 
#   mutate(stream = factor(stream, levels = rod_stream_order)) |> 
#   arrange(session_hour, stream) |> 
#   rename(bc_from = session_hour, unique_ips = unique_listener_count) |> 
#   mutate(pgm_title = NA_character_, tot_hours = NA_real_, bc_hours = 1) |> 
#   select(pgm_title, stream, bc_from, bc_hours, unique_ips, tot_hours)
  
# link stats and reorder ----
merged_stream_order <- c("cz_live_stream", "woj_live_stream", "cz_on_demand", "woj_on_demand",
                         "themakanaal_bach", "themakanaal_barok", "themakanaal_chanson", "themakanaal_film",
                         "themakanaal_gehoorde_stilte", "themakanaal_gregoriaans", "themakanaal_hardbop", 
                         "themakanaal_hedendaags", "themakanaal_jazz", "themakanaal_jazznotjazz",
                         "themakanaal_klassiek", "themakanaal_orientexpress", "themakanaal_oude_muziek", 
                         "themakanaal_wereldmuziek", "overige_themakanalen", "RoD_maand", "TOTAAL")

cz_stats_merged.1 <- cz_stats_sessions_cha |> 
  mutate(stream = factor(stream, levels = merged_stream_order)) |>
  arrange(bc_from, stream, pgm_title) |>
  select(bc_from, stream, pgm_title, everything()) |> 
  mutate(pgm_title = if_else(is.na(pgm_title), "on_demand", pgm_title))

cz_stats_rod.4 <- cz_stats_rod.2a |> filter(pgm_title %in% cz_stats_merged.1$pgm_title) |> 
  mutate(bc_from = NA_POSIXct_, 
         stream = if_else(post_type == "programma", "cz_on_demand", "woj_on_demand"),
         stream = factor(stream, levels = merged_stream_order),
         bc_hours = NA_real_,
         unique_ips = tot_listeners,
         tot_hours = NA_real_) |> 
  select(bc_from, stream, pgm_title, bc_hours, unique_ips, tot_hours)

cz_stats_merged <- bind_rows(cz_stats_merged.1, cz_stats_rod.4) |> arrange(pgm_title, stream, desc(bc_from)) 

# store final result ----
write_rds(cz_stats_merged, str_glue("{stats_flr}/cz_stats_merged.RDS"))

# prepare excel ----
sf <- lubridate::stamp(" - luistercijfers oktober 2021", locale = "nl_NL.utf8", orders = "my", quiet = TRUE)
wb <- createWorkbook()
report_title_sfx <- sf(cz_reporting_day_one)

titles_by_desk <- read_tsv(str_glue("{stats_flr}/titel-genre koppeling.txt"), col_types = cols(.default = "c")) |> 
  mutate(titel = str_replace(titel, "&amp;", "&"))
cz_sheet <- "genres_en_redacties"
addWorksheet(wb, cz_sheet)
writeData(wb, cz_sheet, titles_by_desk)

cz_sheet <- "cz_stats"
addWorksheet(wb, cz_sheet)
writeData(wb, cz_sheet, cz_stats_merged)
datetime_style <- createStyle(numFmt = "yyyy-mm-dd hh:mm")
addStyle(wb, cz_sheet, style = datetime_style, rows = 2:(1 + nrow(cz_stats_merged)), cols = 1, gridExpand = TRUE)
saveWorkbook(wb, str_glue("{stats_flr_reports}/cz_stats_merged{report_title_sfx}.xlsx"), overwrite = TRUE)

# create pdf's ----
# . live-streams ----
# cz_stats_merged_a <- cz_stats_merged |> filter(str_detect(pgm_title, "Droom"))
cz_stats_merged_a <- cz_stats_merged |> filter(!pgm_title %in% c("on_demand", "themakanaal"))

for (cur_title in unique(cz_stats_merged_a$pgm_title)) {
  cat("Rendering:", cur_title, "\n")
  
  cz_summary <- cz_stats_merged |>
    filter(pgm_title == cur_title) |>
    mutate(
      date = as_date(bc_from),
      time = format(bc_from, "%H:%M"),
      weekday = lubridate::wday(date, label = TRUE, abbr = FALSE, locale = "nl_NL.utf8"), 
      weekday = if_else(is.na(weekday), "RoD_maand", weekday),
      ordered = "A"
    ) |>
    select(weekday, date, time, stream, bc_hours, unique_ips, tot_hours, ordered)
  
  cz_total <- cz_summary |>
    summarise(
      weekday = "TOTAAL",
      date = as.Date(NA),
      time = "",
      stream = "",
      bc_hours = sum(bc_hours, na.rm = TRUE),
      unique_ips = sum(unique_ips, na.rm = TRUE),
      tot_hours = sum(tot_hours, na.rm = TRUE),
      ordered = "B"
    )
  
  cz_final <- bind_rows(cz_summary, cz_total) |> 
    mutate(
      date = if_else(weekday %in% c("TOTAAL", "RoD_maand"), "", as.character(date)),
      time = if_else(weekday %in% c("TOTAAL", "RoD_maand"), "", as.character(time)),
      bc_hours = format(bc_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
      unique_ips = format(unique_ips, big.mark = ".", decimal.mark = ",", scientific = FALSE),
      tot_hours = format(tot_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
      stream = factor(stream, levels = merged_stream_order)
    ) |> 
    arrange(ordered, weekday, date, time, stream) |> 
    select(-ordered) |> 
    rename(dag = weekday, datum = date, tijd = time, uitzenduren = bc_hours, 
           luisteraars = unique_ips, luisteruren = tot_hours) |> 
    mutate(uitzending = str_glue("{datum}  {tijd}"),
           uitzenduren = str_remove(uitzenduren, " ?NA"),
           luisteruren = str_remove(luisteruren, " ?NA"),
           stream = if_else(is.na(stream), "", stream)) |> 
    select(dag, uitzending, everything(), -datum, -tijd)
  
  print(nrow(cz_final))
  
  # Render PDF using Rmd template
  render(
    input = "cz_stats_report_pgm.Rmd",
    output_file = str_glue("{cur_title}{report_title_sfx}.pdf"),
    output_dir = stats_flr_reports,
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
  mutate(ordered = "A",
         bc_hours = if_else(stream == "overige_themakanalen", 16 * bc_hours, bc_hours)) |> 
  select(stream, bc_hours, unique_ips, tot_hours, ordered)

cz_total <- cz_summary |>
  summarise(
    stream = "TOTAAL",
    bc_hours = sum(bc_hours),
    unique_ips = sum(unique_ips),
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
    # unique_ips = if_else(stream == "TOTAAL", "", unique_ips),
    bc_hours = format(bc_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE)) |> 
  arrange(ordered) |> select(-ordered) |> 
  rename(uitzenduren = bc_hours,
         luisteraars = unique_ips,
         luisteruren = tot_hours)

# # . on-demand ----
# cat("Compiling OnDemand\n")
# cz_summary <- cz_stats_merged |> filter(pgm_title == "on_demand") |>
#   mutate(bc_from = as_date(bc_from)) |> 
#   group_by(bc_from) |> summarise(bc_hours = sum(bc_hours),
#                                  total_ips = sum(unique_ips)) |> ungroup() |> 
#   mutate(stream = "on-demand") |> select(stream, everything())
# 
# 
# n_rand <- cz_summary |> nrow()
# tot_h_col <- runif(n = n_rand, min = 0.3, max = 0.7) |> as_tibble()
# cz_summary <- bind_cols(cz_summary, tot_h_col) |> rename(multiplier = value) |> 
#   mutate(tot_hours = total_ips * multiplier, ordered = "A") |> select(-multiplier)
# 
# cz_total <- cz_summary |>
#   summarise(
#     stream = "TOTAAL",
#     bc_from = NA_Date_,
#     bc_hours = sum(bc_hours),
#     total_ips = sum(as.numeric(total_ips)),
#     tot_hours = sum(tot_hours),
#     ordered = "B"
#   )
# 
# cz_final_rod <- bind_rows(cz_summary, cz_total) |> 
#   mutate(
#     total_ips = round(total_ips, -1),
#     total_ips = format(total_ips, big.mark = ".", decimal.mark = ",", scientific = FALSE),
#     tot_hours = round(tot_hours, -1),  # round to nearest 10
#     tot_hours = format(tot_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
#     bc_hours = format(bc_hours, big.mark = ".", decimal.mark = ",", scientific = FALSE),
#     bc_from = if_else(stream == "TOTAAL", "", as.character(bc_from))) |> 
#   arrange(ordered) |> select(-ordered) |> 
#   rename(dag = bc_from, uitzendingen = bc_hours, luisteraars = total_ips, luisteruren = tot_hours)

# . unique IP's ----
unique_ips <- unique(c(unique(cz_stats_rod.2$ip_address), unique(cz_stats_cha.1a$lg_ip))) |> length()

# . rendering ----
render(
  input = "cz_stats_report_tk.Rmd",
  output_file = str_glue("Themakanalen{report_title_sfx}.pdf"),
  output_dir = stats_flr_reports,
  params = list(
    title = str_glue("Themakanalen{report_title_sfx}"),
    data_tk = cz_final_tk,
    data_ips = unique_ips
  ),
  envir = new.env(parent = globalenv())  # avoid variable leakage
)

# upload GD ----
drive_auth(cache = ".secrets", email = "cz.teamservice@gmail.com")
sf_flr_a <- lubridate::stamp("1985-07", locale = "nl_NL.utf8", orders = "ym", quiet = TRUE)
sf_flr_b <- lubridate::stamp("juli", locale = "nl_NL.utf8", orders = "m", quiet = TRUE)
flr_a <- sf_flr_a(cz_reporting_day_one)
flr_b <- sf_flr_b(cz_reporting_day_one)
gd_parent_flr_chr <- "Statistieken/luistercijfers_2025/"
gd_parent_flr <- drive_get(path = gd_parent_flr_chr)
drive_mkdir(str_glue("{flr_a}, {flr_b}"), path = gd_parent_flr)
gd_flr <- drive_get(path = str_glue("{gd_parent_flr_chr}{flr_a}, {flr_b}"))
gd_flr_id <- as_id(gd_flr)

stats_files <- list.files(str_glue("{stats_flr}/reports"), full.names = TRUE, pattern = "\\.pdf$")
walk(stats_files, ~ drive_upload(.x, path = gd_flr_id, type = "application/pdf"))

stats_files <- list.files(str_glue("{stats_flr}/reports"), full.names = TRUE, pattern = "\\.xlsx$")
walk(stats_files, ~ drive_upload(.x, path = gd_flr_id, 
                                 type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"))
