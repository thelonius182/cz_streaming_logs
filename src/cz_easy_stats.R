# ount unique hourly listeners by stream group
count_listeners <- function(log_data) {
  
  log_data |>
    mutate(
      session_end = ts + d,
      session_hours = map2(ts, session_end, \(start, end) {
        seq(
          from = floor_date(start, "hour"),
          to = floor_date(end, "hour"),
          by = "hour"
        )
      })
    ) |>
    select(ip_address, stream, session_hours) |>
    unnest(session_hours) |>
    distinct(ip_address, stream, session_hour = session_hours) |>
    count(stream, session_hour, name = "unique_listener_count") |>
    select(session_hour, stream, unique_listener_count)
}

cz_wj_streams <- read_tsv("/home/lon/Documents/cz_stats_data/streams.tsv", col_types = cols(.default = "c"))
stream_order <- c("cz_live_stream", "woj_live_stream", "themakanaal_bach", "themakanaal_barok", "themakanaal_gehoorde_stilte", 
                  "themakanaal_gregoriaans", "themakanaal_hardbop", "themakanaal_hedendaags", "themakanaal_jazz", 
                  "themakanaal_klassiek", "themakanaal_oude_muziek", "overige_themakanalen")

cz_stats_cha <- read_rds(file = "/home/lon/Documents/cz_stats_data/2025-01/cz_stats_cha.RDS")
unique_monthly_listeners <- cz_stats_cha |> distinct(lg_ip) |> nrow()

# variant 1
cz_wj_stats <- cz_stats_cha |> mutate(stream = paste0(str_to_lower(bc_src), "_", lg_http_req)) |> 
  select(ip_address = lg_ip, ts = lg_cz_ts, d = frag_seconds, stream) |> 
  left_join(cz_wj_streams, by = join_by(stream)) |> select(-stream) |> rename(stream = cz_redactie)

# variant 2
# cz_wj_stats <- cz_stats_cha |> 
#   select(ip_address = lg_ip, ts = lg_cz_ts, d = frag_seconds, stream = lg_http_req)

cutoff <- ymd_hms("2025-02-01 00:00:00", tz = "Europe/Amsterdam")
cz_wj_counts <- count_listeners(log_data = cz_wj_stats) |> filter(session_hour < cutoff) 
cz_wj_counts_sorted <- cz_wj_counts |> mutate(stream = factor(stream, levels = stream_order)) |> arrange(session_hour, stream)
# cz_wj_counts_sorted <- cz_wj_counts |> arrange(session_hour, stream)
# cz_wj_counts_sorted_tz <- cz_wj_counts_sorted |> mutate(session_hour = format(session_hour, tz = "Europe/Amsterdam", usetz = TRUE)) 

# write_tsv(x = cz_wj_counts_sorted_tz, file = "/home/lon/Documents/cz_stats_data/2025-01/cz_wj_counts.tsv", na = "", append = FALSE)
