pacman::p_load(magrittr, tidyr, dplyr, stringr, readr, lubridate, fs, futile.logger, curlconverter,
               jsonlite, httr, yaml, ssh, googledrive, purrr, yaml)

cz_stats_cfg <- read_yaml("config.yaml")

fa <- flog.appender(appender.file(paste0(cz_stats_cfg$ws_geodata_home, "cz_stats_geodata.log")), "geodata_log")

source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_rod.10 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_rod.10.RDS")) # from prep_rod4.R (RoD)
cz_stats_cha_08 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_cha_08.RDS")) # from prep8.R (Live + ThCh)
cz_ipa_geo_full <- read_rds(file = paste0(cz_stats_cfg$ws_geodata_home, "cz_ipa_geo_full.RDS")) # from previous run
cz_ipa_geo_full.1 <- cz_ipa_geo_full %>% select(ip) %>% distinct()

# full set current month: RoD + Live + ThCh
cz_stats_joined_01 <- cz_stats_cha_08 %>%
  bind_rows(cz_stats_rod.10) %>%
  select(-cz_row_id, -cz_id, -cz_cha_id)

# unique IP's in full set current month
cz_stats_joined_01.1 <- cz_stats_joined_01 %>% select(cz_ipa) %>% distinct()

# extract new IP's ----
cz_new_ipa <- cz_stats_joined_01.1 %>% anti_join(cz_ipa_geo_full.1, by = c("cz_ipa" = "ip"))

# log number of new arrivals and departures
flog.info(paste0("Current month = ", stats_data_flr()), name = "geodata_log")
flog.info(paste0("known ip-addresses = ", nrow(cz_ipa_geo_full.1)), name = "geodata_log")
flog.info(paste0("ip-addresses this month = ", nrow(cz_stats_joined_01.1)), name = "geodata_log")
flog.info(paste0("new arrivals this month = ", nrow(cz_new_ipa)), name = "geodata_log")

# stop if maxmind stock is too low
maxmind_creds <- read_rds(file = paste0(cz_stats_cfg$ws_geodata_home, "mm_creds.RDS"))
n_queries <- queries_remaining(maxmind_creds)

for (block in 1:1) {
  
  if (nrow(cz_new_ipa) > n_queries) {
    flog.info(paste0("job canceled - maxmind subscription ran out. Available = ", 
                     n_queries,
                     ", required = ",
                     nrow(cz_new_ipa)), 
              name = "geodata_log")
    break
  } 
  
  # init maxmind loop
  geo_response_err <- read_rds(file = "geo_response_dft.RDS")
  geo_response_set <- read_rds(file = "geo_response_dft.RDS") %>% mutate(message = "stub_row")
  
  http_template <- "https://geoip.maxmind.com/geoip/v2.1/city/@CZ_IPA?pretty"
  # wi_loops <- 0
  
  for (an_ip in cz_new_ipa$cz_ipa) {
    
    # wi_loops <- wi_loops + 1
    print(an_ip)
    ip_request <- http_template %>% str_replace("@CZ_IPA", an_ip)
    req_sts <- try(geo_response.1 <- GET(url = ip_request,
                                         authenticate(maxmind_creds$account_id, maxmind_creds$license_key)))
    
    # service-error
    if (attr(req_sts, "class") == "try-error" | geo_response.1$status_code != 200) {
      geo_response <- geo_response_err %>% mutate(message = "service failed", ip = an_ip)
      geo_response_set %<>% add_row(geo_response)
      next
    } 
    
    geo_response.2 <- content(geo_response.1, as = "parsed", type = "application/json")
    
    gr3_sts <- try(geo_response.3 <- geo_response.2 %>% as.data.frame() %>% as_tibble() %>% 
      pivot_longer(cols = contains("names.en") | (contains("code") & !contains("metro")), names_to = "key", values_to = "value") %>% 
      select(lat = location.latitude, lng = location.longitude, stats_tz = location.time_zone, queries_remaining,
             key, value))
    
    # service-error
    if ("try-error" %in% attr(gr3_sts, "class")) {
      geo_response <- geo_response_err %>% mutate(message = "service failed", ip = an_ip)
      geo_response_set %<>% add_row(geo_response)
      next
    } 
    
    n_req_stock_tib <- geo_response.3 %>% select(n = queries_remaining) 
    n_req_stock <- n_req_stock_tib$n[[1]]
    
    geo_response.4 <- geo_response.3 %>% select(key, value) 
    country_code_tib <- geo_response.4 %>% filter(key == "country.iso_code") %>% select(value)
    country_name_tib <- geo_response.4 %>% filter(key == "country.names.en") %>% select(value)
    region_code_tib <- geo_response.4 %>% filter(key == "subdivisions.iso_code") %>% select(value)
    region_name_tib <- geo_response.4 %>% filter(key == "subdivisions.names.en") %>% select(value)
    city_tib <- geo_response.4 %>% filter(key == "city.names.en") %>% select(value)
    zip_code_tib <- geo_response.4 %>% filter(key == "code") %>% select(value)
    continent_name_tib <- geo_response.4 %>% filter(key == "continent.names.en") %>% select(value)
    
    geo_response <- tibble(ip = an_ip,
                           country_code = if (nrow(country_code_tib) > 0) {country_code_tib$value[[1]]} else {NA_character_},
                           country_name = if (nrow(country_name_tib) > 0) {country_name_tib$value[[1]]} else {NA_character_},
                           region_code = if (nrow(region_code_tib) > 0) {region_code_tib$value[[1]]} else {NA_character_},
                           region_name = if (nrow(region_name_tib) > 0) {region_name_tib$value[[1]]} else {NA_character_},
                           city = if (nrow(city_tib) > 0) {city_tib$value[[1]]} else {NA_character_},
                           zip_code = if (nrow(zip_code_tib) > 0) {zip_code_tib$value[[1]]} else {NA_character_},
                           time_zone = if (nrow(geo_response.3) > 0) {geo_response.3$stats_tz[[1]]} else {NA_character_},
                           latitude = if (nrow(geo_response.3) > 0) {geo_response.3$lat[[1]]} else {NA_real_},
                           longitude = if (nrow(geo_response.3) > 0) {geo_response.3$lng[[1]]} else {NA_real_},
                           metro_code = NA_integer_,
                           message = NA_character_,
                           continent_name = if (nrow(continent_name_tib) > 0) {continent_name_tib$value[[1]]} else {NA_character_}
    )
    
    geo_response_set %<>% add_row(geo_response)
    
    # if (wi_loops >= 3) {
    #   break
    # }
  }
  
  # remove the stub
  geo_response_set <- geo_response_set %>% filter(is.na(message) | message != "stub_row")
  
  # log number of failures
  n_failed <- geo_response_set %>% filter(message == "service failed") %>% nrow()
  flog.info(paste0("Failed service calls = ", n_failed), name = "geodata_log")
  
  # store the set, to prevent processing the same ip-address more than once (across months)
  if (nrow(geo_response_set) > 0) {
    cz_ipa_geo_full %<>% bind_rows(geo_response_set)
    write_rds(x = cz_ipa_geo_full, file = paste0(cz_stats_cfg$ws_geodata_home, "cz_ipa_geo_full.RDS"), compress = "gz")
  }
}

# ### handy to save this ###
# geo_response <- tibble(ip = an_ip,
#                        country_code = NA_character_,
#                        country_name = NA_character_,
#                        region_code = NA_character_,
#                        region_name = NA_character_,
#                        city = NA_character_,
#                        zip_code = NA_character_,
#                        time_zone = NA_character_,
#                        latitude = NA_real_,
#                        longitude = NA_real_,
#                        metro_code = NA_integer_,
#                        message = NA_character_,
#                        continent_name = NA_character_
# )
