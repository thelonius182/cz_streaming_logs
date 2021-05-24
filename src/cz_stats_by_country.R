library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)
library(curlconverter)
library(jsonlite)
library(magrittr)
library(httr)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

cz_stats_cha_08 <- readRDS("cz_stats_cha_08.RDS")

cz_unique_ipa <- cz_stats_cha_08 %>% 
  filter(cz_ts < ymd_hms("2021-03-01 00:00:00")) %>% 
  select(cz_ipa) %>% 
  distinct() %>% 
  mutate(ip_idx = row_number(),
         bat_id = 1 + ((ip_idx - ip_idx %% 9000) / 9000))

cz_curl_template <- "curl 'https://freegeoip.app/json/¶IPA¶' \
  -H 'authority: freegeoip.app' \
  -H 'dnt: 1' \
  -H 'upgrade-insecure-requests: 1' \
  -H 'user-agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0' \
  -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
  -H 'sec-fetch-site: none' \
  -H 'sec-fetch-mode: navigate' \
  -H 'sec-fetch-user: ?1' \
  -H 'sec-fetch-dest: document' \
  -H 'accept-language: nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7' \
  -H 'cookie: __cfduid=de21a991dc201214c6f5e3971f1d930621616846464' \
  --compressed  "

ipa_batch <- cz_unique_ipa %>% filter(bat_id == 4)
cz_geo_set = NULL

for (cur_ipa in ipa_batch$cz_ipa) {
  
  cz_curl <- str_replace(cz_curl_template, "¶IPA¶", cur_ipa)
  cz_straight <- straighten(cz_curl)
  cz_res <- make_req(cz_straight, add_clip = F)
  cz_geo <- toJSON(content(cz_res[[1]](), as="parsed"), auto_unbox = TRUE, pretty=TRUE)
  cz_geo_ti <- fromJSON(cz_geo, simplifyDataFrame = T) %>% as_tibble()
  
  if (is.null(cz_geo_set)) {
    cz_geo_set <- cz_geo_ti
  } else {
    cz_geo_set %<>% add_row(cz_geo_ti) 
  }
  
  flog.info(paste0("new geo for IP ", cur_ipa), name = "cz_stats_cha_log")
  Sys.sleep(0.5)
}

saveRDS(cz_geo_set, "cz_geo_set_4.RDS")

# cz_curl <- "curl 'https://freegeoip.app/json/208.94.246.226' \
#   -H 'authority: freegeoip.app' \
#   -H 'dnt: 1' \
#   -H 'upgrade-insecure-requests: 1' \
#   -H 'user-agent: Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:87.0) Gecko/20100101 Firefox/87.0' \
#   -H 'accept: text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9' \
#   -H 'sec-fetch-site: none' \
#   -H 'sec-fetch-mode: navigate' \
#   -H 'sec-fetch-user: ?1' \
#   -H 'sec-fetch-dest: document' \
#   -H 'accept-language: nl-NL,nl;q=0.9,en-US;q=0.8,en;q=0.7' \
#   -H 'cookie: __cfduid=de21a991dc201214c6f5e3971f1d930621616846464' \
#   --compressed  "

# 
# 
# library(lutz)
# tz_lookup_coords(lat = 37.751, lon = -97.822, method = "accurate")
# tz_offset("2021-03-07", tz = "America/Chicago")
