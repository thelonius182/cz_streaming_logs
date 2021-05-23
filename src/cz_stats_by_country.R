library(tidyr)
library(dplyr)
library(stringr)
library(readr)
library(lubridate)
library(fs)
library(futile.logger)
library(curlconverter)
library(jsonlite)
library(httr)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_cha_log")

cz_curl <- "curl 'https://freegeoip.app/json/208.94.246.226' \
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

cz_straight <- straighten(cz_curl)

cz_res <- make_req(cz_straight, add_clip = F)

cz_geo <- toJSON(content(cz_res[[1]](), as="parsed"), auto_unbox = TRUE, pretty=TRUE)
# 

library(lutz)
tz_lookup_coords(lat = 37.751, lon = -97.822, method = "accurate")
tz_offset("2021-03-07", tz = "America/Chicago")
