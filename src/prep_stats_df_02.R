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

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_cha.log"), "cz_stats_proc_log")

# load known geo data
cz_ipa_geo_full <- readRDS("cz_ipa_geo_full.RDS")

# extract new ip-addresses
# from stats collected in prep_stats_df_01.R
cz_new_ipa <- cz_stats_joined_01 %>% 
  select(cz_ipa) %>% 
  distinct() %>% 
  # only the new ones
  anti_join(cz_ipa_geo_full, by = c("cz_ipa" = "ip")) %>% 
  # split into batches, to stay within freegeoip limit of allowed number of requests
  mutate(ip_idx = row_number(),
         bat_id = 1 + ((ip_idx - ip_idx %% 9000) / 9000))

# log number of new arrivals and departures
flog.info(paste0("Number of new ip-addresses in past month = ", nrow(cz_new_ipa)), name = "cz_stats_proc_log")

cz_departures <- cz_ipa_geo_full %>% anti_join(cz_stats_joined_01, by = c("ip" = "cz_ipa"))

flog.info(paste0("Number of lost ip-addresses in past month = ", nrow(cz_departures)), name = "cz_stats_proc_log")

# prep geo requests template
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

# fetch geo-info, once for each batch
max_batch_id <- max(cz_new_ipa$bat_id)

for (b1 in 1:max_batch_id) {
  # give geo-server a break
  Sys.sleep(10.0)
  
  # process (next) batch
  ipa_batch <- cz_new_ipa %>% filter(bat_id == b1)
  cz_geo_set = NULL
  
  for (cur_ipa in ipa_batch$cz_ipa) {
    cz_curl <- str_replace(cz_curl_template, "¶IPA¶", cur_ipa)
    cz_straight <- straighten(cz_curl)
    cz_res <- make_req(cz_straight, add_clip = F)
    cz_geo <-
      toJSON(content(cz_res[[1]](), as = "parsed"),
             auto_unbox = TRUE,
             pretty = TRUE)
    cz_geo_ti <-
      fromJSON(cz_geo, simplifyDataFrame = T) %>% as_tibble()
    
    if (is.null(cz_geo_set)) {
      cz_geo_set <- cz_geo_ti
    } else {
      cz_geo_set %<>% add_row(cz_geo_ti)
    }
    
    flog.info(paste0("new geo for IP ", cur_ipa), name = "cz_stats_cha_log")
    Sys.sleep(0.5)
  }
  
  # add batch: this prevents processing the same ip-address more than once (between months)
  cz_ipa_geo_full <- cz_ipa_geo_full %>%
    bind_rows(cz_geo_set)
}

# finally: persist the new list
write_rds(x = cz_ipa_geo_full,
          file = "cz_ipa_geo_full.RDS",
          compress = "gz")
