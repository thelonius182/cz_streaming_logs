library(tidyverse)
library(fs)
library(magrittr)
library(jsonlite)
library(httr)

stats_rds.1 <- dir_ls(path = "/home/lon/Documents/cz_stats_data/", regexp = ".*cz_stats_cha_08.*", recurse = T) %>% as_tibble()

rds_all.1 <- NULL

for (a_file in stats_rds.1$value) {
  rds.1 <- read_rds(a_file) %>% select(cz_ipa) %>% distinct()
  
  if (is_null(rds_all.1)) {
    rds_all.1 <- rds.1
  } else {
    rds_all.1 %<>% bind_rows(rds.1)
  }
  
}

rds_all.2 <- rds_all.1 %>% as_tibble() %>% distinct()

cz_ipa_geo.1 <- read_rds(file = "cz_ipa_geo_full.RDS") %>% as_tibble() %>% 
  select(ip) %>% rename(cz_ipa = ip) %>% filter(!is.na(cz_ipa)) %>% distinct()

rds_all.3 <- rds_all.2 %>% anti_join(cz_ipa_geo.1)

ipstack_key = "825a28031a3f92f76608202d5f9e1bb2"
http_template <- paste0("http://api.ipstack.com/@CZ_IPA?access_key=",
                        ipstack_key, 
                        "&fields=region_name,continent_name,country_code,country_name,city&output=json")
rds_all.4 <- NULL
wi_loops <- 0

for (an_ip in rds_all.3$cz_ipa) {
  
  an_ip <- cz_ipa_geo.1$cz_ipa[[1]]
  # print(paste("ip =", an_ip))
  ip_request <- http_template %>% str_replace("@CZ_IPA", "77.251.72.219")
  geo_response.1 <- GET(ip_request)

  # service-error > skip
  if (status_code(geo_response.1) != 200) {
    next
  }
  
  geo_response.2 <- content(geo_response.1, "parsed") %>% as_tibble() %>% mutate(cz_ipa = "77.251.72.219")

  # bad request > skip
  if ("error" %in% names(geo_response.2)) {
    next
  }

  if (is_null(rds_all.4)) {
    rds_all.4 <- geo_response.2
  } else {
    rds_all.4 %<>% bind_rows(geo_response.2)
  }
  
  wi_loops <- wi_loops + 1
  
  if (wi_loops == 15) {
    break
  }
}

write_rds(x = geo_response.2,
          file = "geo_response_dft.RDS",
          compress = "gz")

cat("sleeping ", headers_timeout, "\n")
Sys.sleep(headers_timeout)
  



