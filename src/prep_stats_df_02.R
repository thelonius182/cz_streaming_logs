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
library(yaml)

fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

source("src/prep_funcs.R", encoding = "UTF-8")
cz_stats_cfg <- read_yaml("config.yaml")

cz_stats_rod.10 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_rod.10.RDS")) # from prep_rod4.R (RoD)
cz_stats_cha_08 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_cha_08.RDS")) # from prep8.R (Live + ThCh)
cz_ipa_geo_full <- read_rds(file = "cz_ipa_geo_full.RDS") # from previous prep_stats_df_02.R run
cz_ipa_geo_full.1 <- cz_ipa_geo_full %>% select(ip) %>% distinct()

# full set current month: RoD + Live + ThCh
cz_stats_joined_01 <- cz_stats_cha_08 %>%
  bind_rows(cz_stats_rod.10) %>%
  select(-cz_row_id, -cz_id, -cz_cha_id)

# unique IP's in full set current month
cz_stats_joined_01.1 <- cz_stats_joined_01 %>% 
  select(cz_ipa) %>% 
  distinct()

# extract new IP's ----
cz_new_ipa <- cz_stats_joined_01.1 %>% anti_join(cz_ipa_geo_full.1, by = c("cz_ipa" = "ip"))

# log number of new arrivals and departures
flog.info(paste0("Number of all known ip-addresses = ", nrow(cz_ipa_geo_full.1)), name = "cz_stats_proc_log")
flog.info(paste0("Number of ip-addresses in reported month = ", nrow(cz_stats_joined_01.1)), name = "cz_stats_proc_log")
flog.info(paste0("Number of new arrivals in reported month = ", nrow(cz_new_ipa)), name = "cz_stats_proc_log")

ipstack_key = "825a28031a3f92f76608202d5f9e1bb2"
geo_request_template <- paste0("http://api.ipstack.com/@CZ_IPA?access_key=",
                        ipstack_key, 
                        "&fields=region_name,continent_name,country_code,country_name,city&output=json")

geo_response_template <- read_rds(file = "geo_response_dft.RDS")
geo_response_set <- geo_response_template %>% mutate(cz_ipa = "to_be_removed")
wi_loops <- 0

for (an_ip in cz_new_ipa$cz_ipa) {
  
  # an_ip <- cz_new_ipa$cz_ipa[[1]]
  # print(paste("ip =", an_ip))
  
  # prep current dft-response (geo_response_set should have all IP's in cz_new_ipa, also the ones ipstack.com refuses)
  geo_response_dft <- geo_response_template %>% mutate(cz_ipa = an_ip)
  
  # get the IP-details
  geo_request <- geo_request_template %>% str_replace("@CZ_IPA", an_ip)
  geo_response.1 <- GET(geo_request)
  
  # service-error
  if (status_code(geo_response.1) != 200) {
    geo_response_set %<>% bind_rows(geo_response_dft)
    next
  }
  
  geo_response.2 <- content(geo_response.1, "parsed") %>% as_tibble() %>% mutate(cz_ipa = an_ip)
  wi_loops <- wi_loops + 1
  
  # invalid response
  if ("error" %in% names(geo_response.2)) {
    geo_response_set %<>% bind_rows(geo_response_dft)
    next
  }

  geo_response_set %<>% bind_rows(geo_response.2)
  
  if (wi_loops == 2) {
    break
  }
}

# store the set, to prevent processing the same ip-address more than once (across months)
# - after removing the stub!
geo_response_set <- geo_response_set %>% filter(cz_ipa != "to_be_removed") %>% rename(ip = cz_ipa)
cz_ipa_geo_full %<>% bind_rows(geo_response_set)

# finally: persist the new list
write_rds(x = cz_ipa_geo_full, file = "cz_ipa_geo_full.RDS", compress = "gz")
