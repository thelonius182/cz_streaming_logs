library(stringr)
library(dplyr)
library(tidyr)
library(lubridate)


# zenderschema ------------------------------------------------------------

tbl_zenderschema.1 <- tbl_raw_zenderschema %>% 
  mutate(start = str_pad(string = start, side = "left", width = 5, pad = "0"), 
         slot = paste0(str_sub(dag, start = 1, end = 2), start)
  ) %>% 
  rename(hh_formule = `hhOffset-dag.uur`,
         wekelijks = `elke week`,
         AB_cyclus = `twee-wekelijks`,
         cyclus_A = A,
         cyclus_B = B,
         week_1 = `week 1`,
         week_2 = `week 2`,
         week_3 = `week 3`,
         week_4 = `week 4`,
         week_5 = `week 5`
  ) 

tbl_zenderschema.2 <- tbl_zenderschema.1 %>%
  select(-starts_with("r"), -starts_with("b"), -starts_with("t", ignore.case = F), -dag, -start, -Toon) %>% 
  select(slot, hh_formule, everything()) %>% 
  pivot_longer(names_to = "wanneer", cols = starts_with("week_"), values_to = "titel") 

krimp <- tbl_zenderschema.2 %>% filter(!is.na(titel)) %>% 
  group_by(slot, titel) %>% mutate(ronu = row_number()) %>% ungroup() %>% filter(ronu == 5) %>% select(titel)

tbl_zenderschema <- tbl_zenderschema.2 %>% 
  mutate(titel = if_else(!is.na(titel), titel, if_else(!is.na(wekelijks), wekelijks, AB_cyclus)),
         wanneer = if_else(hh_formule == "tw" | !is.na(wekelijks) | titel %in% krimp$titel, NA_character_, wanneer),
         hh_formule = if_else(hh_formule == "tw", NA_character_, hh_formule)) %>% 
  select(-wekelijks, -contains("cyclus")) %>% 
  distinct() 


# Verzendlijst Luistercijfers - de lijst ----------------------------------

tbl_stats_vzl_lst <- tbl_raw_stats_vzl_lst %>% 
  mutate(deelnemer_actief = if_else(deelnemer_actief == "j", T, F),
         live_stream_tonen = if_else(live_stream_tonen == "j", T, F))


# Verzendlijst Luistercijfers - mailadressen ------------------------------

tbl_stats_vzl_mad <- tbl_raw_stats_vzl_mad 


# Verzendlijst Luistercijfers - mailadressen ------------------------------

tbl_stats_vzl_tpt <- tbl_raw_stats_vzl_tpt


# Opruimen ----------------------------------------------------------------

rm(
  tbl_raw_zenderschema,
  tbl_raw_stats_vzl_lst,
  tbl_raw_stats_vzl_mad,
  tbl_raw_stats_vzl_tpt,
  tbl_zenderschema.1,
  tbl_zenderschema.2
)
