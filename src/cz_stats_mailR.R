suppressPackageStartupMessages(library(gmailr))
suppressPackageStartupMessages(library(tidyverse))
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(viridis))
suppressPackageStartupMessages(library(magrittr))
suppressPackageStartupMessages(library(yaml))
suppressPackageStartupMessages(library(fs))

cz_stats_cfg <- read_yaml("config.yaml")

# load functions ----
source("src/prep_funcs.R", encoding = "UTF-8")

# prep title list ----
cz_stats_verzendlijst.tpt <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - themakanalen-per-titel.tsv",
                                      delim = "\t", escape_double = FALSE,
                                      trim_ws = TRUE,
                                      show_col_types = FALSE) 
 
# tpt_dft.1 <- cz_stats_verzendlijst.tpt %>% 
#   select(titel_gids) %>% distinct() %>% 
#   mutate(themakanaal = "Live")
# 
# tpt_dft.2 <- cz_stats_verzendlijst.tpt %>% 
#   select(titel_gids) %>% distinct() %>%  
#   mutate(themakanaal = "Radio on Demand")
#   
# cz_stats_verzendlijst.tpt.1 <-
#   cz_stats_verzendlijst.tpt %>% bind_rows(tpt_dft.1) %>% bind_rows(tpt_dft.2) %>% arrange(titel_gids, themakanaal)
  
cz_stats_verzendlijst.tpt.list <- cz_stats_verzendlijst.tpt %>% 
  select(-greenhost_stream) %>% 
  group_by(titel_gids) %>% 
  mutate(grp = row_number())

cz_stats_verzendlijst.mad <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - mailadressen.tsv",
                                      delim = "\t", escape_double = FALSE,
                                      trim_ws = TRUE,
                                      show_col_types = FALSE) 

cz_stats_verzendlijst.vzl <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - verzendlijst.tsv",
                                      delim = "\t", escape_double = FALSE,
                                      trim_ws = TRUE,
                                      show_col_types = FALSE) %>% 
  filter(deelnemer_actief == "j") %>% 
  select(-deelnemer_actief, -live_stream_tonen) %>% 
  left_join(cz_stats_verzendlijst.mad)

# get start from config file----
fmt_cz_month <- stamp("202112", orders = "%y%0m", quiet = T)
cz_reporting_day_one_chr <- cz_stats_cfg$`current-month`
cz_reporting_day_one <- ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")
cz_rep_month_start <- paste0(fmt_cz_month(ymd_hms(cz_reporting_day_one_chr, tz = "Europe/Amsterdam")), "00")
cz_rep_month_stop <- paste0(fmt_cz_month(ceiling_date(cz_reporting_day_one + ddays(1), unit = "months")), "00")

salsa_stats_all_pgms_w_editor <-
  read_delim(
    "~/Downloads/salsa_stats_all_pgms.txt",
    delim = "\t",
    escape_double = FALSE,
    lazy = F,
    quote = "",
    trim_ws = TRUE,
    show_col_types = FALSE
  )

cur_pgms_w_editor <- salsa_stats_all_pgms_w_editor %>% 
  filter(pgmStart > cz_rep_month_start &
           pgmStart < cz_rep_month_stop & 
           post_editor != "CZ" & 
           !is.na(post_editor)) %>% 
  select(pgmTitle, post_editor) %>% distinct() %>% arrange(pgmTitle, post_editor) %>% 
  mutate(pgmTitle = case_when(pgmTitle == "Bach &amp; Co" ~ "Bach en Co",
                              pgmTitle == "Groove &amp; Grease" ~ "Groove and Grease",
                              pgmTitle == "JazzNotJazz Music &amp; Politics" ~ "JazzNotJazz",
                              pgmTitle == "Framework 1" ~ "Framework",
                              pgmTitle == "Framework 2" ~ "Framework",
                              pgmTitle == "Missa etcetera" ~ "Missa Etcetera",
                              pgmTitle == "Dansen en de blues" ~ "Dansen en de Blues",
                              TRUE ~ pgmTitle),
         post_editor = case_when(post_editor == "Framework" ~ "Hessel Veldman",
                                 TRUE ~ post_editor))

# init mailinglist cur month ----
cur_pgms_vzl <- cur_pgms_w_editor %>% 
  left_join(cz_stats_verzendlijst.vzl, by = c("pgmTitle" = "titel_gids")) %>% 
  mutate(matching_editor = str_detect(post_editor, wie)) %>% 
  filter(matching_editor) %>% 
  select(-post_editor, -matching_editor) %>% distinct() %>% 
  group_by(titel_stats, wie) %>% mutate(tsw = row_number()) %>% 
  ungroup() %>% filter(tsw == 1) %>% select(-tsw)

# get pgm stats cur month ----
cur_pgms_stats <- read_rds(file = paste0(stats_data_flr(), "cz_licharod_stats_pgm_report.2.RDS")) %>% 
  filter(!is.na(hours_tot) & pgm_title %in% cur_pgms_vzl$titel_stats)

# complete mailinglist cur month ----
cz_stats_emails <- cur_pgms_stats %>% select(pgm_title) %>% distinct() %>% 
  inner_join(cur_pgms_vzl, by = c("pgm_title" = "titel_stats")) %>% 
  select(email, aanhef, pgmTitle) %>% 
  mutate(cz_stats_pgm_png = paste0(stats_data_flr(), "diagrams/", pgmTitle, ".png"),
         cz_stats_streams_png = paste0(stats_data_flr(), "diagrams/Streams waar ", pgmTitle, " in opgenomen is.png")) %>% 
  select(-pgmTitle) %>% 
  arrange(email, aanhef)

send_loop_mail_to <- cz_stats_emails %>% select(email) %>% distinct() 

# cz_stats_mailaddress_salutation <- cz_stats_emails %>% select(email, aanhef) %>% distinct()
# cz_stats_mailattachment <- cz_stats_emails %>% select(email, cz_stats_diagram)

# connect to gmail ----
gm_auth_configure(path = "cz-studiomails.json")
gm_auth(email = "cz.teamservice@gmail.com")

cz_stats_msg_body_template <- "
@aanhef

Wat zal het worden? Een opsteker of een afknapper? 
De kop in het zand of het hoofd in de wolken? 
Ga je door de grond of door het dak? 

Hier zijn de eerste cijfers.

Met groet van CZ-teamservice,
Lon
"

# send_loop_mail_to <- read_rds("rerun_emails.RDS")

# create emails ----
for (cur_mail_to in send_loop_mail_to$email) {
  
  email_details <- cz_stats_emails %>% filter(email == cur_mail_to)
  
  cur_salutations <- email_details %>% select(aanhef) %>% distinct()
  
  for (cur_aanhef in cur_salutations$aanhef) {
    cur_body <- cz_stats_msg_body_template %>% str_replace("@aanhef", cur_aanhef)
    att_set <- email_details %>% filter(aanhef == cur_aanhef) %>% select(cz_stats_pgm_png, cz_stats_streams_png)
  
    print(cur_mail_to)
    
    cz_stats_msg <- gm_mime() %>%
      # gm_to("vandenakker.info@xs4all.nl") %>%
      gm_to(cur_mail_to) %>%
      gm_from("cz.teamservice@concertzender.nl") %>%
      gm_subject(paste0("CZ-luistercijfers, ", stats_data_flr() %>% str_extract(pattern = "\\d{4}-\\d{2}"))) %>%
      gm_text_body(cur_body)
      # gm_text_body(cur_body %>% str_replace("@cz_adres", cur_mail_to))
  
    for (cur_png in att_set$cz_stats_pgm_png) {
      cz_stats_msg <- cz_stats_msg %>% gm_attach_file(cur_png)
    }
  
    for (cur_png in att_set$cz_stats_streams_png) {
      cz_stats_msg <- cz_stats_msg %>% gm_attach_file(cur_png)
    }
    
    cz_stats_msg <- cz_stats_msg %>% gm_attach_file("/home/lon/Documents/cz_stats_data/2022-04/diagrams/CZ-luistercijfers, alle kanalen.png")
    
    gm_send_message(cz_stats_msg)
  }
}

# 
# test_email <-
#   gm_mime() %>%
#   gm_to("vandenakker.info@xs4all.nl") %>%
#   gm_from("cz.teamservice@gmail.com") %>%
#   gm_subject("this is just a gmailr test") %>%
#   gm_text_body("Can you hear me now?")
# 
# # Verify it looks correct
# gm_create_draft(test_email)
# 
# # If all is good with your draft, then you can send it
# gm_send_message(test_email)
# # You can add a file attachment to your message with gm_attach_file().
# 
# write.csv(mtcars,"mtcars.csv")
# test_email <- test_email %>% gm_attach_file("/home/lon/Documents/cz_stats_data/2022-04/diagrams/Nuove musiche.png")
# 
# # Verify it looks correct
# gm_create_draft(test_email)
# 
# # If so, send it
# gm_send_message(test_email)
