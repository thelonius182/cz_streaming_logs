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

# load verzendlijst ----
source("src/get_google_czdata_stats.R", encoding = "UTF-8")

# cz_stats_verzendlijst.mad <- read_delim("~/Downloads/Luistercijfers verzendlijst 2.0 - mailadressen.tsv",
#                                         delim = "\t", escape_double = FALSE,
#                                         trim_ws = TRUE,
#                                         show_col_types = FALSE) 

# prep title list ----
cz_stats_mailaddress_salutation <- tbl_stats_vzl_lst %>% 
  filter(deelnemer_actief) %>% 
  inner_join(tbl_stats_vzl_mad) %>% 
  select(email, aanhef) %>% distinct() 

# connect to gmail ----
gm_auth_configure(path = "cz-studiomails.json")
gm_auth(email = "cz.teamservice@gmail.com")

cz_stats_msg_body_template <- "
@aanhef

Ionica Smeets, bekend van de wetenschapsquiz en haar columns in de volkskrant over 
alledaagse wiskunde - die konden we er net niet voor strikken. Zó jammer ... maar 
met onze luistercijfers is het toch nog goed gekomen. Van de miljoenen logrecords 
die onze server maand in maand uit produceert, hebben we een paar overzichten weten 
te maken die je laten zien wat onze luisteraars allemaal uitspoken op concertzender.nl. 
Vanaf komende week sturen we je elke maand een stuk of wat grafieken die over jouw programma('s) gaan.

In de bijlage vind je onder de kop Het Exacte Verhaal alvast een voorproefje met toelichting. 
En heb je last van cijferallergie, of kunnen dit soort inzichten je vierkant gestolen worden, 
meld je dan gerust af! Dat kan op bovenstaand mailadres.

Hartelijke groet, namens CZ-teamservice,
Lon

PS - dit project begon in april '21, en had een jaar later de eerste bruikbare cijfers. 
We beginnen daarom straks met 2022-04. Daarna elke week een volgende maand tot we in 
de pas lopen met de kalender. Vanaf dan gewoon per maand.

"

# create emails ----
for (cur_mailaddress in cz_stats_mailaddress_salutation$email) {
  
  cur_salutation <- cz_stats_mailaddress_salutation %>% filter(email == cur_mailaddress) %>% select(aanhef)
  
  cur_bodies <- cz_stats_msg_body_template %>% str_replace("@aanhef", cur_salutation$aanhef)
  
  for (cur_body in cur_bodies) {
    
    cz_stats_msg <- gm_mime() %>%
      # gm_to("vandenakker.info@xs4all.nl") %>%
      gm_to(cur_mailaddress) %>%
      gm_from("cz.teamservice@concertzender.nl") %>%
      gm_subject("Binnenkort in dit theater - Het Exacte Verhaal") %>%
      gm_text_body(cur_body)
    
    # cz_stats_msg <- cz_stats_msg %>% gm_attach_file("/home/lon/Downloads/Het exacte verhaal.pdf")
    
    # gm_create_draft(cz_stats_msg)
    gm_send_message(cz_stats_msg)
  }
}
