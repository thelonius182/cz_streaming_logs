fa <- flog.appender(appender.file("/home/lon/Documents/cz_stats_proc.log"), "cz_stats_proc_log")

cz_stats_cfg <- read_yaml("config.yaml")

source("src/prep_funcs.R", encoding = "UTF-8")

cz_stats_rod.10 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_rod.10.RDS")) # from prep_rod4.R
cz_stats_cha_08 <- read_rds(file = paste0(stats_data_flr(), "cz_stats_cha_08.RDS")) # from prep8.R

# from ws_geodata.R
cz_ipa_geo_full <- read_rds(file = paste0(cz_stats_cfg$ws_geodata_home, "cz_ipa_geo_full.RDS")) %>% 
  group_by(ip) %>% mutate(ip_row = row_number()) %>% ungroup() %>% filter(ip_row == 1) %>% select(-ip_row)

cz_stats_joined_01 <- cz_stats_cha_08 %>% 
  bind_rows(cz_stats_rod.10) %>% 
  select(-cz_row_id, -cz_id, -cz_cha_id) 

cz_stats_joined_02 <- cz_stats_joined_01 %>% 
  left_join(cz_ipa_geo_full, by = c("cz_ipa" = "ip")) %>% 
  select(-region_code, -region_name, -zip_code, -city, -latitude, -longitude, -metro_code) 

cz_tzones <- cz_stats_joined_02 %>% 
  select(time_zone) %>% 
  distinct() %>% 
  filter(!is.na(time_zone)) %>% 
  mutate(rrn = row_number(),
         NL_time = ymd_hms("2021-08-01 12:00:00", tz = "Europe/Amsterdam")) 
  
cz_tzones_diffs.1 <- NULL

for (t1 in 1:nrow(cz_tzones)) {
  # t1 <- 1
  cz_tz_tmp <- cz_tzones %>% 
    filter(rrn == t1) %>% 
    mutate(cz_local_ts = force_tz(NL_time, tzone = time_zone),
           cz_adj = as.period(NL_time - cz_local_ts, unit = "hour"),
           NL_time_adjusted = NL_time + cz_adj)
  
  if (is.null(cz_tzones_diffs.1)) {
    cz_tzones_diffs.1 <- cz_tz_tmp
  } else {
    cz_tzones_diffs.1 %<>% add_row(cz_tz_tmp)
  }
  
}

cz_tzones_diffs.2 <- cz_tzones_diffs.1 %>% 
  select(time_zone, cz_adj) 
  
cz_stats_joined_03 <- cz_stats_joined_02 %>% 
  left_join(cz_tzones_diffs.2) %>% 
  mutate(cz_ts_local = if_else(is.na(cz_adj), cz_ts, cz_ts + cz_adj)) %>% 
  select(-time_zone, -cz_adj) %>% 
  filter(cz_ts_local >= cz_reporting_start & cz_ts_local <= cz_reporting_stop)

# program titles ----
# LET OP - doe dit alleen als er VEEL titels gewijzigd of bijgekomen zijn.
#          Alleen dan de onderstaande code ontsterren en uitvoeren
#          en de lijst opnieuw corrigeren. 
#          Is er maar een handvol wijzigingen, dan pgm_title_cleaner.tsv direct wijzigen.

# salsa_stats_all_pgms_raw <-
#   read_delim(
#     "/mnt/muw/cz_stats_wpdata/salsa_stats_all_pgms.txt",
#     delim = "\t",
#     escape_double = FALSE,
#     col_types = cols(pgmLang = col_skip()),
#     trim_ws = TRUE
#   ) %>% filter(pgmTitle != "NULL") %>% 
# 
# pgm_titles_unique <- salsa_stats_all_pgms_raw %>%
#   select(pgmTitle) %>%
#   distinct() %>%
#   mutate(pgmTtle_clean = str_to_sentence(str_replace_all(pgmTitle, "[[:punct:]]", ""))) %>%
#   arrange(pgmTitle)
# 
# write_delim(pgm_titles_unique, file = "/mnt/muw/cz_stats_wpdata/pgm_title_cleaner_20220402.tsv", delim = "\t")

# In ieder geval wel deze doen!
# vertaalsleutel programmatitels: "pgm_title_fixed" in de tsv == "titel_stats" in de GD-verzendlijst
cz_pgm_titles_fixed.1 <- read_delim("/mnt/muw/cz_stats_wpdata/pgm_title_cleaner.tsv",
  delim = "\t",
  escape_double = FALSE,
  col_names = TRUE,
  trim_ws = TRUE,
  quote = ""
)

names(cz_pgm_titles_fixed.1) <- c("pgm_title", "pgm_title_fixed")

cz_stats_joined_04 <- cz_stats_joined_03 %>% 
  left_join(cz_pgm_titles_fixed.1, by = c("pgm_title" = "pgm_title"))

cz_stats_joined_04 <- cz_stats_joined_04 |> 
  mutate(pgm_title_fixed = 
           case_when(pgm_title == "Lâ\\u0080\\u0099Esprit Baroque" ~ "Esprit Baroque", 
                     pgm_title == "Meester â\\u0080\\u0093 Gezel" ~ "Meester en Gezel",
                     pgm_title == "Oriënt Expres" ~ "Oriënt Express",
                     pgm_title == "OriÃ«nt Expres" ~ "Oriënt Express",
                     pgm_title == "OriÃ«nt Express" ~ "Oriënt Express",
                     pgm_title == "Brazil In A Nutshell" ~ "Brazil in a Nutshell",
                     pgm_title == "Brazil In a Nutshell" ~ "Brazil in a Nutshell",
                     pgm_title == "Componist van de maand" ~ "Componist van de Maand",
                     pgm_title == "Componist vd maand" ~ "Componist van de Maand",
                     pgm_title == "De Muzikant / Concertzender Live" ~ "De Muzikant",
                     pgm_title == "De Muzikant Concertzender Live" ~ "De Muzikant",
                     pgm_title == "Componist vd Maand" ~ "Componist van de Maand",
                     pgm_title == "Aktueel" ~ "Actueel",
                     pgm_title == "De piano etude" ~ "De piano-etude",
                     pgm_title == "De piano etude: Van oefening tot kunst" ~ "De piano-etude",
                     pgm_title == "De piano-etude, van oefening tot kunst" ~ "De piano-etude",
                     pgm_title == "Duitse barok vÃ³Ã³r Bach" ~ "Duitse barok vóór Bach",
                     pgm_title == "De wandeling" ~ "De Wandeling",
                     pgm_title == "De Ochtendwandeling" ~ "De Wandeling",
                     pgm_title == "Een late wandeling" ~ "De Wandeling",
                     pgm_title == "Grand CafÃ© Europa" ~ "Grand Café Europa",
                     pgm_title == "Klassiek TsjechiÃ«" ~ "Klassiek Tsjechië",
                     pgm_title == "Meester â Gezel" ~ "Meester - Gezel",
                     pgm_title == "Tango FusiÃ³n" ~ "Tango Fusión",
                     pgm_title == "Tango FusiÃ³n" ~ "Tango Fusión",
                     str_detect(pgm_title, ".Mambo.") ~ "Mambo",
                     str_detect(pgm_title, ".*Esprit Baroque") ~ "Esprit Baroque",
                     str_detect(pgm_title, "Een [Vv]roege [Ww]andeling") ~ "De Wandeling",
                     str_detect(pgm_title, "Concertzender (Live Kerst|[Ll]ive!?|Live Napels|)") ~ "Concertzender Live",
                     str_detect(pgm_title, "Vredenburg live!?") ~ "Vredenburg live",
                     str_detect(pgm_title, "Disc-[Cc]over!?") ~ "Disc-cover",
                     str_detect(pgm_title, "Donder, bliksem en manen?schijn") ~ "Donder, bliksem en maneschijn",
                     str_detect(pgm_title, "Door de [Mm]azen van het [Nn]et") ~ "Door de Mazen van het Net",
                     str_detect(pgm_title, "Folk [Ii]t!?") ~ "Folk it",
                     str_detect(pgm_title, "Framework.*") ~ "Framework",
                     str_detect(pgm_title, "Front Runn.*") ~ "Front Running",
                     str_detect(pgm_title, "Geen dag zonder Bach.*") ~ "Geen dag zonder Bach",
                     str_detect(pgm_title, "Groove &(amp;)? Grease") ~ "Groove en Grease",
                     str_detect(pgm_title, "Inventions [Ff]or Radio.*") ~ "Inventions For Radio",
                     str_detect(pgm_title, "JazzNotJazz.*") ~ "JazzNotJazz",
                     str_detect(pgm_title, "Kroniek van de .*[Mm]uziek") ~ "Kroniek van de Nederlandse muziek",
                     str_detect(pgm_title, "Moanin. the Blues") ~ "Moaning the Blues",
                     str_detect(pgm_title, "New Folk [Ss]ounds [Oo]n [Aa]ir") ~ "New Folk Sounds on Air",
                     str_detect(pgm_title, "Ongehoorde [Ss]cores") ~ "Ongehoorde Scores",
                     str_detect(pgm_title, ".*?Krizz Krazz.?") ~ "Krizz Krazz",
                     str_detect(pgm_title, "Popart.*") ~ "Popart",
                     str_detect(pgm_title, "Rheinbergers kamermuziek.*") ~ "Rheinbergers kamermuziek",
                     str_detect(pgm_title, "Sound of [Mm]ovies") ~ "Sound of Movies",
                     str_detect(pgm_title, "Tango: [Ll]ied van Buenos Aires") ~ "Tango: Lied van Buenos Aires",
                     str_detect(pgm_title, "Zwerven door .*") ~ "Zwerven door de Klassieke Periode",
                     str_detect(pgm_title, "Zuiver [Kk]lassiek") ~ "Zuiver Klassiek",
                     T ~ pgm_title))

# signal missing titles
cz_stats_joined_04_missing <- cz_stats_joined_04 %>% filter(is.na(pgm_title_fixed))

if (nrow(cz_stats_joined_04_missing) > 0) {
  
  # disregard if missing hours is low
  hrs_missing <- sum(cz_stats_joined_04_missing$cz_length) / 3600
  
  if (hrs_missing < 100) {
    cz_stats_joined_04 %<>% filter(!is.na(pgm_title_fixed))
    
    # clear this so main.R can continue
    cz_stats_joined_04_missing %<>% filter(!is.na(pgm_title_fixed))
  }
}

write_rds(x = cz_stats_joined_04,
          file = paste0(stats_data_flr(), "cz_stats_joined_04.RDS"),
          compress = "gz")
