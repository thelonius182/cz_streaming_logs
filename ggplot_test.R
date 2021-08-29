library(tidyverse)
library(readr)

pres_rating <- data.frame(
  rating = as.numeric(presidents),
  year = as.numeric(floor(time(presidents))),
  quarter = as.numeric(cycle(presidents))
)

wiki_vs_pres <-
  read_delim(
    "~/Documents/Presidenten van de Verenigde Staten (1789–heden).txt",
    delim = "¶",
    escape_double = FALSE,
    col_names = FALSE,
    trim_ws = TRUE
  )

wiki_vs_pres_1 <- wiki_vs_pres %>%
  filter(!str_detect(X1, "bar")) %>%
  mutate(pres_name = if_else(str_detect(X1, "Lifespan"),
                             sub("[^\\[]*\\[\\[([^]|()]*)\\|?.*", "\\1", X1, perl=TRUE),
                             NA_character_),
         pres_from = if_else(str_detect(X1, "Lifespan"),
                             NA_character_,
                             sub(".*(?<!Lifespan\t)\\S*from:(\\d{4}\\.\\d).*", "\\1", X1, perl=TRUE)),
         pres_till = if_else(str_detect(X1, "Lifespan"),
                             NA_character_,
                             sub(".*(?<!Lifespan\t)\\S*till:(\\d{4}\\.\\d|\\$now).*", "\\1", X1, perl=TRUE)),
         pres_till = if_else(pres_till == "$now", "2021.8", pres_till)
  ) %>% 
  fill(pres_name, .direction = "down") %>% 
  filter(!is.na(pres_from)) %>% 
  select(-X1)

rm(wiki_vs_pres)

wiki_vs_pres_2 <- wiki_vs_pres_1 %>% 
  mutate(pres_from_year = as.integer(str_sub(pres_from, 1, 4)),
         pres_from_mnth = 1 + as.integer(str_sub(pres_from, 6)),
         pres_qtr = case_when(pres_from_mnth > 9 ~ 4L,
                              pres_from_mnth > 6 ~ 3L,
                              pres_from_mnth > 3 ~ 2L,
                              TRUE ~ 1L)
  ) %>% 
  select(-pres_from, -pres_till, -pres_from_mnth) %>% 
  filter(pres_from_year >= 1945L & pres_from_year <= 1974L)

rm(wiki_vs_pres_1)

wiki_vs_pres_2.1 <- pres_rating %>%
  left_join(wiki_vs_pres_2,
            by = c("year" = "pres_from_year", "quarter" = "pres_qtr")) %>%
  fill(pres_name, .direction = "downup")

distinct_pres_names <- wiki_vs_pres_2.1 %>% select(pres_name) %>% distinct() %>% unlist()

wiki_vs_pres_3 <- wiki_vs_pres_2.1 %>%
  mutate(pres_name_order = factor(pres_name, levels = distinct_pres_names))

rm(pres_rating, wiki_vs_pres_2)

p <- ggplot(data = wiki_vs_pres_3, aes(x = year, y = quarter, fill = rating))

p +
  geom_tile() +
  scale_x_continuous(breaks = seq(1940, 1976, by = 4), expand = c(0, 0)) +
  scale_y_reverse(expand = c(0, 0)) +
  scale_fill_gradient2(midpoint = 50, mid = "grey70", limits = c(0, 100)) + 
  facet_grid(rows = vars(pres_name_order))
