# Voorbereiden van de maandelijkse luistercijfers
# - Dit script draait op de nipper-pc, niet de VM!

pacman::p_load(DBI, googledrive, dplyr, stringr, stringi, lubridate, readr, fs, futile.logger)

# init: connect to database
grh_con <- dbConnect(odbc::odbc(), "wpprd_mariadb", timeout = 10, encoding = "CP850")

# Authenticate with Google Drive
drive_auth(email = "cz.teamservice@gmail.com")

# 1. Data CZ-gids ophalen ----
#    query = C:\Users\nipper\Documents\cz_queries\salsa_stats_cz_gids_w_editor.sql
#    opslaan (overwrite) als tab-separated text in salsa_stats_all_pgms.txt
qry_stats01 <- "drop temporary table if exists salsa_stats_aux_replay_chr;"
dbExecute(grh_con, qry_stats01)

qry_stats02 <- "
create temporary table salsa_stats_aux_replay_chr as
    SELECT
        meta_id, 
        post_id wppm_pid, 
        meta_value as wppm_orgpid_chr
	from wp_postmeta 
where meta_key = 'pr_metadata_orig'
  and meta_value is not null
  and char_length(trim(meta_value)) > 0
;
"
dbExecute(grh_con, qry_stats02)

qry_stats03 <- "drop temporary table if exists salsa_stats_aux_replay;"
dbExecute(grh_con, qry_stats03)

qry_stats04 <- "create temporary table salsa_stats_aux_replay as
    SELECT
        meta_id, 
        wppm_pid, 
        cast(wppm_orgpid_chr as unsigned) wppm_orgpid
from salsa_stats_aux_replay_chr 
;"
dbExecute(grh_con, qry_stats04)

qry_stats05 <- "ALTER TABLE salsa_stats_aux_replay ADD INDEX (wppm_orgpid);"
dbExecute(grh_con, qry_stats05)

qry_stats06 <- "drop temporary table if exists salsa_stats_aux_replay_title;"
dbExecute(grh_con, qry_stats06)

qry_stats07 <- "
CREATE temporary TABLE salsa_stats_aux_replay_title AS 
    SELECT 
        meta_id, 
        wppm_pid, 
        wppm_orgpid, 
        po1.post_title 
	FROM salsa_stats_aux_replay sar1
        JOIN
    wp_posts po1 ON sar1.wppm_orgpid = po1.id
        JOIN
    wp_term_relationships tr1 ON tr1.object_id = po1.id
WHERE
    tr1.term_taxonomy_id = 5
;"
dbExecute(grh_con, qry_stats07)

qry_stats08 <- "ALTER TABLE salsa_stats_aux_replay_title ADD INDEX (wppm_pid);"
dbExecute(grh_con, qry_stats08)

qry_stats09 <- "drop temporary table if exists salsa_stats_aux_editor;"
dbExecute(grh_con, qry_stats09)

qry_stats10 <- "
CREATE temporary TABLE salsa_stats_aux_editor AS 
select po1.id as pgmID,
       pm1.meta_value as post_editor
from wp_posts po1 
        JOIN
    wp_term_relationships tr1 ON tr1.object_id = po1.id
       left join 
    wp_postmeta pm1 on pm1.post_id = po1.id
where pm1.meta_key = 'pr_metadata_production1_person'
  and post_title is not null 
  and length(trim(post_title)) > 0
  and tr1.term_taxonomy_id = 5
union
select ar1.wppm_pid as pgmID,
       pm1.meta_value as post_editor
from salsa_stats_aux_replay_title ar1 
        JOIN
    wp_term_relationships tr1 ON tr1.object_id = ar1.wppm_orgpid
       left join 
    wp_postmeta pm1 on pm1.post_id = ar1.wppm_orgpid
where pm1.meta_key = 'pr_metadata_production1_person'
  and post_title is not null 
  and length(trim(post_title)) > 0
  and tr1.term_taxonomy_id = 5
order by 1  
;"
dbExecute(grh_con, qry_stats10)

qry_stats11 <- "ALTER TABLE salsa_stats_aux_editor ADD INDEX (pgmID);"
dbExecute(grh_con, qry_stats11)

qry_stats12 <- "drop table if exists salsa_stats_all_pgms;"
dbExecute(grh_con, qry_stats12)

qry_stats13 <- "
SELECT 
    DATE_FORMAT(po1.post_date, '%Y%m%d_%H') AS pgmStart,
    DATE_FORMAT(pm3.meta_value, '%Y%m%d_%H') AS pgmStop,
    case when po1.post_title is not null 
		      and char_length(trim(po1.post_title)) > 0 
	       then po1.post_title
         else rt1.post_title
	  end as pgmTitle,
    cast(tr1.term_taxonomy_id as decimal(1, 0)) as pgmLang,
    ed1.post_editor
FROM
    wp_posts po1
        JOIN
    wp_postmeta pm3 ON pm3.post_id = po1.id
        JOIN
    wp_term_relationships tr1 ON tr1.object_id = po1.id   
        left join
	salsa_stats_aux_replay_title rt1 on rt1.wppm_pid = po1.id
        left join
	salsa_stats_aux_editor ed1 on ed1.pgmID = po1.id
WHERE
        po1.post_type = 'programma'
    AND po1.post_status = 'publish'
    AND pm3.meta_key = 'pr_metadata_uitzenddatum_end'
    AND tr1.term_taxonomy_id = 5    
ORDER BY pgmStart
;
"
salsa_stats_all_pgms <- dbGetQuery(grh_con, qry_stats13)

salsa_stats_all_pgms_df <- salsa_stats_all_pgms %>% as_tibble() %>% 
  mutate(pgmTitle = if_else(pgmTitle %in% c("L?Esprit Baroque", "Front Runnin?", "Moanin? the Blues"), 
                            str_replace(pgmTitle, "\\?", "'"),
                            pgmTitle))

# # Create a tab-separated file
file_path <- "C:/cz_salsa/cz_exchange/salsa_stats_all_pgms.txt"
write.table(
  salsa_stats_all_pgms_df,
  file = file_path,
  sep = "\t",
  quote = FALSE,
  row.names = FALSE,
  fileEncoding = "UTF-8"
)

# Zip it
zip_path <- str_replace(file_path, "\\.txt", ".zip")
zip(zip_path, files = file_path)

# . Upload to Google Drive ----
drive_upload(zip_path, type = "application/zip")

# drive_trash(path_file(zip_path))

# 2. Playlists Themakanalen ophalen ----
# query = C:\Users\nipper\Documents\cz_queries\themakanalen_2.sql
# opslaan (overwrite) als tab-separated text in themakanalen_listed.txt
qry_stats20 <- "
SELECT distinct
   p1.channel, 
   t1.name, 
   case when p1.pid = t1.current_program then '#' else ' ' end as cupro,
   p1.url,
   p1.start,
   p1.stop,
   (select po1.post_title
	from wp_posts po1
    where po1.id = p1.pid) as pgm_title,
    pid as pgm_id,
   (select DATE_FORMAT(po1.post_date, '%Y-%m-%d %H:%i') as start_pgm
	from wp_posts po1
    where po1.id = p1.pid) as pgm_start,
   (select pm1.meta_value
    from wp_posts po1 
       left join wp_postmeta pm1 on pm1.post_id = po1.id
	where po1.id = p1.pid
      and pm1.meta_key = 'pr_metadata_uitzenddatum_end') as pgm_stop
FROM cz.wp_create_playlists p1
     join wp_themakanalen t1 on t1.id = p1.channel
order by p1.channel, p1.internal_id
;"

themakanalen_listed <- dbGetQuery(grh_con, qry_stats20)

themakanalen_listed_df <- themakanalen_listed %>% as_tibble() %>% 
  mutate(pgm_title = if_else(pgm_title %in% c("L?Esprit Baroque", "Front Runnin?", "Moanin? the Blues"), 
                            str_replace(pgm_title, "\\?", "'"),
                            pgm_title))

# # Create a tab-separated file
file_path <- "C:/cz_salsa/cz_exchange/themakanalen_listed.txt"
write.table(themakanalen_listed_df, file = file_path, sep = "\t", quote = FALSE, row.names = FALSE)

# Zip it
zip_path <- str_replace(file_path, "\\.txt", ".zip")
zip(zip_path, files = file_path)

# . Upload to Google Drive ----
drive_upload(zip_path, type = "application/zip")

# 3. Huidig pgm per Themakanaal ophalen ----
# (deze rapp.periode)
# query = C:\Users\nipper\Documents\cz_queries\themakanalen.sql
# opslaan (overwrite) als tab-separated text in themakanalen_current_pgms.txt
qry_stats30 <- "
SELECT distinct
   p1.channel, 
   t1.name, 
   t1.current_program,
   p1.url,
   p1.start,
   p1.stop,
   (select po1.post_title
	    from wp_posts po1
      where po1.id = p1.pid) as pgm_title,
   (select DATE_FORMAT(po1.post_date, '%Y-%m-%d %H:%i') as start_pgm
	    from wp_posts po1
      where po1.id = p1.pid) as pgm_start,
   (select pm1.meta_value
      from wp_posts po1 left join wp_postmeta pm1 
                               on pm1.post_id = po1.id
	    where po1.id = p1.pid
        and pm1.meta_key = 'pr_metadata_uitzenddatum_end') as pgm_stop,
	 now() as ts_snapshot
FROM cz.wp_create_playlists p1
     join wp_themakanalen t1 on t1.id = p1.channel
where p1.pid = t1.current_program
order by p1.channel, p1.internal_id
;"

themakanalen_current_pgms <- dbGetQuery(grh_con, qry_stats30)

# Convert the list to a data frame
themakanalen_current_pgms_df <- themakanalen_current_pgms %>% as_tibble() %>% 
  mutate(pgm_title = if_else(pgm_title %in% c("L?Esprit Baroque", "Front Runnin?", "Moanin? the Blues"), 
                             str_replace(pgm_title, "\\?", "'"),
                             pgm_title))


# # Create a tab-separated file
file_path <- "C:/cz_salsa/cz_exchange/themakanalen_current_pgms.txt"
write.table(themakanalen_current_pgms_df, file = file_path, sep = "\t", quote = FALSE, row.names = FALSE)

# Zip it
zip_path <- str_replace(file_path, "\\.txt", ".zip")
zip(zip_path, files = file_path)

# . Upload to Google Drive ----
drive_upload(zip_path, type = "application/zip")

dbDisconnect(grh_con)
