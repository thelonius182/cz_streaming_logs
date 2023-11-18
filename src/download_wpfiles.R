drive_auth(email = "cz.teamservice@gmail.com")

restore_wpfile <- function(arg_wpfile) {
  wpfiles_home <- "/mnt/muw/cz_stats_wpdata/"
  wpfiles_unzipped_home <- paste0(wpfiles_home, "cz_salsa/cz_exchange/")
  wpfile_unzipped <- str_replace(arg_wpfile, "\\.zip", ".txt")
  wppath <- paste0(wpfiles_home, arg_wpfile)
  gd_result <- drive_find(type = "application/zip", pattern = arg_wpfile)
  
  if (nrow(gd_result) > 0) {
    drive_download(file = arg_wpfile,
                   path = wppath,
                   overwrite = T)
    unzip(wppath, exdir = wpfiles_home)
    file_delete(wppath)
    file_move(
      path = paste0(wpfiles_unzipped_home, wpfile_unzipped),
      new_path = paste0(wpfiles_home, wpfile_unzipped)
    )
    dir_delete("/mnt/muw/cz_stats_wpdata/cz_salsa")
    drive_trash(arg_wpfile)
  }
}

wpfiles <- c("themakanalen_current_pgms.zip", "themakanalen_listed.zip", "salsa_stats_all_pgms.zip") %>% as_tibble()

for (cur_wpfile in wpfiles$value) {
  restore_wpfile(cur_wpfile)
}
