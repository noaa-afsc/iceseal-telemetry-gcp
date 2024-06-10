library(DBI)
library(dbplyr)
library(dplyr)
library(RPostgres)
library(sf)
library(arrow)
library(geoarrow)

tryCatch({
  con <- dbConnect(
    RPostgres::Postgres(),
    dbname = 'pep',
    host = Sys.getenv('PEP_PG_IP'),
    user = keyringr::get_kc_account("pgpep_londonj"),
    password = keyringr::decrypt_kc_pw("pgpep_londonj")
  )
}, error = function(cond) {
  print("Unable to connect to Database.")
})

tbl_deploy <- dplyr::tbl(con,in_schema("telem","tbl_tag_deployments")) |> 
  dplyr::collect()

locs_obs <- sf::st_read(con,Id("telem","geo_wc_locs_qa"))  |> 
  dplyr::left_join(tbl_deploy) |> 
  dplyr::filter(meta_project %in% c('Ice Seals'),
                species %in% c('Hf','Pl')) |> 
  dplyr::select(speno,deployid,ptt,instr,tag_family,type,quality,locs_dt,latitude,longitude,
                error_radius, error_semi_major_axis,error_semi_minor_axis,
                error_ellipse_orientation, project, species, qa_status,deploy_dt, end_dt,
                deploy_lat,deploy_long,capture_lat,capture_long, geom)

unlink(here::here('locs_obs'), 
       recursive = TRUE, 
       force = TRUE)
dir.create(here::here('locs_obs'))

write_parquet(locs_obs,
              here::here('locs_obs/locs_obs.parquet'))

dbDisconnect(con, disconnect = TRUE)
