library(duckdb)
library(dbplyr)
library(dplyr)
library(sf)
library(aniMotum)

con <- dbConnect(duckdb())

dbExecute(con, "INSTALL httpfs; LOAD httpfs;")
# dbExecute(con, "INSTALL 'spatial'; LOAD 'spatial';")

locs_obs <- tbl(con, "read_parquet('https://github.com/noaa-afsc/cefi-ice-seal-movement/raw/main/locs_obs/locs_obs.parquet'") |>
  dplyr::filter(between(locs_dt,
                        deploy_dt,
                        end_dt)) |> 
  dplyr::rename(datetime=locs_dt) |> 
  dplyr::mutate(
    quality = case_when(
      type == 'FastGPS' ~ 'G',
      type == 'User' ~ 'G',
      .default = quality
    )
  ) |> 
  dplyr::filter(!quality %in% c('Z')) |> 
  dplyr::select(speno,deployid,species,
                datetime,
                quality,
                error_semi_major_axis,
                error_semi_minor_axis,
                error_radius,
                geom
                ) |> 
  collect() 


locs_obs <- locs_obs |> 
  group_by(speno) |> 
  arrange(datetime, error_radius) |> 
  mutate(
    rank = 1L,
    rank = case_when(duplicated(datetime, fromLast = FALSE) ~
                       lag(rank) + 1L, TRUE ~ rank))  |> 
  dplyr::filter(rank == 1)  |>  
  arrange(speno,datetime)  |>  
  ungroup() |> 
  st_as_sf() |> 
  st_set_crs(4326)


dbDisconnect(con, disconnect = TRUE)
