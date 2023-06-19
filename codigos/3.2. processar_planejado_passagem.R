### Lembre-se de ajustar o nome do corredor em todos os scripts.

mao_dupla <- F

nome_corredor <- 'CampoGrande_NorteSul'

testInstall <- function(x) {
  for (i in x) {
    if (!require(i , character.only = TRUE)) {
      install.packages(i , dependencies = TRUE)
      library(i)
    }
  }
}

testInstall(c(
  'gtfstools',
  'dplyr',
  'data.table',
  'sf',
  'mapview',
  'Hmisc',
  'geobr',
  'tidyr'
))

local_dados <- paste0("./corredores/", nome_corredor)

gtfs_sppo <- read_gtfs("../../dados/gtfs/2023/gtfs_rio-de-janeiro.zip") %>% 
  filter_by_weekday('monday')

calendar_remover <- gtfs_sppo$calendar %>% 
  filter(service_id %like% 'DESAT') %>% 
  select(service_id) %>% 
  unlist()

gtfs_sppo <- gtfs_sppo %>% 
  filter_by_service_id(calendar_remover,keep=F)

gtfs_inter <- read_gtfs("../../dados/gtfs/2023/detro.zip") %>% 
  filter_by_weekday('monday')

data_ref <- as.Date('2023-05-09')

pontos_usar <- st_read(paste0(local_dados,"/pontos_usar.gpkg"))

via <- st_read(paste0(local_dados,"/via.gpkg")) %>% 
  summarise()

gtfs_sppo <- gtfs_sppo %>%
  filter_by_weekday(., tolower(lubridate::wday(
    data_ref, label = T, abbr = F, locale = "English"
  ))) %>%
  filter_by_stop_id(., pontos_usar$stop_id) %>%
  frequencies_to_stop_times(.)

gtfs_inter <- gtfs_inter %>%
  filter_by_weekday(., tolower(lubridate::wday(
    data_ref, label = T, abbr = F, locale = "English"
  ))) %>%
  filter_by_stop_id(., pontos_usar$stop_id) %>%
  frequencies_to_stop_times(.)

gtfs_sppo <- convert_time_to_seconds(gtfs_sppo, file = "stop_times")

gtfs_inter <- convert_time_to_seconds(gtfs_inter, file = "stop_times")

gtfs_inter$shapes <- as.data.table(gtfs_inter$shapes) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes_sppo <- convert_shapes_to_sf(gtfs_sppo)

shapes_inter <- convert_shapes_to_sf(gtfs_inter)

stop_times_sppo <- gtfs_sppo$stop_times %>% 
  left_join(select(gtfs_sppo$trips,trip_id,trip_short_name,direction_id)) %>% 
  mutate(linha_sentido = paste0(trip_short_name,'_',direction_id))

stop_times_inter <- gtfs_inter$stop_times %>% 
  left_join(select(gtfs_inter$trips,trip_id,trip_short_name,direction_id)) %>% 
  mutate(linha_sentido = paste0(trip_short_name,'_',direction_id))

stop_times_manter <- data.frame()

for (i in 1:nrow(pontos_usar)) {
  ponto_analise = pontos_usar$stop_id[i]
  ponto <- pontos_usar %>%
    filter(stop_id == ponto_analise) %>% 
    st_set_crs(4326) %>% 
    distinct(stop_id,.keep_all = T)
  
  if(!mao_dupla){
    via_prox <- st_nearest_points(via, ponto)
    
    ponto_via_prox <- st_coordinates(via_prox[1]) %>% 
      as.data.frame() %>% 
      rename(stop_lon = X,
             stop_lat = Y) %>% 
      select(-c(L1)) %>% 
      left_join(ponto) %>% 
      filter(is.na(stop_id)) %>% 
      st_drop_geometry() %>% 
      rename(longitude = stop_lon,
             latitude = stop_lat) %>% 
      select(latitude,longitude) %>% 
      st_as_sf(coords = c("longitude", "latitude")) %>% 
      st_set_crs(4326) %>%
      st_transform(31983) %>% 
      st_buffer(4) %>% 
      st_transform(4326)
    
    linhas <- st_intersection(ponto_via_prox, shapes_sppo) %>%
      st_drop_geometry()
  } else {
    linhas <- gtfs_sppo$stop_times %>% 
      filter(stop_id == ponto_analise) %>% 
      left_join(select(gtfs_sppo$trips,trip_id,trip_short_name,direction_id)) %>% 
      distinct(trip_short_name, direction_id)
  }
  
  linhas_sentido <- linhas %>%
    left_join(select(gtfs_sppo$trips,shape_id,trip_short_name,direction_id)) %>% 
    mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
    select(linhas_sentido) %>% 
    distinct(linhas_sentido)
  
  for(j in 1:nrow(linhas_sentido)){
    linha_olhar <- linhas_sentido[j,]
    pontos_linha_olhar <- stop_times_sppo %>% 
      filter(linha_sentido == linha_olhar) %>% 
      distinct(stop_id) %>% 
      left_join(pontos_usar) %>% 
      filter(!is.na(stop_lat))
    
    ponto_usar_linha <- pontos_linha_olhar %>% 
      select(-c(geom)) %>% 
      st_as_sf(coords = c('stop_lon','stop_lat')) %>% 
      st_set_crs(4326) %>% 
      mutate(dist = as.integer(st_distance(.,ponto))) %>% 
      slice_min(dist) %>% 
      st_drop_geometry() %>% 
      select(stop_id) %>% 
      unlist()
    
    stop_ponto_linha <- stop_times_sppo %>% 
      filter(linha_sentido == linha_olhar) %>% 
      filter(stop_id == ponto_usar_linha) %>% 
      mutate(stop_id = ponto_analise)
    
    stop_times_manter <- stop_times_manter %>% 
      bind_rows(stop_ponto_linha)
      
  }

}


sppo_hora_planejado <- stop_times_manter %>%
  filter(stop_id %in% pontos_usar$stop_id) %>% 
  group_by(stop_id) %>% 
  mutate(arrival_time_secs = if_else(arrival_time_secs>86400,
                                     arrival_time_secs-86400,
                                     arrival_time_secs)) %>% 
  mutate(arrival_time = hms::hms(lubridate::seconds_to_period(arrival_time_secs))) %>% 
  mutate(fx_hora = lubridate::hour(as.POSIXct(as.ITime(arrival_time)))) %>%
  group_by(fx_hora,stop_id) %>%
  summarise(sppo_hora_planejado = n()) %>%
  arrange(fx_hora)

sppo_hora_planejado_detalhado <- stop_times_manter %>%
  filter(stop_id %in% pontos_usar$stop_id) %>% 
  group_by(stop_id) %>% 
  mutate(arrival_time_secs = if_else(arrival_time_secs>86400,
                                     arrival_time_secs-86400,
                                     arrival_time_secs)) %>% 
  mutate(arrival_time = hms::hms(lubridate::seconds_to_period(arrival_time_secs))) %>% 
  mutate(fx_hora = lubridate::hour(as.POSIXct(as.ITime(arrival_time)))) %>%
  left_join(select(gtfs_sppo$trips, trip_id, trip_short_name)) %>%
  group_by(fx_hora,stop_id,trip_short_name) %>%
  summarise(sppo_hora_planejado = n()) %>%
  arrange(fx_hora)

stop_times_manter_inter <- data.frame()

rm(i,j)

for (i in 1:nrow(pontos_usar)) {
  ponto_analise = pontos_usar$stop_id[i]
  ponto <- pontos_usar %>%
    filter(stop_id == ponto_analise) %>% 
    st_set_crs(4326) %>% 
    distinct(stop_id,.keep_all = T)
  
  if(!mao_dupla){
    via_prox <- st_nearest_points(via, ponto)
    
    ponto_via_prox <- st_coordinates(via_prox[1]) %>% 
      as.data.frame() %>% 
      rename(stop_lon = X,
             stop_lat = Y) %>% 
      select(-c(L1)) %>% 
      left_join(ponto) %>% 
      filter(is.na(stop_id)) %>% 
      st_drop_geometry() %>% 
      rename(longitude = stop_lon,
             latitude = stop_lat) %>% 
      select(latitude,longitude) %>% 
      st_as_sf(coords = c("longitude", "latitude")) %>% 
      st_set_crs(4326) %>%
      st_transform(31983) %>% 
      st_buffer(4) %>% 
      st_transform(4326)
    
    linhas_inter <- st_intersection(ponto_via_prox, shapes_inter) %>%
      st_drop_geometry() 
  } else {
    
    linhas_inter <- gtfs_inter$stop_times %>% 
      filter(stop_id == ponto_analise) %>% 
      left_join(select(gtfs_inter$trips,trip_id,trip_short_name,direction_id)) %>% 
      distinct(trip_short_name, direction_id)
  }
  
  if(nrow(linhas_inter) > 0){
    linhas_sentido <- linhas_inter %>%
      left_join(select(gtfs_inter$trips,shape_id,trip_short_name,direction_id)) %>% 
      mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
      select(linhas_sentido) %>% 
      distinct(linhas_sentido)
    
    for(j in 1:nrow(linhas_sentido)){
      linha_olhar <- linhas_sentido[j,]
      pontos_linha_olhar <- stop_times_inter %>% 
        filter(linha_sentido == linha_olhar) %>% 
        distinct(stop_id) %>% 
        left_join(pontos_usar) %>% 
        filter(!is.na(stop_lat))
      
      ponto_usar_linha <- pontos_linha_olhar %>% 
        select(-c(geom)) %>% 
        st_as_sf(coords = c('stop_lon','stop_lat')) %>% 
        st_set_crs(4326) %>% 
        mutate(dist = as.integer(st_distance(.,ponto))) %>% 
        slice_min(dist) %>% 
        st_drop_geometry() %>% 
        select(stop_id) %>% 
        unlist()
      
      stop_ponto_linha <- stop_times_inter %>% 
        filter(linha_sentido == linha_olhar) %>% 
        filter(stop_id == ponto_usar_linha) %>% 
        mutate(stop_id = ponto_analise)
      
      stop_times_manter_inter <- stop_times_manter_inter %>% 
        bind_rows(stop_ponto_linha)
    }
  }
}

inter_hora_planejado <- stop_times_manter_inter %>%
  filter(stop_id %in% pontos_usar$stop_id) %>% 
  group_by(stop_id) %>% 
  mutate(arrival_time_secs = if_else(arrival_time_secs>86400,
                                     arrival_time_secs-86400,
                                     arrival_time_secs)) %>% 
  mutate(arrival_time = hms::hms(lubridate::seconds_to_period(arrival_time_secs))) %>% 
  mutate(fx_hora = lubridate::hour(as.POSIXct(as.ITime(arrival_time)))) %>%
  group_by(fx_hora,stop_id) %>%
  summarise(inter_hora_planejado = n()) %>%
  arrange(fx_hora)

inter_hora_planejado_detalhado <- stop_times_manter_inter %>%
  filter(stop_id %in% pontos_usar$stop_id) %>% 
  group_by(stop_id) %>% 
  mutate(arrival_time_secs = if_else(arrival_time_secs>86400,
                                     arrival_time_secs-86400,
                                     arrival_time_secs)) %>% 
  mutate(arrival_time = hms::hms(lubridate::seconds_to_period(arrival_time_secs))) %>% 
  mutate(fx_hora = lubridate::hour(as.POSIXct(as.ITime(arrival_time)))) %>%
  left_join(select(gtfs_inter$trips, trip_id, trip_short_name)) %>%
  group_by(fx_hora,stop_id,trip_short_name) %>%
  summarise(inter_hora_planejado = n()) %>%
  arrange(fx_hora)

local_frequencias <- paste0(local_dados, "/frequencias/")
ifelse(!dir.exists(file.path(getwd(), local_frequencias)),
       dir.create(file.path(getwd(), local_frequencias)), FALSE)

local_planejado <- paste0(local_frequencias, "/planejado/")
ifelse(!dir.exists(file.path(getwd(), local_planejado)),
       dir.create(file.path(getwd(), local_planejado)), FALSE)

fwrite(sppo_hora_planejado,
       paste0(local_planejado, "/sppo_hora_planejado_passagem.csv"))
fwrite(
  sppo_hora_planejado_detalhado,
  paste0(local_planejado, "/sppo_hora_planejado_detalhado_passagem.csv")
)
fwrite(inter_hora_planejado,
       paste0(local_planejado, "/inter_hora_planejado_passagem.csv"))
fwrite(
  inter_hora_planejado_detalhado,
  paste0(local_planejado, "/inter_hora_planejado_detalhado_passagem.csv")
)