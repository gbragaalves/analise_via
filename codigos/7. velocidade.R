### Lembre-se de ajustar o nome do corredor em todos os scripts.

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
  'tidyr',
  'sfnetworks',
  'RColorBrewer',
  'pbapply',
  'osmdata',
  'basemapR',
  'ggthemes',
  'ggplot2'
))

local_dados <- paste0("./corredores/", nome_corredor)

gtfs <- read_gtfs("../../dados/gtfs/2023/gtfs_rio-de-janeiro.zip") %>% 
  filter_by_weekday('monday')

resumo <- fread(paste0("./corredores/", nome_corredor,"/resumo.csv"))

nome_vias <- resumo$nome_humano

data_ref <- as.Date('2023-05-09')

data_ref_text <- as.character(data_ref,format='%d-%m-%Y')

sequencia_dias <- 3

local_dados <- paste0("./corredores/", nome_corredor)
local_gps_bruto <- paste0(local_dados, "/gps/bruto/")


nm <- list.files(path=local_gps_bruto, full.names = T, pattern = "*.csv",recursive = TRUE)
nm <- grep(paste0("^.*gps_", data_ref_text,".*\\.csv$"), nm, value = TRUE)

gps <- do.call(rbind, lapply(nm, function(x) fread(file=x)))

gps <- gps %>% 
  distinct(id_veiculo,timestamp_gps,.keep_all = T)

local_viagens <- paste0(local_dados, "/viagens/")

nm <- list.files(path=local_viagens, full.names = T, pattern = "*.csv",recursive = TRUE)
nm <- grep(paste0("^.*viagens_", data_ref_text,".*\\.csv$"), nm, value = TRUE)

viagens <- do.call(rbind, lapply(nm, function(x) fread(file=x)))

viagens <- viagens %>% 
  distinct(id_viagem,.keep_all = T) %>%
  select(
    id_veiculo,
    datetime_partida,
    datetime_partida,
    datetime_chegada,
    servico_realizado,
    id_viagem
  ) %>%
  rename(id_veiculo_viagem = id_veiculo)

via <- st_read(paste0(local_dados,"/via.gpkg")) %>% 
  summarise() %>% 
  st_transform(31983)


via_buffer <- via %>% 
  st_buffer(30)

if(!exists("pontos_usar")){
  pontos_usar <- st_read(paste0(local_dados, "/pontos_usar.gpkg"))
}

numOfPoints  <-  as.integer(st_length(via))

via_pontos <- st_line_sample(via, n = numOfPoints, type = "regular")

via_pontos <- data.frame(st_coordinates(via_pontos)) %>% 
  select(X,Y) %>% 
  mutate(metro = 1:nrow(.)) %>% 
  st_as_sf(coords = c('X','Y')) %>% 
  st_set_crs(31983) %>% 
  st_transform(4326)

linhas <- gtfs$stop_times %>%
  filter(stop_id %in% pontos_usar$stop_id) %>%
  left_join(select(gtfs$trips, trip_id, trip_short_name, direction_id)) %>%
  distinct(trip_short_name, .keep_all = T)

linhas_sentido <- linhas %>%
  mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
  select(linhas_sentido) %>%
  unlist()

gps <- gps %>%
  # select(-c(stop_lon,stop_lat)) %>% 
  #tidyr::separate(geometry, c("longitude", "latitude"), sep = "\\|") %>%
  st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
  left_join(
    viagens,
    join_by(
      id_veiculo == id_veiculo_viagem,
      timestamp_gps >= datetime_partida,
      timestamp_gps <= datetime_chegada
    )
  ) %>%
  filter(!is.na(id_viagem))

gps <- gps %>%
  st_transform(31983) 
  
gps <- st_intersection(gps,via_buffer)

gps <- gps %>% 
  st_transform(4326) %>% 
  mutate(metro = st_nearest_feature(.,via_pontos)) %>% 
  mutate(dist = st_distance(.,via_pontos[metro,], by_element=TRUE)) %>% 
  filter(as.integer(dist) < 30)

gps <- gps %>% 
  group_by(id_viagem) %>% 
  arrange(timestamp_gps) %>% 
  mutate(metro_anterior = lag(metro)) %>% 
  mutate(tempo_anterior = lag(timestamp_gps)) %>% 
  mutate(time_dif = as.integer(difftime(timestamp_gps,lag(timestamp_gps),units = "secs"))) %>% 
  mutate(distancia_percorrida = metro-metro_anterior) %>% 
  filter(distancia_percorrida > -5) %>% 
  mutate(distancia_percorrida = if_else(distancia_percorrida < 0,0,distancia_percorrida)) %>% 
  mutate(velocidade_ms = distancia_percorrida/time_dif) %>% 
  mutate(velocidade = round(velocidade_ms*3.6,2)) %>% 
  filter(!is.na(metro_anterior))

limites <- st_bbox(st_transform(via,4326))

pontos_usar <- pontos_usar %>% 
  st_set_crs(4326)

tuneis_query <- opq(bbox = limites) %>%
  add_osm_feature(key = c('tunnel','highway'),
                  value_exact = FALSE)

tunel <- osmdata_sf(tuneis_query)

rm(tuneis_query)

if(!is.null(tunel$osm_lines)){
  
  tunel <- tunel$osm_lines %>% 
    filter(highway %nin% c('footway','pedestrian'))
  
  if(nrow(tunel)>0){
    tunel <- tunel %>% 
      st_transform(31983) %>% 
      st_buffer(10) %>% 
      st_transform(4326)
    
    pontos_no_tunel <- st_intersection(pontos_usar,tunel)
    
    pontos_usar <- pontos_usar %>% 
      filter(stop_id %nin% pontos_no_tunel$stop_id)
  }
}

via <- via %>% 
  st_transform(4326)

via_sfnetwork <- as_sfnetwork(via)

mescl_via_sfnetwork <- st_network_blend(via_sfnetwork, pontos_usar)

via_seg_sf <- mescl_via_sfnetwork %>%
  sfnetworks::activate("edges") %>%
  mutate(trecho_id = as.character(1:n())) %>%
  st_as_sf() %>% 
  mutate(extensao = as.integer(st_length(.)))

via_seg_sf_buffer <- via_seg_sf %>% 
  st_transform(31983) %>% 
  st_buffer(2)

velocidades <- data.frame()

for(t in 1:nrow(via_seg_sf_buffer)){
  segmento_testado <- via_seg_sf_buffer %>% 
    filter(trecho_id == t) %>% 
    st_transform(4326)
  extensao_segmento <- st_filter(via_pontos,segmento_testado)
  inicio <- min(extensao_segmento$metro)+2
  fim <- max(extensao_segmento$metro)-2
  gps_considerar <- gps %>% 
    filter(metro > inicio &
             metro_anterior < fim) %>% 
    mutate(inicio_considerado = if_else(metro_anterior<inicio,inicio,metro_anterior),
           fim_considerado = if_else(metro>fim,fim,metro),
           extensao_trecho = fim_considerado - inicio_considerado,
           extensao_trecho = if_else(extensao_trecho < 0,0,extensao_trecho)) %>% 
    mutate(tempo_trecho = extensao_trecho/velocidade_ms) %>% 
    mutate(tempo_trecho = if_else(extensao_trecho==0,time_dif,tempo_trecho))
  
  resumo_segmento <- gps_considerar %>% 
    st_drop_geometry() %>% 
    group_by(id_viagem) %>% 
    summarise(tempo_trecho = sum(tempo_trecho),
              dist_trecho = sum(extensao_trecho),
              hora = min(timestamp_gps)) %>% 
    mutate(veloc_media = (dist_trecho/tempo_trecho)*3.6,
           fx_hora = lubridate::hour(hora))
  
  sumario_velocidade <- resumo_segmento %>% 
    group_by(fx_hora) %>% 
    summarise(veloc_media = round(weighted.mean(veloc_media, dist_trecho),2),
              amostras = n()) %>% 
    mutate(trecho_id = as.character(t))
  velocidades <- rbindlist(list(velocidades,sumario_velocidade))
}

congest <- velocidades %>%
  mutate(trecho_id = as.character(trecho_id)) %>% 
  left_join(select(via_seg_sf,trecho_id,extensao)) %>% 
  group_by(fx_hora) %>% 
  summarise(veloc = weighted.mean(veloc_media, extensao),
            amostras = sum(amostras)) 

pior_hora <- congest %>% 
  filter(amostras > max(amostras)*0.1) %>% 
  slice_min(veloc)

velocs_pico <- velocidades %>% 
  mutate(trecho_id = as.character(trecho_id)) %>% 
  filter(fx_hora == pior_hora$fx_hora)

view_congest <- via_seg_sf %>% 
  left_join(velocs_pico)

mapviewOptions(vector.palette = colorRampPalette(rev(RColorBrewer::brewer.pal(9, "YlOrRd")[4:9])))

mapview(view_congest,
        lwd=10,
        zcol="veloc_media")

rio <- read_municipality(3304557,simplified = F)

bbox_via <- expand_bbox(st_bbox(pontos_usar),50,50)
bbox_vias <- expand_bbox(st_bbox(pontos_usar),500,500)

vias_usar <- c('motorway', 'trunk', 'primary', 'secondary', 'tertiary',
               'motorway_link', 'trunk_link', 'primary_link', 
               'secondary_link', 'tertiary_link')

malha_viaria_query <- opq(bbox = bbox_vias) %>%
  add_osm_feature(key = 'highway',
                  value_exact = T)

malha_viaria <- osmdata_sf(malha_viaria_query)

malha_viaria <- malha_viaria$osm_lines %>% 
  filter(highway %in% vias_usar)

subtitulo_mapa <- paste0("Horário Crítico (",
                         pior_hora$fx_hora,
                         "h - ",
                         as.integer(pior_hora$fx_hora)+1,
                         "h)")

legenda_mapa <-
  paste0(
    "Dados de ",
    format(data_ref, "%d/%m/%Y"),
    " até ",
    format(data_ref + (sequencia_dias-1), "%d/%m/%Y"),
    "."
  )

cores <- c('#00c0f3'='#00c0f3','#004a80'='#004a80')
reds = colorRampPalette(rev(RColorBrewer::brewer.pal(9, "YlOrRd")[4:9]))

mapa <- ggplot() +
  geom_sf(data = rio, color='#cccccc', fill='#eeeeee', size = 1, alpha = 1) +
  geom_sf(data = malha_viaria, color = '#d4d3bf', linewidth = 0.5)+
  geom_sf(data = view_congest, linewidth = 3, aes(color = veloc_media)) +
  scale_color_gradientn(colors = rev(brewer.pal(9, "Reds")[1:9]),
                        label = scales::label_number(suffix = " km/h")) +
  geom_sf(data = pontos_usar, shape=21, color = '#004a80', fill = '#00c0f3',size = 1.5) +
  coord_sf(xlim = c(st_bbox(bbox_via)[[1]]-0.002, st_bbox(bbox_via)[[3]]+0.002),
           ylim = c(st_bbox(bbox_via)[[2]]-0.002, st_bbox(bbox_via)[[4]]+0.002))+
  theme_map() +
  theme(plot.title = element_text(face = "bold", size = 18, family = "cera", colour = "#004a80")) +
  theme(plot.subtitle = element_text(face = "bold", size = 14, family = "cera", colour = "#000000")) +
  labs(title = paste0("Velocidade Média:\n",nome_vias),
       subtitle = subtitulo_mapa,
       color = "Velocidade média (km/h)",
       caption = legenda_mapa
  ) +
  theme(line = element_blank(),
        axis.text = element_blank(),
        axis.title = element_blank(),
        panel.background = element_rect(fill = "white"),
        legend.position = 'bottom',
        axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank(),
        legend.key.width = unit(.10, "npc"),
        legend.box = "none")

local_resultados <- paste0(local_dados,"/resultados/")
local_mapas <- paste0(local_resultados,"/mapas/")
ifelse(!dir.exists(file.path(getwd(), local_mapas)),
       dir.create(file.path(getwd(), local_mapas)), FALSE)

end_img <- paste0(local_mapas,nome_corredor,'_velocidade.png')

ggsave(end_img, dpi = 300,bg = "white")


local_resultados <- paste0(local_dados, "/resultados/")
ifelse(!dir.exists(file.path(getwd(), local_resultados)),
       dir.create(file.path(getwd(), local_resultados)), FALSE)
local_relatorios <- paste0(local_resultados, "/relatorios/")
ifelse(!dir.exists(file.path(getwd(), local_relatorios)),
       dir.create(file.path(getwd(), local_relatorios)), FALSE)
local_velocidades <- paste0(local_relatorios, "/velocidades/")
ifelse(!dir.exists(file.path(getwd(), local_velocidades)),
       dir.create(file.path(getwd(), local_velocidades)), FALSE)

local_veloc_media_corredor <-
  paste0(local_velocidades, "/", nome_corredor, "_veloc_media_corredor_fx-hora.csv")
fwrite(congest,local_veloc_media_corredor)

local_veloc_trecho_detalhado  <-
  paste0(local_velocidades, "/", nome_corredor, "_veloc_trecho_detalhado.csv")
fwrite(velocidades,local_veloc_trecho_detalhado)

local_veloc_trecho_pico  <-
  paste0(local_velocidades, "/", nome_corredor, "_veloc_trecho_pico.csv")
fwrite(velocs_pico,local_veloc_trecho_pico)