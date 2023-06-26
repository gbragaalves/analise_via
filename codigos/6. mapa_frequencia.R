### Lembre-se de ajustar o nome do corredor em todos os scripts.

nome_corredor <- 'EstrCampinho_AvBrasil'

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
  'ggplot2',
  'ggthemes',
  'basemapR',
  'osmdata'
))

resumo <- fread(paste0("./corredores/", nome_corredor,"/resumo.csv"))

nome_corredor_humano <- resumo$nome_humano

local_dados <- paste0("./corredores/", nome_corredor)

subtitulo_mapa <- paste0(nome_corredor_humano, collapse = ", ")

if(!exists("pontos_usar")){
  pontos_usar <- st_read(paste0(local_dados, "/pontos_usar.gpkg"))
}

pontos_usar <- pontos_usar %>% 
  st_set_crs(4326)

ponto_maior_frequencia <- pontos_usar %>% 
  filter(stop_id == resumo$mais_carregado)

cores <- c('#00c0f3'='#00c0f3','#004a80'='#004a80')

rio <- read_municipality(3304557,simplified = F)
via <- st_read(paste0(local_dados,"/via.gpkg"))

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

mapa <- ggplot() +
  geom_sf(data = rio, color='#cccccc', fill='#eeeeee', size = 1, alpha = 1) +
  geom_sf(data = malha_viaria, color = '#d4d3bf', linewidth = 0.5)+
  geom_sf(data = via, color = '#004a80', linewidth = .7) +
  geom_sf(data = pontos_usar, shape=21, color = '#004a80', fill = '#00c0f3',size = 2) +
  geom_sf(data = ponto_maior_frequencia, shape=16, color = 'red',size = 5) +
  coord_sf(xlim = c(st_bbox(bbox_via)[[1]]-0.002, st_bbox(bbox_via)[[3]]+0.002),
           ylim = c(st_bbox(bbox_via)[[2]]-0.002, st_bbox(bbox_via)[[4]]+0.002))+
  scale_colour_manual(values = cores)+
  theme_map() +
  theme(plot.title = element_text(face = "bold", size = 18, family = "cera", colour = "#004a80")) +
  theme(plot.subtitle = element_text(face = "bold", size = 14, family = "cera", colour = "#000000")) +
  labs(title = "Análise de Frequência",
       subtitle = subtitulo_mapa,
  ) +
  theme(line = element_blank(),
        axis.text = element_blank(),
        axis.title = element_blank(),
        panel.background = element_rect(fill = "white"),
        legend.position = 'bottom',
        axis.title.x=element_blank(),
        axis.text.x=element_blank(),
        axis.ticks.x=element_blank())

local_resultados <- paste0(local_dados,"/resultados/")
local_mapas <- paste0(local_resultados,"/mapas/")
ifelse(!dir.exists(file.path(getwd(), local_mapas)),
       dir.create(file.path(getwd(), local_mapas)), FALSE)

end_img <- paste0(local_mapas,nome_corredor,'_frequencia.png')

ggsave(end_img, dpi = 300,bg = "white")

