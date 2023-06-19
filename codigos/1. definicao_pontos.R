### Rode os comandos abaixo caso você não tenha o pacote basemapR instalado.

# remove.packages("cli")
#
# install.packages("cli")
#
# install.packages("devtools")
#
# devtools::install_github('Chrisjb/basemapR')

### Carrega pacotes necessarios para execucao do codigo.


pacman::p_load(gtfstools, dplyr, data.table, sf, mapview, osmdata, Hmisc, geobr, basemapR, sfnetworks)

### Le GTFS de onibus municipais e ajusta shapes.

gtfs <- read_gtfs("../../dados/gtfs/2023/gtfs_rio-de-janeiro.zip") %>%
  filter_by_weekday("monday")

gtfs$shapes <- as.data.table(gtfs$shapes) %>%
  arrange(shape_id, shape_pt_sequence)

### Define o nome do corredor a ser usado nos códigos.
### Não utilizar acentuação ou espaços.

nome_corredor <- "Downtown_Uptown"

### Define o nome do corredor a ser usado em materiais de visualização, como
### mapas. Utilizar grafia compreensível para pessoas, com acentos, espaços e
### demais sinais gráficos.

nome_humano <- "Barra da Tijuca: Av. das Américas e Av. Ayrton Senna"

pontos <- convert_stops_to_sf(gtfs) %>%
  distinct(stop_id, .keep_all = TRUE) %>%
  filter(location_type == 0) %>%
  select(stop_id, stop_name) %>%
  left_join(select(gtfs$stops, stop_id, stop_lat, stop_lon),
    by = c("stop_id")
  )

trips_join <- gtfs$trips %>%
  select(shape_id, trip_id, trip_short_name, trip_headsign, direction_id) %>%
  distinct(shape_id, .keep_all = T)

shapes <- convert_shapes_to_sf(gtfs) %>%
  left_join(trips_join)

View(shapes)

################################################################################
################################################################################
######### DEFINA AQUI A LINHA E O SENTIDO DA LINHA QUE SERÁ UTILIZADO. #########
################################################################################
################################################################################

linha_usar <- "900"
sentido_usar <- "1"

shape_usar <- shapes %>%
  filter(trip_short_name == linha_usar & direction_id == sentido_usar)

################################################################################
################################################################################
############ DEFINA A DISTÂNCIA ENTRE CADA PONTO DE CORTE DA VIA. ##############
################################################################################
################################################################################

refinamento_corte <- 50

shape_usar_m <- shape_usar %>%
  st_transform(31983)

via_pontos <- st_line_sample(shape_usar_m, n = st_length(shape_usar_m) / refinamento_corte, type = "regular")

quebras_via <- data.frame(st_coordinates(via_pontos)) %>%
  select(X, Y) %>%
  mutate(ponto = 1:nrow(.)) %>%
  st_as_sf(coords = c("X", "Y")) %>%
  st_set_crs(31983) %>%
  st_transform(4326)

mapview(quebras_via) + shape_usar_m

################################################################################
################################################################################
################## DEFINA O PRIMEIRO E O ÚLTIMO PONTO DA VIA ###################
################# PARA RECORTAR O CORREDOR CONFORME DESEJADO. ##################
############### SE NECESSÁRIO, REAVALIE O REFINAMENTO DO CORTE. ################
################################################################################
################################################################################

inicio_corredor <- 1
fim_corredor <- 191

inicio_corredor <- quebras_via %>%
  filter(ponto == inicio_corredor)

fim_corredor <- quebras_via %>%
  filter(ponto == fim_corredor)

pontos_limite <- rbind(inicio_corredor, fim_corredor)

shape_usar_sfnetwork <- as_sfnetwork(shape_usar)

mescl_shape_sfnetwork <- st_network_blend(shape_usar_sfnetwork, pontos_limite)

shapes_sf <- mescl_shape_sfnetwork %>%
  sfnetworks::activate("edges") %>%
  mutate(trecho_id = as.character(1:n())) %>%
  st_as_sf() %>%
  mutate(extensao = as.integer(st_length(.)))

mapview(shapes_sf, zcol = "trecho_id")

################################################################################
################################################################################
################### CLIQUE NO MAPA E VERIFIQUE QUAL TRECHO #####################
################### DA LINHA SERÁ CONSIDERADO NA AVALIAÇÃO. ####################
######################### (GERALMENTE É O TRECHO 2.) ###########################
################################################################################
################################################################################

trecho_usar <- 2
via <- shapes_sf %>%
  filter(trecho_id == trecho_usar)

via_buffer <- via %>%
  st_transform(31983) %>%
  st_buffer(30) %>%
  st_transform(4326)

linhas_ponto <- gtfs$stop_times %>%
  left_join(select(gtfs$trips, trip_id, trip_short_name, trip_headsign, direction_id)) %>%
  select(trip_id, stop_id, trip_short_name, trip_headsign, direction_id) %>%
  distinct(stop_id, trip_short_name, direction_id, .keep_all = T) %>%
  mutate(linha_sentido = paste0(trip_short_name, "_", direction_id))

linhas_ponto_agg <- aggregate(linha_sentido ~ stop_id, data = linhas_ponto, FUN = function(x) unique(x))

pontos_usar <- st_intersection(pontos, via_buffer) %>%
  distinct(stop_id, .keep_all = T) %>%
  select(stop_id, stop_name, stop_lat, stop_lon)

################################################################################
################################################################################
################# INFORME SE A VIA POSSUI PONTOS SELETIVADOS. ##################
####### SE NÃO POSSUIR, SERÃO UTILIZADOS TODOS OS PONTOS POR ONDE A LINHA ######
###### SELECIONADA PASSA. CASO HOUVER ALGUM TRECHO COM PONTOS SELETIVADOS ######
######## SERÃO APRESENTADOS TODOS OS PONTOS A ATÉ 30 METROS DE DISTÂNCIA #######
####### DO EIXO DA VIA E VOCÊ DEVERÁ REMOVER OS PONTOS QUE EVENTUALMENTE #######
#################### NÃO FAÇAM PARTE DO TRECHO EM ANÁLISE. #####################
################################################################################
################################################################################

via_seletivada <- F

if (!via_seletivada) {
  pontos_linha <- gtfs$stop_times %>%
    filter(trip_id == shape_usar$trip_id) %>%
    select(stop_id) %>%
    unlist()

  pontos_usar <- pontos_usar %>%
    filter(stop_id %in% pontos_linha)

  rm(pontos_linha)

  mapview(pontos_usar)
} else {
  mapview(pontos_usar)
}

################################################################################
################################################################################
######## CASO HAJA ALGUM PONTO QUE NÃO ESTÁ NA VIA ANALISADA, INCLUA O #########
######## stop_id DENTRO DA VARIÁVEL pontos_remover,ENTRE ASPAS SIMPLES #########
######## SEPARADAS POR VÍRGULA. RODE O if ATÉ OBTER O RESULTADO DESEJADO #######
################################################################################
################################################################################

if (!via_seletivada) {
  pontos_remover <- c("")

  pontos_usar <- pontos_usar %>%
    filter(stop_id %nin% pontos_remover)

  mapview(pontos_usar)
}

pontos_usar <- pontos_usar %>%
  left_join(linhas_ponto_agg)

rm(pontos)

pontos_ver <- as.data.table(table(unlist(pontos_usar$linha_sentido))) %>%
  rename(
    servico = V1,
    ocorrencias = N
  )

pontos_ok <- data.frame()
pontos_usar_filt <- pontos_usar
linhas_pontos_usar <- c()

while (nrow(pontos_usar_filt) != 0) {
  linha_uso <- pontos_ver %>%
    slice_max(ocorrencias, n = 1) %>%
    head(1)

  linha_uso <- linha_uso$servico

  pontos_ok <- pontos_usar_filt %>%
    filter(sapply(linha_sentido, function(x) any(grepl(linha_uso, x))))

  pontos_usar_filt <- pontos_usar_filt %>%
    filter(stop_id %nin% pontos_ok$stop_id)

  pontos_ver <- pontos_ver %>%
    filter(servico != linha_uso)

  if (nrow(pontos_ok) > 0) {
    linhas_pontos_usar <- c(linhas_pontos_usar, linha_uso)
  }
}

linhas_ponto <- linhas_ponto %>%
  filter(linha_sentido %in% linhas_pontos_usar) %>%
  distinct(linha_sentido, .keep_all = T)

stop_times_usar <- gtfs$stop_times %>%
  filter(trip_id %in% linhas_ponto$trip_id) %>%
  ungroup() %>%
  distinct(stop_id, .keep_all = T) %>%
  filter(stop_id %in% pontos_usar$stop_id)

pontos_usar <- pontos_usar %>%
  filter(stop_id %in% stop_times_usar$stop_id) %>%
  left_join(select(stop_times_usar, stop_id, stop_sequence)) %>%
  filter(!grepl("Ponto Final", stop_name))

segmentos <- st_segmentize(via, 10)

pontos <- st_cast(segmentos, "POINT") %>%
  mutate(ordem = row_number())

nearest_points <- st_nearest_feature(pontos_usar, pontos)

pontos_usar <- cbind(pontos_usar, nearest_points) %>%
  rename(ordem = nearest_points) %>%
  arrange(ordem) %>%
  mutate(stop_sequence = 1:n()) %>%
  select(-c(linha_sentido))

mapview(via) + (pontos_usar)

local_dados <- paste0("corredores/", nome_corredor)
ifelse(!dir.exists(file.path(getwd(), local_dados)),
  dir.create(file.path(getwd(), local_dados), recursive = TRUE), FALSE
)

st_write(pontos_usar, paste0(local_dados, "/pontos_usar.gpkg"), delete_dsn = TRUE)
st_write(via, paste0(local_dados, "/via.gpkg"), delete_dsn = TRUE)

linhas_pontos_usar <- paste0(linhas_pontos_usar, collapse = ",")

resumo_corredor <- data.frame(
  nome_corredor = nome_corredor,
  nome_humano = nome_humano,
  linha_base = linha_usar,
  sentido_base = sentido_usar,
  linhas_stop_times = linhas_pontos_usar
)

fwrite(resumo_corredor, paste0("./corredores/", nome_corredor, "/resumo.csv"))
