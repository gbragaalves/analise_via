### Lembre-se de ajustar o nome do corredor em todos os scripts.

mao_dupla <- F

nome_corredor <- "EstrCampinho_AvBrasil"

usar_frescao <- TRUE

################################################################################
################################################################################
####### DEFINIR A DATA DE REFERÊNCIA E A QUANTIDADE DE DIAS CUJOS DADOS ########
######## SERÃO UTILIZADOS. COMO PADRÃO, UTILIZA-SE TRÊS DIAS, INICIANDO ########
############################## NUMA TERÇA-FEIRA. ###############################
################ UTILIZE DATAS A PARTIR DE 01 DE JUNHO DE 2022. ################
################################################################################
################################################################################

data_ref <- as.Date("2023-05-09")

sequencia_dias <- 3

pacman::p_load(
  gtfstools,
  dplyr,
  data.table,
  sf,
  mapview,
  Hmisc,
  geobr,
  tidyr,
  basedosdados,
  lubridate,
  nngeo,
  glue
)

basedosdados::set_billing_id("rj-smtr")

gtfs <- read_gtfs("../../dados/gtfs/2023/gtfs_rio-de-janeiro.zip") %>%
  filter_by_weekday("monday")

gtfs_inter <- read_gtfs("../../dados/gtfs/2023/detro.zip") %>%
  filter_by_weekday("monday")

local_dados <- paste0("./corredores/", nome_corredor)

distancia_gps <- 500

dias <- seq(data_ref, length.out = sequencia_dias, by = "days")
dias_paste <- paste(dias, collapse = "','")

pontos_usar <- if (!exists("pontos_usar")) {
  st_read(paste0(local_dados, "/pontos_usar.gpkg"), stringsAsFactors = FALSE) %>%
    filter(!is.na(stop_id))
}

resumo_corredor <- fread(paste0("./corredores/", nome_corredor, "/resumo.csv"))

resumo_corredor <- resumo_corredor %>% 
  mutate(usar_frescao = usar_frescao)

fwrite(resumo_corredor,paste0("./corredores/", nome_corredor, "/resumo.csv"))

local_viagens <- paste0(local_dados, "/viagens/")
ifelse(!dir.exists(file.path(getwd(), local_viagens)),
  dir.create(file.path(getwd(), local_viagens)), FALSE
)

local_gps <- paste0(local_dados, "/gps/")
ifelse(!dir.exists(file.path(getwd(), local_gps)),
  dir.create(file.path(getwd(), local_gps)), FALSE
)

via <- st_read(paste0(local_dados, "/via.gpkg")) %>%
  summarise()

gtfs$shapes <- as.data.table(gtfs$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id,shape_pt_sequence)

dicionario_lecd <- fread('../../dados/insumos/correspondencia_servico_lecd.csv')

shapes <- convert_shapes_to_sf(gtfs) %>%
  left_join(distinct(gtfs$trips, shape_id, .keep_all = TRUE) %>% 
              select(trip_short_name, direction_id, shape_id),
            by = "shape_id") %>% 
  left_join(dicionario_lecd, by = c('trip_short_name' = 'LECD')) %>% 
  mutate(trip_short_name = if_else(!is.na(servico),servico,trip_short_name)) %>% 
  select(-c(servico))

gtfs_inter$shapes <- as.data.table(gtfs_inter$shapes) %>% 
  group_by(shape_id) %>% 
  arrange(shape_id,shape_pt_sequence)

shapes_inter <- convert_shapes_to_sf(gtfs_inter) %>%
  left_join(distinct(gtfs_inter$trips, shape_id, .keep_all = TRUE) %>% 
              select(trip_short_name, direction_id, shape_id),
            by = "shape_id")

### Baixar viagens ----

area_download <- via %>%
  st_transform(31983) %>%
  st_buffer(distancia_gps) %>% 
  st_transform(4326) %>%
  st_geometry() %>%
  st_as_text()

pontos_usar <- pontos_usar %>%
  st_set_crs(4326) %>%
  distinct(stop_id, .keep_all = T)

via_prox <- st_nearest_points(via, pontos_usar)

ponto_via_prox <- st_coordinates(via_prox) %>%
  as.data.frame() %>% 
  transmute(
    longitude = X,
    latitude = Y
  ) %>%
  distinct() %>%
  st_as_sf(coords = c("longitude", "latitude")) %>%
  st_set_crs(4326) %>%
  st_transform(31983) %>%
  st_buffer(4) %>%
  st_transform(4326)

linhas <- st_intersection(ponto_via_prox, shapes) %>%
  st_drop_geometry() %>%
  distinct(trip_short_name, direction_id)

linhas_inter <- st_intersection(ponto_via_prox, shapes_inter) %>%
  st_drop_geometry() %>%
  distinct(trip_short_name, direction_id)

linhas_lista <- linhas %>%
  pull(trip_short_name)

lecds_usar <- dicionario_lecd %>% 
  filter(servico %in% linhas_lista) %>% 
  pull(LECD)

linhas_lista <- c(linhas_lista,lecds_usar) %>% 
  paste(collapse = "','")

linhas_sentido <- linhas %>%
  mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
  select(linhas_sentido)

end_viagens <- paste0(
  local_viagens,
  "viagens_",
  format(data_ref, "%d-%m-%Y"),
  "_",
  sequencia_dias,
  "d.csv"
)

if (file.exists(end_viagens)) {
  viagens <- fread(end_viagens) %>% 
    mutate(servico_realizado = as.character(servico_realizado))
} else {
  query_viagens <- glue::glue("SELECT *
                               FROM `rj-smtr.projeto_subsidio_sppo.viagem_completa`
                               WHERE data IN ('{dias_paste}') AND servico_informado IN ('{linhas_lista}')")

  viagens <- try(basedosdados::read_sql(query_viagens)) %>%
    left_join(dicionario_lecd, by = c('servico_realizado' = 'LECD')) %>% 
    mutate(servico_realizado = if_else(!is.na(servico),servico,servico_realizado)) %>% 
    select(-c(servico)) %>% 
    mutate(concat_linha_sentido = paste0(
      servico_realizado,
      "_",
      ifelse(sentido == "V", "1", "0")
    )) %>%
    filter(concat_linha_sentido %in% linhas_sentido$linhas_sentido) %>%
    select(data, id_veiculo, id_viagem, servico_realizado, shape_id, sentido, 
           datetime_partida, datetime_chegada, concat_linha_sentido) %>% 
    mutate(servico_realizado = as.character(servico_realizado))

  if (!inherits(viagens, "try-error")) {
    fwrite(viagens, end_viagens)
  } else {
    stop("Erro ao tentar baixar dados de viagens do BigQuery.")
  }
}

if(usar_frescao){
  source("codigos/2.1. apuracao_viagens_frescao.R")
  
  viagens_frescao <- viagens_frescao %>% 
    filter(concat_linha_sentido %in% linhas_sentido$linhas_sentido)
  
  # Realizar o bind_rows apenas com as colunas comuns 
  viagens <- viagens %>% 
    bind_rows(viagens_frescao)
  
  rm(gps_frescao,gtfs_frescao,viagens_freq_frescao,viagens_frescao,
     frescoes_baixar,frescoes_usar,frescoes_usar_desagg)
}

### Baixar GPS ----

local_gps_bruto <- paste0(local_dados, "/gps/bruto/")
ifelse(!dir.exists(file.path(getwd(), local_gps_bruto)),
  dir.create(file.path(getwd(), local_gps_bruto)), FALSE
)

local_gps_proc <- paste0(local_dados, "/gps/proc/")
ifelse(!dir.exists(file.path(getwd(), local_gps_proc)),
  dir.create(file.path(getwd(), local_gps_proc)), FALSE
)

end_gps_bruto <- file.path(local_gps_bruto,
                           glue::glue("gps_{format(data_ref, '%d-%m-%Y')}_{sequencia_dias}d.csv"))

end_gps_proc <- file.path(local_gps_proc,
                          glue::glue("gps_{format(data_ref, '%d-%m-%Y')}_{sequencia_dias}d-PROC.csv"))

if (!file.exists(end_gps_bruto)) {
  query_gps <- glue::glue("SELECT *
                           FROM `rj-smtr.br_rj_riodejaneiro_veiculos.gps_sppo`
                           WHERE data IN ('{dias_paste}')
                           AND servico IN ('{linhas_lista}')
                           AND ST_WITHIN(ST_GEOGPOINT(longitude, latitude), ST_GEOGFROMTEXT('{area_download}'))")
  
  dicionario_lecd <- dicionario_lecd %>% 
    rename(linha = servico)
  
  gps <- try(basedosdados::read_sql(query_gps)) %>%
    left_join(dicionario_lecd, by = c('servico' = 'LECD')) %>% 
    mutate(servico = if_else(!is.na(linha),linha,servico)) %>% 
    select(-c(linha)) %>%
    select(timestamp_gps, data, hora, id_veiculo, servico, 
           latitude, longitude, velocidade_instantanea)
  
  gps <- gps %>%
    mutate(hora = hms::as_hms(hora))
  
  if (!inherits(gps, "try-error")) {
    fwrite(gps, end_gps_bruto)
  } else {
    stop("Erro ao tentar baixar dados de GPS do BigQuery.")
  }
} else {
  gps <- fread(end_gps_bruto)
}

gps_sf <- gps %>%
  st_as_sf(coords = c("longitude", "latitude")) %>%
  st_set_crs(4326)

### Processar por ponto ----

for (i in 1:nrow(pontos_usar)) {
  ponto_analise <- pontos_usar$stop_id[i]
  ponto <- pontos_usar %>%
    filter(stop_id == ponto_analise) %>%
    st_set_crs(4326) %>%
    distinct(stop_id, .keep_all = T)

  end_viagens_ponto <- paste0(
    local_viagens,
    "viagens_passam_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d.csv"
  )

  end_viagens_ponto_param <- paste0(
    local_viagens,
    "viagens_param_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d.csv"
  )

  if (!mao_dupla) {
    via_prox <- st_nearest_points(via, ponto)

    ponto_via_prox <- st_coordinates(via_prox[1]) %>%
      as.data.frame() %>%
      rename(
        stop_lon = X,
        stop_lat = Y
      ) %>%
      select(-c(L1)) %>%
      left_join(ponto) %>%
      filter(is.na(stop_id)) %>%
      st_drop_geometry() %>%
      rename(
        longitude = stop_lon,
        latitude = stop_lat
      ) %>%
      select(latitude, longitude) %>%
      st_as_sf(coords = c("longitude", "latitude")) %>%
      st_set_crs(4326) %>%
      st_transform(31983) %>%
      st_buffer(4) %>%
      st_transform(4326)

    linhas <- st_intersection(ponto_via_prox, shapes) %>%
      st_drop_geometry() %>%
      distinct(trip_short_name, direction_id)

    linhas_inter <- st_intersection(ponto_via_prox, shapes_inter) %>%
      st_drop_geometry() %>%
      distinct(trip_short_name, direction_id)
  } else {
    linhas <- gtfs$stop_times %>%
      filter(stop_id == ponto_analise) %>%
      left_join(select(gtfs$trips, trip_id, trip_short_name, direction_id)) %>%
      distinct(trip_short_name, direction_id)

    linhas_inter <- gtfs_inter$stop_times %>%
      filter(stop_id == ponto_analise) %>%
      left_join(select(gtfs_inter$trips, trip_id, trip_short_name, direction_id)) %>%
      distinct(trip_short_name, direction_id)
  }

  linhas_sentido <- linhas %>%
    mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
    select(linhas_sentido)

  linhas_sentido_df <- data.frame(
    modo = "sppo",
    linhas = linhas_sentido$linhas_sentido
  )


  if (nrow(linhas_inter != 0)) {
    linhas_sentido_inter <- linhas_inter %>%
      mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
      select(linhas_sentido)

    linhas_sentido_inter_df <- data.frame(
      modo = "inter",
      linhas = linhas_sentido_inter$linhas_sentido
    )

    linhas_sentido_df <- bind_rows(linhas_sentido_df, linhas_sentido_inter_df)
  } else {
    linhas_sentido_df <- linhas_sentido_df
  }

  local_linhas_ponto <- paste0(local_dados, "/linhas_ponto/")
  ifelse(!dir.exists(file.path(getwd(), local_linhas_ponto)),
    dir.create(file.path(getwd(), local_linhas_ponto)), FALSE
  )

  end_linhas_ponto <- paste0(
    local_linhas_ponto,
    "linhas_passam_",
    ponto$stop_id,
    ".csv"
  )

  fwrite(linhas_sentido_df, end_linhas_ponto)

  linhas_param <- gtfs$stop_times %>%
    filter(stop_id == ponto_analise) %>%
    left_join(select(gtfs$trips, trip_id, trip_short_name, direction_id)) %>%
    distinct(trip_short_name, direction_id)

  linhas_inter_param <- gtfs_inter$stop_times %>%
    filter(stop_id == ponto_analise) %>%
    left_join(select(gtfs_inter$trips, trip_id, trip_short_name, direction_id)) %>%
    distinct(trip_short_name, direction_id)

  linhas_sentido_param <- linhas_param %>%
    mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
    select(linhas_sentido)

  linhas_sentido_param <- data.frame(
    modo = "sppo",
    linhas = linhas_sentido_param$linhas_sentido
  )


  if (nrow(linhas_inter_param != 0)) {
    linhas_sentido_inter <- linhas_inter_param %>%
      mutate(linhas_sentido = paste0(trip_short_name, "_", direction_id)) %>%
      select(linhas_sentido)

    linhas_sentido_inter_df <- data.frame(
      modo = "inter",
      linhas = linhas_sentido_inter$linhas_sentido
    )

    linhas_sentido_df <- bind_rows(linhas_sentido_param, linhas_sentido_inter_df)
  } else {
    linhas_sentido_df <- linhas_sentido_param
  }

  end_linhas_ponto_param <- paste0(
    local_linhas_ponto,
    "linhas_param_",
    ponto$stop_id,
    ".csv"
  )

  fwrite(linhas_sentido_df, end_linhas_ponto_param)

  if (file.exists(end_viagens_ponto)) {
    viagens <- fread(end_viagens_ponto)
  } else {
    viagens_ponto <- viagens %>%
      filter(concat_linha_sentido %in% linhas_sentido$linhas)

    viagens_param <- viagens %>%
      filter(concat_linha_sentido %in% linhas_sentido_param$linhas)

    fwrite(viagens_ponto, end_viagens_ponto)
    fwrite(viagens_param, end_viagens_ponto_param)
  }

  local_gps_bruto <- paste0(local_dados, "/gps/bruto/")
  ifelse(!dir.exists(file.path(getwd(), local_gps_bruto)),
    dir.create(file.path(getwd(), local_gps_bruto)), FALSE
  )

  local_gps_proc <- paste0(local_dados, "/gps/proc/")
  ifelse(!dir.exists(file.path(getwd(), local_gps_proc)),
    dir.create(file.path(getwd(), local_gps_proc)), FALSE
  )

  end_gps_ponto_bruto <- paste0(
    local_gps_bruto,
    "gps_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d.csv"
  )

  end_gps_ponto_proc <- paste0(
    local_gps_proc,
    "gps_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d-PROC_param.csv"
  )

  if (!file.exists(end_gps_ponto_proc)) {
    buffer_ponto <- ponto %>%
      st_transform(31983) %>%
      st_buffer(500) %>%
      st_transform(4326)

    gps_ponto <- gps_sf[buffer_ponto, ]

    fwrite(gps_ponto, end_gps_ponto_bruto)

    viagens_ponto <- fread(end_viagens_ponto_param)

    gps_ponto <- gps_ponto %>%
      rename(id_veiculo_viagem = id_veiculo) %>%
      left_join(
        viagens_ponto,
        join_by(
          id_veiculo_viagem == id_veiculo,
          timestamp_gps >= datetime_partida,
          timestamp_gps <= datetime_chegada
        )
      )

    ponto <- ponto %>%
      st_set_crs(4326)

    dist <- st_distance(gps_ponto, ponto) %>%
      as.data.frame() %>%
      rename(dist = 1) %>%
      mutate(dist = as.numeric(dist))

    gps_ponto <- gps_ponto %>%
      bind_cols(., dist) %>%
      group_by(id_viagem) %>%
      slice_min(dist, n = 1) %>%
      distinct(id_viagem, .keep_all = T)

    gps_ponto <- gps_ponto %>%
      filter(!is.na(id_viagem)) %>%
      mutate(fx_hora = lubridate::hour(as.ITime(hora))) %>%
      st_drop_geometry() %>%
      select(
        -c(
          data.y
        )
      ) %>%
      mutate(stop_id = ponto_analise) %>%
      rename(data = data.x)

    fwrite(gps_ponto, end_gps_ponto_proc)
  }

  end_gps_ponto_bruto_passam <- paste0(
    local_gps_bruto,
    "gps_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d_passam.csv"
  )

  end_gps_ponto_proc_passam <- paste0(
    local_gps_proc,
    "gps_",
    ponto_analise,
    "_",
    format(data_ref, "%d-%m-%Y"),
    "_",
    sequencia_dias,
    "d-PROC_passam.csv"
  )

  if (!file.exists(end_gps_ponto_proc_passam)) {
    viagens_ponto <- fread(end_viagens_ponto) %>%
      select(id_veiculo, datetime_partida, datetime_chegada, id_viagem) %>%
      rename(id_veiculo_viagem = id_veiculo)

    gps_ponto <- fread(end_gps_ponto_bruto) %>%
      tidyr::separate(geometry, c("longitude", "latitude"), sep = "\\|") %>%
      st_as_sf(coords = c("longitude", "latitude"), crs = 4326) %>%
      left_join(
        viagens_ponto,
        join_by(
          id_veiculo == id_veiculo_viagem,
          timestamp_gps >= datetime_partida,
          timestamp_gps <= datetime_chegada
        )
      )

    ponto <- ponto %>%
      st_set_crs(4326)

    dist <- st_distance(gps_ponto, ponto) %>%
      as.data.frame() %>%
      rename(dist = 1) %>%
      mutate(dist = as.numeric(dist))

    gps_ponto <- gps_ponto %>%
      bind_cols(., dist) %>%
      group_by(id_viagem) %>%
      slice_min(dist, n = 1) %>%
      distinct(id_viagem, .keep_all = T)

    gps_ponto <- gps_ponto %>%
      filter(!is.na(id_viagem)) %>%
      mutate(fx_hora = lubridate::hour(as.ITime(hora))) %>%
      mutate(stop_id = ponto_analise)

    fwrite(gps_ponto, end_gps_ponto_proc_passam)
  }
  rm(gps_ponto, linhas, linhas_sentido, linhas_lista)
}
