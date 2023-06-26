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
  'tidyr'
))

local_dados <- paste0("./corredores/", nome_corredor)
local_frequencias <- paste0(local_dados, "/frequencias/")
local_planejado <- paste0(local_frequencias, "/planejado/")
local_aferido <- paste0(local_frequencias, "/aferido/")

sppo_hora_aferido_detalhado_parada <-
  fread(paste0(local_aferido, "/sppo_hora_aferido_detalhado_parada.csv"))

sppo_hora_aferido_detalhado_passagem <-
  fread(paste0(local_aferido, "/sppo_hora_aferido_detalhado_passagem.csv"))

if(file.exists(paste0(local_planejado, "/sppo_hora_planejado_detalhado_paradas.csv"))){
  sppo_hora_planejado_detalhado_parada <-
    fread(paste0(local_planejado, "/sppo_hora_planejado_detalhado_paradas.csv"))
  
  sppo_hora_planejado_detalhado_passagem<-
    fread(paste0(local_planejado, "/sppo_hora_planejado_detalhado_passagem.csv"))
}

if(file.exists(paste0(local_planejado, "/inter_hora_planejado_detalhado_paradas.csv"))){
  inter_hora_planejado_detalhado_parada <-
    fread(paste0(local_planejado, "/inter_hora_planejado_detalhado_paradas.csv"))
  
  inter_hora_planejado_detalhado_passagem<-
    fread(paste0(local_planejado, "/inter_hora_planejado_detalhado_passagem.csv"))
}

pontos_usar <- st_read(paste0(local_dados, "/pontos_usar.gpkg"))

hora_pico <- sppo_hora_aferido_detalhado_passagem %>%
  group_by(fx_hora, stop_id) %>%
  summarise(freq = sum(veiculos_hora_aferido)) %>%
  group_by(fx_hora) %>%
  summarise(freq_media = mean(freq)) %>%
  slice_max(freq_media, n = 1) %>%
  slice_min(fx_hora) %>% 
  select(fx_hora) %>%
  unlist()

if(exists("sppo_hora_planejado_detalhado_passagem")){
  qt_linhas_passagem <- sppo_hora_planejado_detalhado_passagem %>%
    group_by(stop_id) %>%
    summarise(qt_linhas_passagem = length(unique(trip_short_name)))
  
  qt_linhas_parada <- sppo_hora_planejado_detalhado_parada %>%
    group_by(stop_id) %>%
    summarise(qt_linhas_parada = length(unique(trip_short_name)))
}

frequ_aferido_pico_sppo_parada <- sppo_hora_aferido_detalhado_parada %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_aferido_pico_sppo_parada = sum(veiculos_hora_aferido))

frequ_aferido_pico_sppo_passagem <- sppo_hora_aferido_detalhado_passagem %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_aferido_pico_sppo_passagem = sum(veiculos_hora_aferido))

if(exists("sppo_hora_planejado_detalhado_parada")){

frequ_plan_pico_sppo_parada <- sppo_hora_planejado_detalhado_parada %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_plan_pico_sppo_parada = sum(sppo_hora_planejado))


frequ_plan_pico_sppo_passagem <- sppo_hora_planejado_detalhado_passagem %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_plan_pico_sppo_passagem = sum(sppo_hora_planejado))

}

if(exists("inter_hora_planejado_detalhado_parada")){
  
frequ_plan_pico_inter_parada <- inter_hora_planejado_detalhado_parada %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_plan_pico_inter_parada = sum(inter_hora_planejado))

frequ_plan_pico_inter_passagem <- inter_hora_planejado_detalhado_passagem %>%
  filter(fx_hora == hora_pico) %>%
  group_by(stop_id) %>%
  summarise(freq_plan_pico_inter_passagem = sum(inter_hora_planejado))

}

resumo_pontos <- pontos_usar %>%
  st_drop_geometry()


resumo_pontos <- resumo_pontos %>%
  select(stop_id, stop_name, stop_sequence) %>%
  arrange(stop_sequence)


if(!exists("qt_linhas_parada")){
  qt_linhas_parada <- data.frame()
  qt_linhas_passagem <- data.frame()
}

if(!exists("frequ_plan_pico_sppo_parada")){
  frequ_plan_pico_sppo_parada <- data.frame()
  frequ_plan_pico_sppo_passagem <- data.frame()
}

if(!exists("frequ_plan_pico_inter_parada")){
  frequ_plan_pico_inter_parada <- data.frame()
  frequ_plan_pico_inter_passagem <- data.frame()
}

resumo_pontos <- resumo_pontos %>%
  {
    if (nrow(qt_linhas_parada) > 0)
      left_join(.,qt_linhas_parada)
    else
      .
  } %>%
  {
    if (nrow(qt_linhas_passagem) > 0)
      left_join(.,qt_linhas_passagem)
    else
      .
  } %>%
  left_join(frequ_aferido_pico_sppo_parada) %>%
  left_join(frequ_aferido_pico_sppo_passagem) %>%
  {
    if (nrow(frequ_plan_pico_sppo_parada) > 0)
      left_join(.,frequ_plan_pico_sppo_parada)
    else
      .
  } %>% 
  {
    if (nrow(frequ_plan_pico_sppo_passagem) > 0)
      left_join(.,frequ_plan_pico_sppo_passagem)
    else
      .
  } %>% 
  {
    if (nrow(frequ_plan_pico_inter_parada) > 0)
      left_join(., frequ_plan_pico_inter_parada)
    else
      .
  } %>%
  {
    if (nrow(frequ_plan_pico_inter_passagem) > 0)
      left_join(., frequ_plan_pico_inter_passagem)
    else
      .
  } %>%
  mutate_all(~ ifelse(is.na(.), 0, .)) %>%
  mutate(freq_plan_total_parada = if (all(
    c("freq_plan_pico_sppo_parada", "freq_plan_pico_inter_parada") %in% names(.)
  ))
    freq_plan_pico_sppo_parada + freq_plan_pico_inter_parada
  else
    NA) %>%
  mutate(freq_plan_total_passagem = if (all(
    c("freq_plan_pico_sppo_passagem", "freq_plan_pico_inter_passagem") %in% names(.)
  ))
    freq_plan_pico_sppo_passagem + freq_plan_pico_inter_passagem
  else
    NA)

media_aferido_fx_hora_parada <- sppo_hora_aferido_detalhado_parada %>%
  group_by(fx_hora, stop_id) %>%
  summarise(veiculos = sum(veiculos_hora_aferido)) %>%
  pivot_wider(names_from = stop_id,
              values_from = veiculos,
              values_fill = 0) |>
  mutate(freq_aferida_parada = round(rowMeans(across(2:n(
  )), na.rm = T))) |>
  select(fx_hora, freq_aferida_parada)

media_aferido_fx_hora_passagem <- sppo_hora_aferido_detalhado_passagem %>%
  group_by(fx_hora, stop_id) %>%
  summarise(veiculos = sum(veiculos_hora_aferido)) %>%
  pivot_wider(names_from = stop_id,
              values_from = veiculos,
              values_fill = 0) |>
  mutate(freq_aferida_passagem = round(rowMeans(across(2:n(
  )), na.rm = T))) |>
  select(fx_hora, freq_aferida_passagem)

if(exists("sppo_hora_planejado_detalhado_parada")){
  if(nrow(sppo_hora_planejado_detalhado_parada)>0){
    media_planejado_fx_hora_parada <- sppo_hora_planejado_detalhado_parada %>%
      group_by(fx_hora, stop_id) %>%
      summarise(veiculos = sum(sppo_hora_planejado)) %>%
      pivot_wider(names_from = stop_id,
                  values_from = veiculos,
                  values_fill = 0) |>
      mutate(freq_planejada_sppo_parada = round(rowMeans(across(2:n(
      )), na.rm = T))) |>
      select(fx_hora, freq_planejada_sppo_parada)
  }
}

if(exists("sppo_hora_planejado_detalhado_passagem")){
  if(nrow(sppo_hora_planejado_detalhado_passagem)>0){
    media_planejado_fx_hora_passagem <- sppo_hora_planejado_detalhado_passagem %>%
      group_by(fx_hora, stop_id) %>%
      summarise(veiculos = sum(sppo_hora_planejado)) %>%
      pivot_wider(names_from = stop_id,
                  values_from = veiculos,
                  values_fill = 0) |>
      mutate(freq_planejada_sppo_passagem = round(rowMeans(across(2:n(
      )), na.rm = T))) |>
      select(fx_hora, freq_planejada_sppo_passagem)
  }
}

resumo_media_corredor <- data.frame(fx_hora = c(0:23)) %>% 
  left_join(media_aferido_fx_hora_parada) %>% 
  left_join(media_aferido_fx_hora_passagem) %>% 
  {
    if (exists("media_planejado_fx_hora_parada") > 0)
      left_join(., media_planejado_fx_hora_parada)
    else
      .
  } %>% 
  {
    if (exists("media_planejado_fx_hora_passagem") > 0)
      left_join(., media_planejado_fx_hora_passagem)
    else
      .
  } %>% 
  mutate_all( ~ ifelse(is.na(.), 0, .))

if(exists("inter_hora_planejado_detalhado_parada")){
  if (nrow(inter_hora_planejado_detalhado_parada) > 0) {
    inter_planejado_fx_hora_parada <- inter_hora_planejado_detalhado_parada %>%
      group_by(fx_hora, stop_id) %>%
      summarise(veiculos = sum(inter_hora_planejado)) %>%
      pivot_wider(
        names_from = stop_id,
        values_from = veiculos,
        values_fill = 0
      ) |>
      mutate(freq_planejada_inter_parada = round(rowMeans(across(2:n(
      )), na.rm = T))) |>
      select(fx_hora, freq_planejada_inter_parada)
    
    inter_planejado_fx_hora_passagem <- inter_hora_planejado_detalhado_passagem %>%
      group_by(fx_hora, stop_id) %>%
      summarise(veiculos = sum(inter_hora_planejado)) %>%
      pivot_wider(
        names_from = stop_id,
        values_from = veiculos,
        values_fill = 0
      ) |>
      mutate(freq_planejada_inter_passagem = round(rowMeans(across(2:n(
      )), na.rm = T))) |>
      select(fx_hora, freq_planejada_inter_passagem)
    
    resumo_media_corredor <- resumo_media_corredor |>
      left_join(inter_planejado_fx_hora_parada) |>
      left_join(inter_planejado_fx_hora_passagem) |>
      mutate_all( ~ ifelse(is.na(.), 0, .)) |>
      mutate(freq_plan_total_parada = freq_planejada_sppo_parada + freq_planejada_inter_parada) %>% 
      mutate(freq_plan_total_passagem = freq_planejada_sppo_passagem + freq_planejada_inter_passagem)
  }
}

ponto_mais_carregado <- resumo_pontos %>%
  slice_max(freq_aferido_pico_sppo_passagem)

ponto_mais_carregado <- ponto_mais_carregado %>%
  {
    if ("freq_plan_total_passagem" %in% colnames(ponto_mais_carregado))
      ponto_mais_carregado %>% slice_max(freq_plan_total_passagem)
    else
      .
  } %>%
  {
    if ("freq_plan_pico_sppo_passagem" %in% colnames(ponto_mais_carregado))
      ponto_mais_carregado %>% slice_max(freq_plan_pico_sppo_passagem)
    else
      .
  } %>%
  {
    if ("qt_linhas_passagem" %in% colnames(ponto_mais_carregado))
      ponto_mais_carregado %>% slice_max(qt_linhas_passagem)
    else
      .
  } %>%
  {
    if ("stop_sequence" %in% colnames(ponto_mais_carregado))
      ponto_mais_carregado %>% slice_min(stop_sequence)
    else
      .
  } %>%
  select(stop_id) %>%
  unlist() %>%
  as.character()

mais_carregado_aferido <- sppo_hora_aferido_detalhado_passagem %>%
  group_by(fx_hora, stop_id) %>%
  summarise(veiculos = sum(veiculos_hora_aferido)) %>%
  pivot_wider(names_from = stop_id,
              values_from = veiculos,
              values_fill = 0) %>%
  select(fx_hora, {
    {
      ponto_mais_carregado
    }
  }) %>%
  rename(freq_aferida_sppo = {
    {
      ponto_mais_carregado
    }
  })

if(exists("sppo_hora_planejado_detalhado_passagem")){
  if(nrow(sppo_hora_planejado_detalhado_passagem)>0){
    mais_carregado_planejado <- sppo_hora_planejado_detalhado_passagem %>%
      group_by(fx_hora, stop_id) %>%
      summarise(veiculos = sum(sppo_hora_planejado)) %>%
      pivot_wider(names_from = stop_id,
                  values_from = veiculos,
                  values_fill = 0) |>
      select(fx_hora, {
        {
          ponto_mais_carregado
        }
      }) %>%
      rename(freq_planejada_sppo = {
        {
          ponto_mais_carregado
        }
      })
    
    mais_carregado_tabela <- data.frame(fx_hora = c(0:23)) |>
      left_join(mais_carregado_aferido) |>
      left_join(mais_carregado_planejado) |>
      mutate_all( ~ ifelse(is.na(.), 0, .))
  }
}

if(exists("inter_hora_planejado_detalhado_passagem")){
  if (nrow(inter_hora_planejado_detalhado_passagem) > 0) {
    if (ponto_mais_carregado %in% inter_hora_planejado_detalhado_passagem$stop_id) {
      mais_carregado_inter <- inter_hora_planejado_detalhado_passagem %>%
        group_by(fx_hora, stop_id) %>%
        summarise(veiculos = sum(inter_hora_planejado)) %>%
        pivot_wider(
          names_from = stop_id,
          values_from = veiculos,
          values_fill = 0
        ) |>
        select(fx_hora, {
          {
            ponto_mais_carregado
          }
        }) %>%
        rename(freq_planejada_inter = {
          {
            ponto_mais_carregado
          }
        })
      
      mais_carregado_tabela <- mais_carregado_tabela |>
        left_join(mais_carregado_inter) |>
        mutate_all( ~ ifelse(is.na(.), 0, .)) |>
        mutate(freq_plan_total = freq_planejada_sppo + freq_planejada_inter)
    }
  }
}

local_resultados <- paste0(local_dados, "/resultados/")
ifelse(!dir.exists(file.path(getwd(), local_resultados)),
       dir.create(file.path(getwd(), local_resultados)), FALSE)
local_relatorios <- paste0(local_resultados, "/relatorios/")
ifelse(!dir.exists(file.path(getwd(), local_relatorios)),
       dir.create(file.path(getwd(), local_relatorios)), FALSE)

local_resumo_pontos <-
  paste0(local_relatorios, "/", nome_corredor, "_resumo_pontos.csv")
local_media_corredor <-
  paste0(local_relatorios, "/", nome_corredor, "_media_corredor.csv")
local_ponto_mais_carregado <-
  paste0(local_relatorios,
         "/",
         nome_corredor,
         "_ponto_mais_carregado.csv")

fwrite(resumo_pontos, local_resumo_pontos)
fwrite(resumo_media_corredor, local_media_corredor)
if(exists("mais_carregado_tabela")){
  fwrite(mais_carregado_tabela, local_ponto_mais_carregado)
}

resumo <-
  fread(paste0("./corredores/", nome_corredor, "/resumo.csv")) %>%
  mutate(mais_carregado = ponto_mais_carregado)

fwrite(resumo, paste0("./corredores/", nome_corredor, "/resumo.csv"))


pico_tarde <- sppo_hora_aferido_detalhado_passagem %>% 
  filter(fx_hora %in% c('17','18'))

fwrite(pico_tarde,"./pico_tarde.csv")

