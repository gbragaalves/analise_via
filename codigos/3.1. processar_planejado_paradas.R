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

gtfs_sppo <- gtfs_sppo %>%
  filter_by_weekday(., tolower(lubridate::wday(
    data_ref, label = T, abbr = F, locale = "English"
  ))) %>%
  filter_by_stop_id(., pontos_usar$stop_id) %>%
  frequencies_to_stop_times(.)

gtfs_sppo <- convert_time_to_seconds(gtfs_sppo, file = "stop_times")

sppo_hora_planejado <- gtfs_sppo$stop_times %>%
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

sppo_hora_planejado_detalhado <- gtfs_sppo$stop_times %>%
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

gtfs_inter <- gtfs_inter %>%
  filter_by_weekday(., tolower(lubridate::wday(
    data_ref, label = T, abbr = F, locale = "English"
  ))) %>%
  filter_by_stop_id(., pontos_usar$stop_id) %>%
  frequencies_to_stop_times(.)

gtfs_inter <- convert_time_to_seconds(gtfs_inter, file = "stop_times")

inter_hora_planejado <- gtfs_inter$stop_times %>%
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

inter_hora_planejado_detalhado <- gtfs_inter$stop_times %>%
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
       paste0(local_planejado, "/sppo_hora_planejado_paradas.csv"))
fwrite(
  sppo_hora_planejado_detalhado,
  paste0(local_planejado, "/sppo_hora_planejado_detalhado_paradas.csv")
)
fwrite(inter_hora_planejado,
       paste0(local_planejado, "/inter_hora_planejado_paradas.csv"))
fwrite(
  inter_hora_planejado_detalhado,
  paste0(local_planejado, "/inter_hora_planejado_detalhado_paradas.csv")
)

