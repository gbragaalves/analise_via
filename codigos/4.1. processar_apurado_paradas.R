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

data_ref <- as.Date('2023-05-09')

sequencia_dias <- 3

local_gps_proc <- paste0(local_dados, "/gps/proc/")

nm <- list.files(path=local_gps_proc, full.names = T, pattern = "*PROC_param.csv",recursive = TRUE)

gps <- do.call(rbind, lapply(nm, function(x) fread(file=x)))

sppo_hora_aferido <- gps %>%
  group_by(stop_id,fx_hora) %>%
  summarise(sppo_hora_aferido = round(n()/sequencia_dias)) %>%
  arrange(fx_hora) %>%
  st_drop_geometry() %>%
  pivot_wider(names_from = stop_id,values_from = sppo_hora_aferido)

sppo_hora_aferido_detalhado <- gps %>%
  group_by(stop_id,fx_hora, servico) %>%
  summarise(veiculos_hora_aferido = round(n()/sequencia_dias)) %>%
  arrange(fx_hora) %>%
  st_drop_geometry()

local_frequencias <- paste0(local_dados, "/frequencias/")
ifelse(!dir.exists(file.path(getwd(), local_frequencias)),
       dir.create(file.path(getwd(), local_frequencias)), FALSE)

local_aferido <- paste0(local_frequencias, "/aferido/")
ifelse(!dir.exists(file.path(getwd(), local_aferido)),
       dir.create(file.path(getwd(), local_aferido)), FALSE)

fwrite(sppo_hora_aferido,
       paste0(local_aferido, "/sppo_hora_aferido_parada.csv"))
fwrite(
  sppo_hora_aferido_detalhado,
  paste0(local_aferido, "/sppo_hora_aferido_detalhado_parada.csv")
)

