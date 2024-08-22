
#' Loads data from the National Registry of Health Establishments (CNES)
#'
#' @description A função cria e salva no ambiente global do R o DataFrame `health_establishment`, que contém a união de todos os arquivos baixados pela função `download_cnes_files.`
#'
#' @export
get_CNES <- function(){
  `%>%` <- dplyr::`%>%`

  files_path = here::here("data-raw/CNES/ST")
  if (dir.exists(files_path)) {
    dbc_files <- list.files(files_path, pattern = "\\.dbc$", full.names = TRUE)
    if (length(dbc_files) == 27){
      #cat("Os arquivos ST(Servicos Temporarios) do CNES nao serao baixados, pois ja estao no diretorio ./data-raw/CNES/ST.\n")
    } else {
      stop("A pasta ./data-raw/CNES/ST nao contem todos os 27 arquivos DBC do ST(Servicos Temporarios) do CNES.\n Exclua a pasta ./data-raw e execute o código novamente. ")
    }
  } else {
    #Realizando a primeira chamada da função download_cnes_files
    download_cnes_files(newer = TRUE)
  }

  health_establishment <-
    here::here("data-raw", "CNES", "ST") %>%
    list.files(full.names = TRUE) %>%
    purrr::map_dfr(read.dbc::read.dbc, as.is=TRUE)

  files_path = here::here("data-raw/CNES/CADGER")
  if (dir.exists(files_path)) {
    unlink(files_path, recursive = TRUE)
  }

  output_dir <- here::here("data-raw", "CNES", "CADGER")
  dir.create(output_dir)

  # Movendo o arquivo do CADGER para a pasta "data-raw/CNES/CADGER"
  caminho_pasta <- system.file("extdata", package = "FaturaSUS.AmbHosp")
  caminho_completo <- file.path(caminho_pasta, "CADGER-BR.rds")
  dir_destino <- "./data-raw/CNES/CADGER"

  # Verifique se o arquivo existe
  if (file.exists(caminho_completo)) {
    novo_caminho_completo <- file.path(dir_destino, "CADGER-BR.rds")
    file.copy(caminho_completo, novo_caminho_completo)
  } else {
    cat("O arquivo 'CADGER-BR.rds' nao foi encontrado no diretório:", caminho_pasta, "\n")
  }

  health_establishment_details <-
    here::here("data-raw", "CNES", "CADGER") %>%
    list.files(full.names = TRUE) %>%
    purrr::keep(~ stringr::str_detect(.x, "\\.rds$")) %>%
    purrr::map_dfr(readRDS)

  health_establishment <- health_establishment %>%
    dplyr::left_join(health_establishment_details, by="CNES") %>%
    dplyr::select(CNES, FANTASIA, COMPETEN) %>%
    dplyr::distinct(CNES, .keep_all = TRUE) %>%
    dplyr::mutate(NO_ESTABELECIMENTO = stringr::str_c(CNES, FANTASIA, sep="-")) %>%
    tibble::as_tibble()

  # Atribuir o dataframe ao environment global
  assign("health_establishment", health_establishment, envir = .GlobalEnv)
}
