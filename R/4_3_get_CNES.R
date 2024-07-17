
#' get_CNES: Carrega os dados dos estabelecimentos de saude.
#'
#' @description A funcao `get_CNES` cria o DataFrame `health_establishment`, que contem a uniao de todos os arquivos baixados pela funcao `download_cnes_files`.
#'
#' @return Salva o DataFrame `health_establishment` no environment global.
#'
#' @export
get_CNES <- function(){
  `%>%` <- dplyr::`%>%`

  caminho = here::here("data-raw/CNES/ST")
  if (dir.exists(caminho)) {
    dbc_files <- list.files(caminho, pattern = "\\.dbc$", full.names = TRUE)
    if (length(dbc_files) == 27){
      cat("Os arquivos ST(Servicos Temporarios) do CNES nao serao baixados, pois ja estao no diretorio ./data-raw/CNES/ST.\n")
    } else if(length(dbc_files)>=1) {
      stop("A pasta ./data-raw/CNES/ST nao contem todos os 27 arquivos DBC do ST(Servicos Temporarios) do CNES")
    } else {
      stop("A pasta ./data-raw/CNES/ST nao contem nenhum dos 27 arquivos DBC do ST(Servicos Temporarios) do CNES")
    }

  } else {
    #Realizando a primeira chamada da função download_cnes_files
    download_cnes_files(newer = TRUE)
  }

  health_establishment <-
    here::here("data-raw", "CNES", "ST") %>%
    list.files(full.names = TRUE) %>%
    purrr::map_dfr(read.dbc::read.dbc, as.is=TRUE)


  caminho2 = here::here("data-raw/CNES/CADGER")
  if (dir.exists(caminho2)) {
    unlink(caminho2, recursive = TRUE)
  }
  output_dir <- here::here("data-raw", "CNES", "CADGER")
  dir.create(output_dir)

  #Movendo o arquivo do CADGER para a pasta "data-raw/CNES/CADGER"

  caminho_pasta <- system.file("extdata", package = "DATASUS.SIA.SIH")
  caminho_completo <- file.path(caminho_pasta, "CADGER-BR.rds")
  dir_destino <- "./data-raw/CNES/CADGER"

  # Verifique se o arquivo existe
  if (file.exists(caminho_completo)) {
    novo_caminho_completo <- file.path(dir_destino, "CADGER-BR.rds")
    file.copy(caminho_completo, novo_caminho_completo)
  } else {
    cat("O arquivo 'CADGER-BR.rds' nao foi encontrado no diretório:", caminho_pasta, "\n")}

  #Le o arquivo rds do diretorio "data-raw/CNES/CADGER"
  health_establishment_details <-
    here::here("data-raw", "CNES", "CADGER") %>%
    list.files(full.names = TRUE) %>%
    purrr::map_dfr(readRDS)

  health_establishment <- health_establishment %>%
    dplyr::left_join(health_establishment_details, by="CNES") %>%
    dplyr::select(CNES, FANTASIA, COMPETEN) %>%
    dplyr::distinct(CNES, .keep_all = TRUE) %>%
    dplyr::mutate(NO_ESTABELECIMENTO = stringr::str_c(CNES, FANTASIA, sep="-")) %>%
    tibble::as_tibble()

  # Atribuir os dataframes ao environment global
  env <- globalenv()
  dataframes <- list(health_establishment = health_establishment)
  list2env(dataframes, envir = env)
}
