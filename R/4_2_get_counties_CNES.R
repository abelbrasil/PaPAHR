
#' get_counties: Obtem informacoes de todos os estados do Brasil
#'
#' @description A funcao `get_counties` cria dois DataFrames. O primeiro, `counties`, contem informacoes detalhadas sobre os municipios do Brasil. O segundo, `health_establishment`, e a uniao de todos os arquivos baixados pela funcao `download_cnes_files`.
#'
#' @return A funcao nao retorna valores diretamente, mas salva no ambiente global do R os DataFrames `counties` e `health_establishment`.
#'
#' @export
get_counties <- function(){
  `%>%` <- dplyr::`%>%`

  request_url <- "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

  #counties e um dataframe que contem todos os municipios do Brasil, e algumas informacoes sobre o municipio
  counties <-
    request_url %>%
    httr::GET(httr::timeout(60)) %>%
    httr::content() %>%
    tibble::as_tibble_col(column_name="municipios") %>%
    tidyr::hoist(municipios,
          id_municipio = "id",
          nome_municipio = "nome",
          nome_microrregiao = c("microrregiao", "nome"),
          nome_mesorregiao = c("microrregiao", "mesorregiao", "nome"),
          id_estado = c("microrregiao", "mesorregiao", "UF", "id"),
          nome_estado = c("microrregiao", "mesorregiao", "UF", "nome")) %>%
    dplyr::mutate(id_municipio = stringr::str_sub(id_municipio, 1, 6)) %>%
    dplyr::select(-municipios)


  #Realizando a primeira chamada da função download_cnes_files
  download_cnes_files(newer = TRUE)

  health_establishment <-
    here::here("data-raw", "CNES", "ST") %>%
    list.files(full.names = TRUE) %>%
    purrr::map_dfr(read.dbc::read.dbc, as.is=TRUE)


  #Movendo os arquivos do CADGER para a pasta "data-raw/CNES/CADGER"
  #------------------------------------------------------------------------
  output_dir <- here::here("data-raw", "CNES", "CADGER")
  dir.create(output_dir)

  caminho_pasta <- system.file("extdata", package = "DATASUS.SIA.SIH")
  caminho_completo <- file.path(caminho_pasta, "CADGER.zip")
  dir_destino <- "./data-raw/CNES/CADGER"

  # Verifique se o arquivo existe
  if (file.exists(caminho_completo)) {
    novo_caminho_completo <- file.path(dir_destino, "CADGER.zip")
    file.copy(caminho_completo, novo_caminho_completo)
    #cat("Arquivos CADGER movido com sucesso para:", novo_caminho_completo, "\n")
  } else {cat("O arquivo 'CADGER.zip' nao foi encontrado no diretório:", caminho_pasta, "\n")}

  #Extraindo arquivo CADGER.zip
  unzip('./data-raw/CNES/CADGER/CADGER.zip', exdir = './data-raw/CNES/CADGER')

  if (file.exists("./data-raw/CNES/CADGER/CADGER.zip")) {
    file.remove('./data-raw/CNES/CADGER/CADGER.zip')
  }
  #------------------------------------------------------------------------

  #Le todos os arquivos DBC do diretorio "data-raw/CNES/CADGER" e combina-os em uma tabela unica.
  health_establishment_details <-
    here::here("data-raw", "CNES", "CADGER") %>%
    list.files(full.names = TRUE) %>%
    purrr::map_dfr(foreign::read.dbf, as.is=TRUE)

  health_establishment <- health_establishment %>%
    dplyr::left_join(health_establishment_details, by="CNES") %>%
    dplyr::select(CNES, FANTASIA, COMPETEN) %>%
    dplyr::mutate(NO_ESTABELECIMENTO = stringr::str_c(CNES, FANTASIA, sep="-")) %>%
    tibble::as_tibble()


  # Atribuir os dataframes ao environment global
  env <- globalenv()
  dataframes <- list(counties = counties, health_establishment = health_establishment)
  list2env(dataframes, envir = env)
}
