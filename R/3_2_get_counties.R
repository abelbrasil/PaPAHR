
#' Create a dataset with information on all municipalities in Brazil.
#'
#' @description Cria e salva no ambiente global do R o DataFrame `counties`, que contém informações detalhadas sobre os municípios do Brasil.
#'
#' @param state_abbr Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param download Logico. O padrao é FALSE. Se for TRUE, os dados de 'counties' são baixados do site do IBGE. Se for FALSE, os dados são obtidos a partir de uma base de dados incluída no pacote.
#'
#' @export
get_counties <- function(state_abbr, county_id, download = FALSE){
  `%>%` <- dplyr::`%>%`

  if(download==TRUE){
    request_url <- "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"

    #counties é um dataframe que contem todos os municipios do Brasil, e algumas informacoes sobre o municipio
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
            nome_estado = c("microrregiao", "mesorregiao", "UF", "nome"),
            sigla_estado = c("microrregiao", "mesorregiao", "UF", "sigla")) %>%
      dplyr::mutate(id_municipio = stringr::str_sub(id_municipio, 1, 6)) %>%
      dplyr::select(-municipios)

  } else{
    # Lendo o arquivo counties.rds
    caminho_pasta <- system.file("extdata", package = "DATASUS.SIA.SIH")
    caminho_completo <- file.path(caminho_pasta, "counties.rds")

    # Verifique se o arquivo existe
    if (file.exists(caminho_completo)) {
      counties = readRDS(caminho_completo)
    } else {
      cat("O arquivo 'counties.rds' nao foi encontrado no diretório:", caminho_pasta, "\n")
    }
  }
  counties = counties %>% dplyr::select(-'sigla_estado')

  # Atribuir o dataframe ao environment global
  assign("counties", counties, envir = .GlobalEnv)
}
