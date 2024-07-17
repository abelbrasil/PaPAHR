
#' get_counties: Obtem informacoes de todos os estados do Brasil
#'
#' @description A funcao `get_counties` cria o DataFrame `counties`, que contem informacoes detalhadas sobre os municipios do Brasil.
#'
#' @param state_abbr Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#'
#' @return Salva o DataFrame `counties` no environment global .
#'
#' @export
get_counties <- function(state_abbr, county_id){
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
          nome_estado = c("microrregiao", "mesorregiao", "UF", "nome"),
          sigla_estado = c("microrregiao", "mesorregiao", "UF", "sigla")) %>%
    dplyr::mutate(id_municipio = stringr::str_sub(id_municipio, 1, 6)) %>%
    dplyr::select(-municipios)

  if (!any(counties$id_municipio == county_id & counties$sigla_estado == state_abbr)){
    stop('O ID do município informado não pertence ao estado informado.')
  }
  counties = counties %>% dplyr::select(-'sigla_estado')

  # Atribuir os dataframes ao environment global
  env <- globalenv()
  dataframes <- list(counties = counties)
  list2env(dataframes, envir = env)
}
