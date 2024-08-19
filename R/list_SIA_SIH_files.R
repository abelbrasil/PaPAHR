
#' Returns the names of the DBC files that need to be downloaded.
#'
#' @description A função retorna um DataFrame com os nomes dos arquivos (PA, RD, RJ ou SP) para cada mês que precisam ser baixados.
#'
#' @param data_source String. Valores aceitos "SIA", "SIH"
#' @param data_type String. Valores aceitos "PA", "RD", "RJ", "SP"
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param publication_date_start Uma string no formato "AAAA-MM-01", indicando o mes de inicio para o download dos dados.
#' @param publication_date_end Uma string no formato "AAAA-MM-01", indicando o mes de termino para o download dos dados.
#'
#' @return dir_files. Um DataFrame contendo o nome do arquivo, a data associada ao arquivo, a UF (Unidade Federativa) do arquivo e o tipo do arquivo (PA, RD, RJ ou SP).
#'
#' @examples
#' \dontrun{
#'   list_SIA_SIH_files(
#'     data_source = "SIA",
#'     data_type = "PA",
#'     state_abbr = "CE",
#'     publication_date_start = "2023-01-01",
#'     publication_date_end = "2023-03-01"
#'   )
#' }
#'
#' @export
list_SIA_SIH_files <-
  function(data_source,
           data_type,
           state_abbr,
           publication_date_start,
           publication_date_end) {

    `%>%` <- dplyr::`%>%`
    data_source = stringr::str_sub(data_source, 1, 3)

    base_url <- stringr::str_glue(
      "ftp://ftp.datasus.gov.br/dissemin/publicos/{data_source}SUS/200801_/Dados/")

    connection <- curl::curl(base_url)

    #$ Listar os arquivos (PA, RD, RJ ou SP) do DATASUS
    dir_files <-
      connection %>%
      readLines() %>%
      stringr::str_sub(start = -13) %>%
      tibble::as_tibble_col(column_name = "file_name") %>%
      dplyr::mutate(
        file_name = stringr::str_trim(file_name, side = "left"),
        file_name = paste0(toupper(substr(file_name, 1, 4)), substr(file_name, 5, nchar(file_name))),
        state = stringr::str_sub(file_name, 3, 4),
        publication_date = lubridate::ym(stringr::str_sub(file_name, 5, 8), quiet = TRUE),
        file_type = stringr::str_sub(file_name, 1, 2)
      ) %>%
      dplyr::filter(
        file_type %in% data_type,
        state %in% state_abbr,
        publication_date >= publication_date_start,
        publication_date <= publication_date_end
      )

    close(connection)

    return(dir_files)

  }
