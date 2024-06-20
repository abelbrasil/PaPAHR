
#' download_sigtap_files: Baixa dados .dbc do site DataSUS
#'
#' @description A funcao `download_sigtap_files` baixar e descompactar arquivos sigtap do site datasus. Esses arquivos são salvos temporariamente na maquina local. Para cada mes vai ser baixado um arquivo compactado.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param newer Logico. O padrao e TRUE. Se for TRUE e os outros parametros forem nulos, obtem o arquivo do mes mais recente disponivel no SIGTAP.
#'
#' @export
#'
#' @examples
#' \dontrun{download_sigtap_files(2024, 1, 2024, 3, newer=FALSE)}
#'
download_sigtap_files <- function(year_start, month_start, year_end, month_end, newer=TRUE){
  `%>%` <- dplyr::`%>%`

  output_dir <- stringr::str_c(tempdir(), "SIGTAP", sep="\\")
  if (!dir.exists(output_dir)){
    dir.create(output_dir)
  }

  base_url <- "ftp://ftp2.datasus.gov.br/pub/sistemas/tup/downloads/"
  connection <- curl::curl(base_url)

  dir_files <-
    connection %>%
    readLines() %>%
    stringr::str_subset("TabelaUnificada*") %>%
    stringr::str_sub(57) %>%
    tibble::as_tibble_col(column_name="file_name") %>%
    dplyr::mutate(file_version_id = stringr::str_sub(file_name, 17, 22),
           publication_date = lubridate::ym(file_version_id))

  close(connection)

  #Se a condição "if" for verdadeira, o código irá selecionar o arquivo mais recente localizado dentro de "dir_files".
  if (newer == TRUE &
    missing(year_start) &
    missing(month_start) &
    missing(year_end) &
    missing(month_end)
  ){
    dir_files <- dir_files %>% dplyr::filter(publication_date == max(publication_date))

  }else{
    publication_date_start <- lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
    publication_date_end <- lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))
    dir_files <- dir_files %>%
      dplyr::filter(publication_date >= publication_date_start, publication_date <= publication_date_end)
  }

  file_version_id <- dplyr::pull(dir_files, file_version_id)

  purrr::walk(stringr::str_glue("{output_dir}/{file_version_id}"), dir.create)

  files_name <- dplyr::pull(dir_files, file_name)

  download_files_url <- stringr::str_glue("{base_url}{files_name}")
  output_files_path <- stringr::str_glue("{output_dir}/{file_version_id}/{files_name}")
  output_zip_files_path <- stringr::str_glue("{output_dir}/{file_version_id}")

  purrr::walk2(download_files_url, output_files_path, curl::curl_download)

  purrr::walk2(output_files_path, output_zip_files_path, ~ utils::unzip(.x, exdir=.y))

}

#Para ver o diretorio temporario onde foi salvo os arquivos
#output_dir <- stringr::str_c(tempdir(), "SIGTAP", sep="\\")
#list.files(output_dir)


