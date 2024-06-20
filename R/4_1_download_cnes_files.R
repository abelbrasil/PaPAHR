
#' download_cnes_files: Faz o download dos dados do CNES
#'
#' @description A funcao `download_cnes_files` baixa arquivos do CNES (Cadastro Nacional de Estabelecimentos de Saude) e os salva temporariamente na maquina local. Esses arquivos podem ser filtrados pela unidade da federacao.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param newer Logico. O padrao e TRUE. Se for TRUE e os outros parametros forem nulos, obtem o arquivo do mes mais recente disponivel no SIGTAP.
#' @param state_abbr String. O padrao e "all". Sigla da Unidade Federativa
#'
#' @export
#'
#' @examples
#' \dontrun{download_cnes_files(newer = TRUE)}
download_cnes_files <-
  function(year_start, month_start, year_end, month_end, newer, state_abbr="all"){
    `%>%` <- dplyr::`%>%`

    base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/ST/"


    output_dir <- here::here("data-raw")
    if (!dir.exists(output_dir)){
      dir.create(output_dir)
    }
    output_dir <- here::here("data-raw", "CNES")
    if (!dir.exists(output_dir)){
      dir.create(output_dir)
    }
    output_dir <- here::here("data-raw", "CNES", "ST")
    dir.create(output_dir)

    #Os dados do CADGER nao estao acessiveis atraves da URL abaixo. Para solucionar esse problema, o pacote ja incorpora esses valores e, quando e chamado, ele os copia para a pasta \data-raw\CNES\CADGER.

    #base_url = "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Auxiliar/"
    #output_dir <- here("data-raw", "CNES", "CADGER")
    #dir.create(output_dir)

    connection <- curl::curl(base_url)

    dir_files <-
      connection %>%
      readLines() %>%
      stringr::str_sub(start=-12) %>%
      tibble::as_tibble_col(column_name = "file_name") %>%
      dplyr::mutate(state = stringr::str_sub(file_name, 3, 4),
             publication_date = lubridate::ym(stringr::str_sub(file_name, 5, 8)),
             format_file = stringr::str_sub(file_name, -3)) %>%
      dplyr::filter(format_file == "dbc")

    close(connection)

    #Se a condição "if" for verdadeira, o código irá selecionar o arquivo mais recente localizado dentro de "dir_files".
    if (newer == TRUE &
        missing(year_start) &
        missing(month_start) &
        missing(year_end) &
        missing(month_end))
    {
      dir_files <- dir_files %>% dplyr::filter(publication_date == max(publication_date))

    } else {
      publication_date_start <- lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
      publication_date_end <- lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))

      dir_files <- dir_files %>%
        dplyr::filter(publication_date >= publication_date_start & publication_date <= publication_date_end)
    }

    if (state_abbr != "all") {
      dir_files <- dir_files %>%
        dplyr::filter(state == state_abbr)
    }

    files_name <- dplyr::pull(dir_files, file_name)
    download_files_url <- stringr::str_glue("{base_url}{files_name}")
    output_files_path <- stringr::str_glue("{output_dir}/{files_name}")

    purrr::walk2(download_files_url, output_files_path, curl::curl_download)
  }
