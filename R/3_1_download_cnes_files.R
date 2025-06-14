
#' Download data from the National Registry of Health Establishments (CNES)
#'
#' @description A função `download_cnes_files` baixa arquivos do CNES (Cadastro Nacional de Estabelecimentos de Saúde) e os armazena temporariamente na máquina local.
#'
#' @param year_start numeric. Ano inicial para o download dos dados, no formato yyyy.
#' @param month_start numeric. Mês inicial para o download dos dados, no formato mm.
#' @param year_end numeric. Ano final para o download dos dados, no formato yyyy.
#' @param month_end numeric. Mês final para o download dos dados, no formato mm.
#' @param newer logical. O padrão é TRUE. Se for TRUE e os outros parâmetros forem nulos, obtém o arquivo do mês mais recente disponível no CNES.
#' @param state_abbr string. O padrao e "all". Sigla da Unidade Federativa.
#' @param type_data string. O padrao e "ST". Valores aceitos ("ST", "LT", "HB", "EQ"). O tipo da base de dados do CNES que sera baixado.
#'
#'@examples
#' \dontrun{download_cnes_files(newer = TRUE)}
#'
#' @export
download_cnes_files <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           newer,
           state_abbr = "all",
           type_data = "ST"){

    `%>%` <- dplyr::`%>%`

    type_data = toupper(trimws(type_data))
    if ( !(type_data %in% c("ST","LT","HB","EQ")) ){
      stop("O valor passado no parametro type_data não é aceito.\n")
    }
    if (type_data=="ST"){
      base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/ST/"
      type = "ST"
    }else if(type_data=="LT"){
      base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/LT/"
      type = "LT"
    }else if(type_data=="HB"){
      base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/HB/"
      type = "HB"
    }else if(type_data=="EQ"){
      base_url <- "ftp://ftp.datasus.gov.br/dissemin/publicos/CNES/200508_/Dados/EQ/"
      type = "EQ"
    }

    output_dir <- here::here("data-raw")
    if (!dir.exists(output_dir)){
      dir.create(output_dir)
    }

    tmp_dir <- tempdir()
    output_dir <- stringr::str_glue("{tmp_dir}\\CNES")
    if (!dir.exists(output_dir)){
      dir.create(output_dir)
    }
    output_dir <- stringr::str_glue("{output_dir}\\{type}")
    dir.create(output_dir)

    #Os dados do CADGER nao estao acessiveis atraves da URL abaixo. Para solucionar esse problema, o pacote ja incorpora esses valores, e quando é chamado, ele os copia para a pasta \data-raw\CNES\CADGER.

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

    if (!("all" %in% state_abbr)) {
      dir_files <- dir_files %>%
        dplyr::filter(state %in% state_abbr)
    }

    files_name <- dplyr::pull(dir_files, file_name)
    download_files_url <- stringr::str_glue("{base_url}{files_name}")
    output_files_path <- stringr::str_glue("{output_dir}/{files_name}")

    purrr::walk2(download_files_url, output_files_path, curl::curl_download)
  }
