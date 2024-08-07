
#' Download SIGTAP Files
#'
#' @description A função `download_sigtap_files` baixa e descompacta arquivos SIGTAP do site DataSUS. Esses arquivos são salvos temporariamente na máquina local. Para cada mês, será baixado um arquivo compactado. Primeiro, a função verifica se o arquivo do mês especificado já existe na pasta temporária. Se o arquivo estiver presente, ela não baixa o arquivo novamente para esse mês.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param newer Logico. O padrao é TRUE. Se for TRUE e os outros parametros forem nulos, obtem o arquivo do mes mais recente disponivel no SIGTAP.
#' @param specific_dates Um vetor de strings contendo as datas específicas que se deseja baixar.
#'
#' @export
#'
#' @examples
#' \dontrun{
#'    download_sigtap_files(2024, 1, 2024, 3, newer = FALSE)
#'  }
#' \dontrun{
#'    download_sigtap_files(specific_dates = c("202210","202301","202303"))}
#'
download_sigtap_files <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           newer = TRUE,
           specific_dates = NULL) {
    `%>%` <- dplyr::`%>%`

    output_dir <- stringr::str_c(tempdir(), "SIGTAP", sep = "\\")

    if (!dir.exists(output_dir)) {
      dir.create(output_dir)
    }

    base_url <- "ftp://ftp2.datasus.gov.br/pub/sistemas/tup/downloads/"
    connection <- curl::curl(base_url)

    dir_files <-
      connection %>%
      readLines() %>%
      stringr::str_subset("TabelaUnificada*") %>%
      stringr::str_sub(57) %>%
      tibble::as_tibble_col(column_name = "file_name") %>%
      dplyr::mutate(
        file_version_id = stringr::str_sub(file_name, 17, 22),
        publication_date = lubridate::ym(file_version_id)
      )

    close(connection)

    #Se a condição "if" for verdadeira, o código irá selecionar o arquivo mais recente localizado dentro de "dir_files".
    if (newer == TRUE &
        missing(year_start) &
        missing(month_start) &
        missing(year_end) &
        missing(month_end)&
        missing(specific_dates)) {
      dir_files <-
        dir_files %>% dplyr::filter(publication_date == max(publication_date))

    } else if (!is.null(specific_dates)) {
      dir_files <- dir_files %>%
        dplyr::filter(file_version_id %in% specific_dates)

    } else{
      #Seleciona todos os arquivos dentro do intervalo especificado.
      publication_date_start <-
        lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
      publication_date_end <-
        lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))

      dir_files <- dir_files %>%
        dplyr::filter(
          publication_date >= publication_date_start,
          publication_date <= publication_date_end
        )
    }

    # Verificar se os arquivos já existem antes de baixar
    existing_files <- list.files(output_dir, full.names = TRUE)
    existing_versions <- basename(existing_files)

    files_to_download <- dir_files %>%
      dplyr::filter(!file_version_id %in% existing_versions)

    if (nrow(files_to_download) == 0) {
      #message("Os arquivos do SIGTAP não serão baixados novamente, pois já foram baixados em execuções anteriores")
    } else {
      file_version_id <- dplyr::pull(files_to_download, file_version_id)

      purrr::walk(
        stringr::str_glue("{output_dir}/{file_version_id}"),
        dir.create,
        showWarnings = FALSE
      )

      files_name <- dplyr::pull(files_to_download, file_name)

      download_files_url <-
        stringr::str_glue("{base_url}{files_name}")
      output_files_path <-
        stringr::str_glue("{output_dir}/{file_version_id}/{files_name}")
      output_zip_files_path <-
        stringr::str_glue("{output_dir}/{file_version_id}")

      purrr::walk2(download_files_url,
                   output_files_path,
                   curl::curl_download)

      purrr::walk2(output_files_path,
                   output_zip_files_path,
                   ~ utils::unzip(.x, exdir = .y))
    }
    # Remover arquivos que foram baixados anteriormente, mas que nao estao no intervalo de datas especificado atualmente
    if (newer == FALSE & is.null(specific_dates)) {
      files_to_keep <- dir_files %>%
        dplyr::filter(
          publication_date >= publication_date_start &
            publication_date <= publication_date_end
        ) %>%
        dplyr::pull(file_version_id)

      files_to_remove <- setdiff(existing_versions, files_to_keep)

      if (length(files_to_remove) > 0) {
        purrr::walk(files_to_remove, ~ unlink(file.path(output_dir, .x), recursive = TRUE))
      }
    }

  }

#Para ver o diretorio temporario onde foi salvo os arquivos
#output_dir <- stringr::str_c(tempdir(), "SIGTAP", sep="\\")
#list.files(output_dir)
