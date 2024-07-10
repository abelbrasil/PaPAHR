
#' get_datasus: Chamar e executar todas as outras funções definidas no pacote.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param information_system String. Valores aceitos "SIA", "SIH-AIH", "SIH-SP"
#' @param health_establishment_id COdigo(s) do estabelecimento de saude
#'
#' @return output
#' @export
#'
get_datasus <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           state_abbr,
           county_id,
           information_system, # "SIA","SIH-AIH","SIH-SP"
           health_establishment_id){

    `%>%` <- dplyr::`%>%`
    publication_date_start <- lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
    publication_date_end <- lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))
    cat(publication_date_start,'\n')
    cat(publication_date_end,'\n')

    #$ Fase 2: Ler os arquivos SIGTAP

    #$ Arquivo get_procedures_details

    procedure_details <- get_procedure_details()

    cbo <- get_detail("CBO")

    cid <- get_detail("CID") %>%
      dplyr::mutate(
        #NO_CID = iconv(NO_CID, "latin1", "UTF-8"),
        dplyr::across(dplyr::ends_with("CID"), stringr::str_trim),
        NO_CID = stringr::str_c(CO_CID, NO_CID, sep="-")
      )

    #$ ______________________________________

    #$ Fase 3: baixar os arquivos .dbc PA, RD, RJ e SP

    tmp_dir <- tempdir()

    data_source = stringr::str_sub(information_system, 1, 3)

    data_type = switch(information_system,
                       "SIA" = "PA",
                       "SIH-AIH" = c("RD", "RJ"),
                       "SIH-SP" = "SP")

    information_system_dir <- stringr::str_glue("{tmp_dir}\\{information_system}")
    if (!dir.exists(information_system_dir)){
      dir.create(information_system_dir)
    } else{
      arquivos <- list.files(information_system_dir, full.names = TRUE)
      unlink(arquivos,recursive = TRUE)
    }

    base_url <- stringr::str_glue("ftp://ftp.datasus.gov.br/dissemin/publicos/{data_source}SUS/200801_/Dados/")

    connection <- curl::curl(base_url)

    #$ Listar todos os arquivos (PA, RD, e etc) do DATASUS
    dir_files <-
      connection %>%
      readLines() %>%
      stringr::str_sub(start=-12) %>%
      tibble::as_tibble_col(column_name = "file_name") %>%
      dplyr::mutate(
        state = stringr::str_sub(file_name, 3, 4),
        publication_date = lubridate::ym(stringr::str_sub(file_name, 5, 8),quiet = TRUE),
        file_type = stringr::str_sub(file_name, 1, 2)) %>%
      dplyr::filter(file_type %in% data_type,
        state == state_abbr,
        publication_date >= publication_date_start,
        publication_date <= publication_date_end)

    close(connection)

    if (any(is.na(dir_files$publication_date))) {
      warning("Algumas datas não puderam ser analisadas corretamente. No argumento: publication_date = lubridate::ym(stringr::str_sub(file_name, 5, 8)).")
    }

    files_name <- dplyr::pull(dir_files, file_name)
    n_files <- files_name %>% length()
    chunk_size <- ceiling(0.2*n_files)
    n_chunks <- ceiling(n_files/chunk_size)

    files_chunks <- list()
    chunk = 1

    while (chunk <= n_chunks) {
      upper_limit <- ifelse(chunk*chunk_size > n_files, n_files, chunk*chunk_size)
      files_chunks[[chunk]] <- files_name[(chunk_size*(chunk-1)+1):upper_limit]
      names(files_chunks)[chunk] = stringr::str_glue("chunk_{chunk}")
      chunk = chunk + 1
    }

    for (chunk in 1:n_chunks) {

      dir.create(stringr::str_glue("{tmp_dir}\\{information_system}\\chunk_{chunk}"))
      download_files_url <- stringr::str_glue("{base_url}{files_chunks[[chunk]]}")
      output_files_path <- stringr::str_glue("{tmp_dir}\\{information_system}\\{names(files_chunks)[chunk]}\\{files_chunks[[chunk]]}")
      purrr::walk2(download_files_url, output_files_path, curl::curl_download)

      if (information_system == "SIA") {

        raw_SIA <- purrr::map_dfr(output_files_path, read.dbc::read.dbc, as.is=TRUE)
        specific_dates <- unique(raw_SIA$PA_CMP)

        # Definir diretorio de saida e obter arquivos existentes
        output_dir <- stringr::str_c(tempdir(), "SIGTAP", sep = "\\")
        existing_files <- list.files(output_dir, full.names = TRUE)
        existing_versions <- basename(existing_files)

        # Determinar quais arquivos baixar
        files_to_download <- specific_dates[!specific_dates %in% existing_versions]

        if (length(files_to_download) > 0){
          download_sigtap_files(newer = FALSE, specific_dates = specific_dates)
          procedure_details <- get_procedure_details()
          cbo <- get_detail("CBO")
          cid <- get_detail("CID") %>%
            dplyr::mutate(
              #NO_CID = iconv(NO_CID, "latin1", "UTF-8"),
              dplyr::across(dplyr::ends_with("CID"), stringr::str_trim),
              NO_CID = stringr::str_c(CO_CID, NO_CID, sep="-")
            )
        }

        output <-
          preprocess_SIA(
            cbo,
            cid,
            raw_SIA,
            county_id,
            procedure_details,
            publication_date_start,
            health_establishment_id
          )
      }

      if (information_system == "SIH-AIH") {
        file_type <- output_files_path %>%
          stringr::str_sub(start=-12) %>%
          tibble::as_tibble_col(column_name = "file_name") %>%
          dplyr::mutate(file_type = stringr::str_sub(file_name, 1, 2),
                 file_id = as.character(dplyr::row_number()))
        dplyr::select(file_type, file_id)

        raw_SIH <- purrr::map_dfr(output_files_path, read.dbc::read.dbc, as.is=TRUE, .id="file_id")

        output <- preprocess_SIH(
          cbo,
          cid,
          raw_SIH,
          file_type,
          county_id,
          procedure_details,
          publication_date_start,
          health_establishment_id
        )
      }

      if (information_system == "SIH-SP") {
        raw_SIH_SP <- purrr::map_dfr(output_files_path, read.dbc::read.dbc, as.is=TRUE)

        output <- preprocess_SIH_SP(
          cbo,
          cid,
          raw_SIH_SP,
          county_id,
          procedure_details,
          publication_date_start,
          health_establishment_id
        )
      }

      output_path <- stringr::str_glue("{tmp_dir}\\{information_system}\\{names(files_chunks)[chunk]}\\output{information_system}_chunk_{chunk}.rds")

      saveRDS(output,
              file = output_path)
    }

    output <-
      tempdir() %>%
      list.files(information_system, full.names=TRUE, recursive=TRUE) %>%
      .[!stringr::str_detect(., "DATASUS\\.SIA\\.SIH_1\\.0\\.1\\.tar\\.gz")] %>%
      purrr::map_dfr(readRDS)

    return(output)

  }


