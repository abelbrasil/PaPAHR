
#' get_datasus_from_local: Chamar e executar todas as outras funções definidas no pacote.
#'
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param county_id Codigo(s) do Municipio de Atendimento
#' @param dbc_dir_path Caminho para o diretorio dos arquivos .dbc
#' @param information_system String. Valores aceitos "SIA", "SIH-AIH", "SIH-SP"
#' @param health_establishment_id COdigo(s) do estabelecimento de saude
#'
#' @return output
#' @export
#'
get_datasus_from_local <-
  function(state_abbr,
           county_id,
           dbc_dir_path,
           information_system, # "SIA","SIH-AIH","SIH-SP"
           health_establishment_id) {

    `%>%` <- dplyr::`%>%`

    procedure_details <- get_procedure_details()

    cbo <- get_detail("CBO")
    cid <- get_detail("CID") %>%
      dplyr::mutate(
        #NO_CID = iconv(NO_CID, "latin1", "UTF-8"),
        dplyr::across(dplyr::ends_with("CID"), stringr::str_trim),
        NO_CID = stringr::str_c(CO_CID, NO_CID, sep="-")
      )

    data_type = switch(information_system,
                       "SIA" = "PA",
                       "SIH-AIH" = c("RD", "RJ"),
                       "SIH-SP" = "SP")

    tmp_dir <- tempdir()
    information_system_dir <- stringr::str_glue("{tmp_dir}\\{information_system}")
    if (!dir.exists(information_system_dir)){
      dir.create(information_system_dir)
    } else{
      arquivos <- list.files(information_system_dir, full.names = TRUE)
      unlink(arquivos,recursive = TRUE)
    }

    dbf_files <- list.files(dbc_dir_path, pattern = "\\.dbc$", full.names = FALSE)

    if(is.vector(data_type)){
      files_name <- dbf_files[grep(paste0(data_type[1],state_abbr), dbf_files)]
      files_name <- c(files_name, dbf_files[grep(paste0(data_type[2],state_abbr), dbf_files)])
    } else if (data_type=='PA' || data_type=='SP') {
      files_name <- dbf_files[grep(paste0(data_type,state_abbr), dbf_files)]
    }

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
      output_files_path <- stringr::str_glue("{dbc_dir_path}\\{files_chunks[[chunk]]}")

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
          health_establishment_id
        )
      }

      output_path <- stringr::str_glue("{tmp_dir}\\{information_system}\\output{information_system}_chunk_{chunk}.rds")

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
