
#' Create SUS-SIH-AIH(RD e RJ) database from local DBC files
#'
#' @description Processar arquivos do sistema de informação SIH (DATASUS) que já estão baixados localmente e combiná-los com informações do CNES e SIGTAP.
#'
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param dbc_dir_path Diretório que contêm os arquivos DBC
#' @param county_id Codigo(s) do Municipio de Atendimento. O padrao é NULL.
#' @param health_establishment_id Código(s) do estabelecimento de saúde
#'
#' @return Um DataFrame estruturado contendo dados do SUS-SIH-AIH(RD e RJ), filtrado por estado ou estabelecimentos de saúde dentro de um intervalo de datas específico, e combinado com informações do CNES e SIGTAP.
#'
#' @examples
#'   \dontrun{
#'     create_output_AIH_from_local(
#'       state_abbr = "CE",
#'       dbc_dir_path = "X:/USID/BOLSA_EXTENSAO_2024/dbc/dbc-2301-2306",
#'       county_id = "230440",
#'       health_establishment_id = c("2561492", "2481286")
#'     )
#'   }
#'
#' @export
create_output_AIH_from_local <-
  function(state_abbr,
           dbc_dir_path,
           county_id = NULL,
           health_establishment_id = NULL) {

    tempo_inicio <- system.time({
      # AIH =
      # RD = Reduzixa
      # RJ = AIH Rejeitada

      `%>%` <- dplyr::`%>%`
      information_system = 'SIH-AIH'

      #Se o id do municipio for igual a 7 caracteres, remove o último caracter.
      if (!is.null(county_id)) {
        if (nchar(county_id) == 7) {
          county_id <- substr(county_id, 1, nchar(county_id) - 1)
        }
      }

      state_abbr = toupper(trimws(state_abbr))

      #Cria uma variável global com os dados do município
      get_counties()

      #Baixa os dados do CNES/ST e descompacta os dados do CNES/CADGER
      get_CNES()

      download_sigtap_files() #obtem os dodos do SIGTAP do mes mais recente disponivel.

      #Ler os dados do SIGTAP (Procedimentos, CBO e CID)
      procedure_details <- get_procedure_details()
      cbo <- get_detail("CBO")
      cid <- get_detail("CID") %>%
        dplyr::mutate(
          #NO_CID = iconv(NO_CID, "latin1", "UTF-8"),
          dplyr::across(dplyr::ends_with("CID"), stringr::str_trim),
          NO_CID = stringr::str_c(CO_CID, NO_CID, sep = "-")
        )


      tmp_dir <- tempdir()
      information_system_dir <- stringr::str_glue("{tmp_dir}\\{information_system}")

      #Verificar se a pasta 'tempdir()/SIA' já existe, se sim, apaga os arquivos que estão dentro dela
      if (!dir.exists(information_system_dir)) {
        dir.create(information_system_dir)
      } else{
        arquivos <- list.files(information_system_dir, full.names = TRUE)
        unlink(arquivos, recursive = TRUE)
      }

      #Retorna um vetor com os nomes de todos os arquivos .dbc presentes no diretório dbc_dir_path
      dbf_files <- list.files(dbc_dir_path, pattern = "\\.dbc$", full.names = FALSE)

      #Filtra só os arquivos RD e RJ
      files_name <- dbf_files[grep(paste0("RD",state_abbr), dbf_files)]
      files_name <- c(files_name, dbf_files[grep(paste0("RJ",state_abbr), dbf_files)])

      #Separa os arquivos RD e RJ em grupos, caso haja vários arquivos para serem baixados.
      files_chunks = chunk(files_name)
      n_chunks = length(files_chunks)

      for (n in 1:n_chunks) {
        output_files_path <- stringr::str_glue("{dbc_dir_path}\\{files_chunks[[n]]}")
        dir.create(stringr::str_glue("{tmp_dir}\\{information_system}\\chunk_{n}"))

        #Um DF com o nome e tipo dos arquivos RD e RJ
        file_type <- output_files_path %>%
          stringr::str_sub(start=-12) %>%
          tibble::as_tibble_col(column_name = "file_name") %>%
          dplyr::mutate(file_type = stringr::str_sub(file_name, 1, 2),
                        file_id = as.character(dplyr::row_number()))
        dplyr::select(file_type, file_id)

        #Carrega os dados RD e RJ
        raw_SIH <- purrr::map_dfr(output_files_path, read.dbc::read.dbc, as.is=TRUE, .id="file_id")

        #Filtra, Estrutura, une e cria novas colunas nos dados RD e RJ.
        output <- preprocess_SIH(cbo,
                                 cid,
                                 raw_SIH,
                                 file_type,
                                 county_id,
                                 procedure_details,
                                 health_establishment_id)


        #O output de cada chunk é salvo em um arquivo .rds em uma pasta temporária do sistema.
        if (!is.null(output)) {
          output_path <-
            stringr::str_glue(
              "{tmp_dir}\\{information_system}\\{names(files_chunks)[n]}\\output{information_system}_chunk_{n}.rds")

          saveRDS(output, file = output_path)
        }
      }

      #Une os arquivos output.rds de cada chunk em um único arquivo.
      outputSIH_AIH <-
        tempdir() %>%
        list.files(information_system,
                   full.names = TRUE,
                   recursive = TRUE) %>%
        purrr::keep(~ stringr::str_detect(.x, "\\.rds$")) %>%
        purrr::map_dfr(readRDS)


      rm("counties", envir = .GlobalEnv)
      rm("health_establishment", envir = .GlobalEnv)

      # Salva o data frame em arquivo CSV no diretorio atual
      write.csv2(outputSIH_AIH,
                 "./data-raw/outputSIH_AIH.csv",
                 na = "",
                 row.names = FALSE)


    })
    cat("Tempo de execução:", tempo_inicio[3] / 60, "minutos\n")
    return(outputSIH_AIH)
  }
