
#' create a SUS-SIH-SP database
#'
#' @description Processar arquivos do sistema de informação SIA (DATASUS) e combina com informações do CNES e SIGTAP.
#'
#' @param year_start Um numero de 4 digitos, indicando o ano de inicio para o download dos dados.
#' @param month_start Um numero de 2 digitos, indicando o mes de inicio para o download dos dados.
#' @param year_end Um numero de 4 digitos, indicando o ano de termino para o download dos dados.
#' @param month_end Um numero de 2 digitos, indicando o mes de termino para o download dos dados.
#' @param state_abbr String. Sigla da Unidade Federativa
#' @param county_id Código(s) do Município de Atendimento: o padrão é nulo. Se o parâmetro health_establishment_id for informado, o parâmetro county_id não é obrigatório.
#' @param health_establishment_id Codigo(s) do estabelecimento de saude
#'
#' @return Um DataFrame estruturado contendo dados do SUS-SIH-SP, filtrado por estado ou estabelecimentos de saúde dentro de um intervalo de datas específico, e combinado com informações do CNES e SIGTAP.
#'
#' @examples
#'   dados = create_output_SP(
#'     year_start = 2023,
#'     month_start = 1,
#'     year_end = 2023,
#'     month_end = 3,
#'     state_abbr = "CE",
#'     county_id = "230440",
#'     health_establishment_id = c("2561492", "2481286")
#'   )
#' @export
#'
create_output_SP <-
  function(year_start,
           month_start,
           year_end,
           month_end,
           state_abbr,
           county_id = NULL,
           health_establishment_id = NULL) {
    tempo_inicio <- system.time({

      # SP = Serviços Profissionais

      `%>%` <- dplyr::`%>%`
      information_system = 'SIH-SP'

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

      download_sigtap_files(year_start,
                            month_start,
                            year_end,
                            month_end,
                            newer = FALSE)

      publication_date_start <- lubridate::ym(stringr::str_glue("{year_start}-{month_start}"))
      publication_date_end <- lubridate::ym(stringr::str_glue("{year_end}-{month_end}"))

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

      #Verificar se a pasta 'tempdir()/SIH' já existe, se sim, apaga os arquivos que estão dentro dela
      if (!dir.exists(information_system_dir)) {
        dir.create(information_system_dir)
      } else{
        arquivos <- list.files(information_system_dir, full.names = TRUE)
        unlink(arquivos, recursive = TRUE)
      }

      #Lista os nomes dos arquivos SP que serão baixados de cada mês
      dir_files = list_SIA_SIH_files(information_system,"SP",
                                     state_abbr,
                                     publication_date_start,
                                     publication_date_end)

      #Verifica se dir_files contém o nome de pelo menos um arquivo SP para cada mês.
      check_file_list(dir_files,
                      "SP",
                      state_abbr,
                      publication_date_start,
                      publication_date_end)

      #Separa os arquivos SP em grupos, caso haja vários arquivos para serem baixados.
      files_chunks = chunk(dir_files$file_name)
      n_chunks = length(files_chunks)

      data_source = stringr::str_sub(information_system, 1, 3)
      base_url <- stringr::str_glue(
        "ftp://ftp.datasus.gov.br/dissemin/publicos/{data_source}SUS/200801_/Dados/")

      for (n in 1:n_chunks) {
        dir.create(stringr::str_glue("{tmp_dir}\\{information_system}\\chunk_{n}"))
        download_files_url <- stringr::str_glue("{base_url}{files_chunks[[n]]}")
        output_files_path <- stringr::str_glue("{tmp_dir}\\{information_system}\\{names(files_chunks)[n]}\\{files_chunks[[n]]}")

        #Download dos dados SP
        purrr::walk2(download_files_url, output_files_path, curl::curl_download)

        #Carrega os dados SP
        raw_SIH_SP <- purrr::map_dfr(output_files_path, read.dbc::read.dbc, as.is = TRUE)

        #Filtra, Estrutura, une e cria novas colunas nos dados SP.
        output <- preprocess_SIH_SP(cbo,
                                    cid,
                                    raw_SIH_SP,
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
      outputSIH_SP <-
        tempdir() %>%
        list.files(information_system,
                   full.names = TRUE,
                   recursive = TRUE) %>%
        purrr::keep(~ stringr::str_detect(.x, "\\.rds$")) %>%
        purrr::map_dfr(readRDS)


      rm("counties", envir = .GlobalEnv)
      rm("health_establishment", envir = .GlobalEnv)

      # Salva o data frame em arquivo CSV no diretorio atual
      write.csv2(outputSIH_SP,
                 "./data-raw/outputSIH_SP.csv",
                 na = "",
                 row.names = FALSE)

    })
    cat("Tempo de execução:", tempo_inicio[3] / 60, "minutos\n")
    return(outputSIH_SP)

  }
